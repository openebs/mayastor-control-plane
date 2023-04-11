use crate::controller::registry::Registry;
use agents::errors::SvcError;
use stor_port::types::v0::transport::{
    uri_with_hostnqn, Nexus, NexusStatus, ReplicaTopology, Volume, VolumeId, VolumeState,
    VolumeStatus, VolumeUsage,
};

use crate::controller::reconciler::PollTriggerEvent;
use grpc::operations::{PaginatedResult, Pagination};
use std::collections::HashMap;
use stor_port::{
    types::v0::store::{replica::ReplicaSpec, volume::VolumeSpec},
    IntoOption,
};

impl Registry {
    /// Get the volume state for the specified volume.
    pub(crate) async fn volume_state(
        &self,
        volume_uuid: &VolumeId,
    ) -> Result<VolumeState, SvcError> {
        let volume_spec = self.specs().volume_clone(volume_uuid)?;
        let replica_specs = self.specs().volume_replicas_cln(volume_uuid);

        self.volume_state_with_replicas(&volume_spec, &replica_specs)
            .await
    }

    /// Get the volume state for the specified volume.
    /// replicas is a pre-fetched list of replicas from any and all volumes.
    pub(crate) async fn volume_state_with_replicas(
        &self,
        volume_spec: &VolumeSpec,
        replicas: &[ReplicaSpec],
    ) -> Result<VolumeState, SvcError> {
        let replica_specs = replicas
            .iter()
            .filter(|r| r.owners.owned_by(&volume_spec.uuid))
            .collect::<Vec<_>>();

        let nexus_spec = self.specs().volume_target_nexus_rsc(volume_spec);
        let nexus = match nexus_spec {
            None => None,
            Some(nexus) => {
                let spec = nexus.immutable_ref();
                self.node_nexus(&spec.node, &spec.uuid)
                    .await
                    .ok()
                    .map(|s| (nexus, s))
            }
        };

        let mut total_usage = 0;
        let mut largest_allocated = 0;
        // Construct the topological information for the volume replicas.
        let mut replica_topology = HashMap::new();
        for replica_spec in &replica_specs {
            let replica = self
                .replica_topology(replica_spec, nexus.as_ref().map(|(_, n)| n))
                .await;
            if let Some(usage) = replica.usage() {
                total_usage += usage.allocated();
                if usage.allocated() > largest_allocated {
                    largest_allocated = usage.allocated();
                }
            }
            replica_topology.insert(replica_spec.uuid.clone(), replica);
        }

        let usage = Some(VolumeUsage::new(
            volume_spec.size,
            largest_allocated,
            total_usage,
        ));

        Ok(if let Some((nexus, mut nexus_state)) = nexus {
            let ah = nexus.lock().allowed_hosts.clone();
            nexus_state.device_uri = uri_with_hostnqn(&nexus_state.device_uri, &ah);
            VolumeState {
                uuid: volume_spec.uuid.to_owned(),
                size: nexus_state.size,
                status: match nexus_state.status {
                    NexusStatus::Online
                        if nexus_state.children.len() != volume_spec.num_replicas as usize =>
                    {
                        VolumeStatus::Degraded
                    }
                    _ => nexus_state.status.clone(),
                },
                target: Some(nexus_state),
                replica_topology,
                usage,
            }
        } else {
            VolumeState {
                uuid: volume_spec.uuid.to_owned(),
                size: volume_spec.size,
                status: if volume_spec.target().is_none() {
                    if replica_specs.len() >= volume_spec.num_replicas as usize {
                        VolumeStatus::Online
                    } else if replica_specs.is_empty() {
                        VolumeStatus::Faulted
                    } else {
                        VolumeStatus::Degraded
                    }
                } else {
                    VolumeStatus::Unknown
                },
                target: None,
                replica_topology,
                usage,
            }
        })
    }

    /// Construct a replica topology from a replica spec.
    /// If the replica cannot be found, return the default replica topology.
    async fn replica_topology(&self, spec: &ReplicaSpec, nexus: Option<&Nexus>) -> ReplicaTopology {
        match self.replica(&spec.uuid).await {
            Ok(state) => {
                let child = nexus.and_then(|n| n.child(&state.uri));
                ReplicaTopology::new(
                    Some(state.node),
                    Some(state.pool_id),
                    state.status,
                    state.space.into_opt(),
                    child.map(|c| c.state.clone()),
                    child.map(|c| c.state_reason.clone()),
                    child.and_then(|c| c.rebuild_progress),
                )
            }
            Err(_) => {
                tracing::trace!(replica.uuid = %spec.uuid, "Replica not found. Constructing default replica topology");
                ReplicaTopology::default()
            }
        }
    }

    /// Get all volumes.
    pub(crate) async fn volumes(&self) -> Vec<Volume> {
        let volume_specs = self.specs().volumes();
        let replicas = self.specs().replicas_cloned();
        let mut volumes = Vec::with_capacity(volume_specs.len());
        for spec in volume_specs {
            if let Ok(state) = self.volume_state_with_replicas(&spec, &replicas).await {
                volumes.push(Volume::new(spec, state));
            }
        }
        volumes
    }

    /// Get a paginated subset of volumes.
    pub(super) async fn paginated_volumes(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<Volume> {
        let volume_specs = self.specs().paginated_volumes(pagination);
        let mut volumes = Vec::with_capacity(volume_specs.len());
        let last = volume_specs.last();
        for spec in volume_specs.result() {
            if let Ok(state) = self.volume_state(&spec.uuid).await {
                volumes.push(Volume::new(spec, state));
            }
        }
        PaginatedResult::new(volumes, last)
    }

    /// Return a volume object corresponding to the ID.
    pub(crate) async fn volume(&self, id: &VolumeId) -> Result<Volume, SvcError> {
        Ok(Volume::new(
            self.specs().volume_clone(id)?,
            self.volume_state(id).await?,
        ))
    }

    /// Notify the reconcilers if the volume is degraded.
    pub(crate) async fn notify_if_degraded(&self, volume: &Volume, event: PollTriggerEvent) {
        if volume.status() == Some(VolumeStatus::Degraded) {
            self.notify(event).await;
        }
    }
}
