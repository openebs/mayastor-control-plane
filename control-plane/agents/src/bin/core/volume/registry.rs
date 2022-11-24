use crate::controller::registry::Registry;
use agents::errors::SvcError;
use common_lib::types::v0::transport::{
    uri_with_hostnqn, NexusStatus, ReplicaTopology, Volume, VolumeId, VolumeState, VolumeStatus,
};

use crate::controller::reconciler::PollTriggerEvent;
use common_lib::types::v0::store::{replica::ReplicaSpec, volume::VolumeSpec};
use grpc::operations::{PaginatedResult, Pagination};
use std::collections::HashMap;

impl Registry {
    /// Get the volume state for the specified volume.
    pub(crate) async fn get_volume_state(
        &self,
        volume_uuid: &VolumeId,
    ) -> Result<VolumeState, SvcError> {
        let volume_spec = self.specs().get_volume(volume_uuid)?;
        let replica_specs = self.specs().get_cloned_volume_replicas(volume_uuid);

        self.get_volume_state_with_replicas(&volume_spec, &replica_specs)
            .await
    }

    /// Get the volume state for the specified volume.
    /// replicas is a pre-fetched list of replicas from any and all volumes.
    pub(crate) async fn get_volume_state_with_replicas(
        &self,
        volume_spec: &VolumeSpec,
        replicas: &[ReplicaSpec],
    ) -> Result<VolumeState, SvcError> {
        let replica_specs = replicas
            .iter()
            .filter(|r| r.owners.owned_by(&volume_spec.uuid))
            .collect::<Vec<_>>();

        let nexus_spec = self.specs().get_volume_target_nexus(volume_spec);
        let nexus_state = match nexus_spec {
            None => None,
            Some(spec) => {
                let nexus_id = spec.lock().uuid.clone();
                self.get_nexus(&nexus_id).await.ok()
            }
        };

        // Construct the topological information for the volume replicas.
        let mut replica_topology = HashMap::new();
        for replica_spec in &replica_specs {
            replica_topology.insert(
                replica_spec.uuid.clone(),
                self.replica_topology(replica_spec).await,
            );
        }

        Ok(if let Some(mut nexus_state) = nexus_state {
            let nqns = nexus_state
                .allowed_hosts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            nexus_state.device_uri = uri_with_hostnqn(&nexus_state.device_uri, &nqns);
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
            }
        } else {
            VolumeState {
                uuid: volume_spec.uuid.to_owned(),
                size: volume_spec.size,
                status: if volume_spec.target.is_none() {
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
            }
        })
    }

    /// Construct a replica topology from a replica spec.
    /// If the replica cannot be found, return the default replica topology.
    async fn replica_topology(&self, spec: &ReplicaSpec) -> ReplicaTopology {
        match self.get_replica(&spec.uuid).await {
            Ok(state) => ReplicaTopology::new(Some(state.node), Some(state.pool_id), state.status),
            Err(_) => {
                tracing::trace!(replica.uuid = %spec.uuid, "Replica not found. Constructing default replica topology");
                ReplicaTopology::default()
            }
        }
    }

    /// Get all volumes
    pub(super) async fn get_volumes(&self) -> Vec<Volume> {
        let volume_specs = self.specs().get_volumes();
        let replicas = self.specs().get_cloned_replicas();
        let mut volumes = Vec::with_capacity(volume_specs.len());
        for spec in volume_specs {
            if let Ok(state) = self.get_volume_state_with_replicas(&spec, &replicas).await {
                volumes.push(Volume::new(spec, state));
            }
        }
        volumes
    }

    /// Get a paginated subset of volumes
    pub(super) async fn get_paginated_volume(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<Volume> {
        let volume_specs = self.specs().get_paginated_volumes(pagination);
        let mut volumes = Vec::with_capacity(volume_specs.len());
        let last = volume_specs.last();
        for spec in volume_specs.result() {
            if let Ok(state) = self.get_volume_state(&spec.uuid).await {
                volumes.push(Volume::new(spec, state));
            }
        }
        PaginatedResult::new(volumes, last)
    }

    /// Return a volume object corresponding to the ID.
    pub(crate) async fn get_volume(&self, id: &VolumeId) -> Result<Volume, SvcError> {
        Ok(Volume::new(
            self.specs().get_volume(id)?,
            self.get_volume_state(id).await?,
        ))
    }

    /// Notify the reconcilers if the volume is degraded
    pub(crate) async fn notify_if_degraded(&self, volume: &Volume, event: PollTriggerEvent) {
        if volume.status() == Some(VolumeStatus::Degraded) {
            self.notify(event).await;
        }
    }
}
