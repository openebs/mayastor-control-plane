use crate::controller::{
    io_engine::{NexusChildApi, ReplicaApi},
    registry::Registry,
    resources::{
        operations::{ResourceOffspring, ResourceOwnerUpdate, ResourceSharing},
        operations_helper::{GuardedOperationsHelper, OperationSequenceGuard},
        OperationGuardArc,
    },
};
use agents::errors::SvcError;
use stor_port::types::v0::{
    store::{
        nexus::{NexusOperation, NexusSpec, ReplicaUri},
        nexus_child::NexusChild,
        volume::VolumeSpec,
    },
    transport::{
        AddNexusReplica, Child, ChildUri, Nexus, NodeId, RemoveNexusChild, RemoveNexusReplica,
        Replica, ReplicaOwners, SetReplicaEntityId, ShareReplica,
    },
};

impl OperationGuardArc<NexusSpec> {
    /// Attach the specified replica to the volume nexus
    /// The replica might need to be shared/unshared so it can be opened by the nexus
    pub(crate) async fn attach_replica(
        &mut self,
        registry: &Registry,
        replica: &Replica,
    ) -> Result<(), SvcError> {
        // Adding a replica to a nexus will initiate a rebuild.
        // First check that we are able to start a rebuild.
        registry.rebuild_allowed().await?;

        let uri = self.make_me_replica_accessible(registry, replica).await?;
        let request = AddNexusReplica {
            node: self.as_ref().node.clone(),
            nexus: self.as_ref().uuid.clone(),
            replica: ReplicaUri::new(&replica.uuid, &uri),
            auto_rebuild: true,
        };
        self.add_replica(registry, &request).await?;
        Ok(())
    }

    /// Remove the given replica from the nexus.
    pub(crate) async fn remove_replica(
        &mut self,
        registry: &Registry,
        replica: &ReplicaUri,
    ) -> Result<(), SvcError> {
        let request = RemoveNexusReplica::new(&self.as_ref().node, self.uuid(), replica);
        let node = registry.node_wrapper(&request.node).await?;

        let status = registry.nexus(&request.nexus).await?;
        let spec_clone = self
            .start_update(
                registry,
                &status,
                NexusOperation::RemoveChild(NexusChild::from(&request.replica)),
            )
            .await?;

        let result = node.remove_child(&RemoveNexusChild::from(&request)).await;
        self.on_remove_disown_replica(registry, &request, &result)
            .await;

        self.complete_update(registry, result, spec_clone).await
    }

    /// Remove a nexus child uri and disown it from the nexus.
    /// If the volume is present, also attempts to remove the replica from the volume, if no
    /// longer required.
    pub(crate) async fn remove_vol_child_by_uri(
        &mut self,
        volume: &mut Option<&mut OperationGuardArc<VolumeSpec>>,
        registry: &Registry,
        nexus: &Nexus,
        uri: &ChildUri,
    ) -> Result<(), SvcError> {
        let nexus_children = &self.as_ref().children;
        match nexus_children.iter().find(|c| &c.uri() == uri).cloned() {
            Some(NexusChild::Replica(replica)) => {
                self.remove_replica(registry, &replica).await?;
                if let Some(volume) = volume {
                    volume
                        .remove_unused_volume_replica(registry, replica.uuid())
                        .await
                        .ok();
                }
                Ok(())
            }
            Some(NexusChild::Uri(uri)) => {
                let request = RemoveNexusChild::new(&nexus.node, &nexus.uuid, &uri);
                self.remove_child(registry, &request).await
            }
            None => {
                let request = RemoveNexusChild::new(&nexus.node, &nexus.uuid, uri);
                self.remove_child(registry, &request).await
            }
        }
    }

    /// Remove a nexus child uri and disown it from the nexus.
    pub(crate) async fn remove_child_by_uri(
        &mut self,
        registry: &Registry,
        nexus: &Nexus,
        uri: &ChildUri,
    ) -> Result<(), SvcError> {
        self.remove_vol_child_by_uri(&mut None, registry, nexus, uri)
            .await
    }

    /// Make the replica accessible on the specified `NodeId`.
    /// This means the replica might have to be shared/unshared so it can be open through
    /// the correct protocol (loopback locally, and nvmf remotely).
    pub(crate) async fn make_me_replica_accessible(
        &self,
        registry: &Registry,
        replica_state: &Replica,
    ) -> Result<ChildUri, SvcError> {
        Self::make_replica_accessible(registry, replica_state, &self.as_ref().node).await
    }

    /// Make the replica accessible on the specified `NodeId`.
    /// This means the replica might have to be shared/unshared so it can be open through
    /// the correct protocol (loopback locally, and nvmf remotely).
    pub(crate) async fn make_replica_accessible(
        registry: &Registry,
        replica_state: &Replica,
        nexus_node: &NodeId,
    ) -> Result<ChildUri, SvcError> {
        let mut replica = registry.specs().replica(&replica_state.uuid).await?;
        if replica_state.entity_id.is_none() {
            if let Some(vol) = replica.as_ref().owners.volume() {
                let node = registry.node_wrapper(&replica_state.node).await?;
                let set_entity_id = SetReplicaEntityId::new(
                    replica_state.uuid.clone(),
                    vol.clone(),
                    replica_state.node.clone(),
                );
                if let Err(error) = node.set_replica_entity_id(&set_entity_id).await {
                    if error.tonic_code() != tonic::Code::Unimplemented {
                        tracing::error!(
                            replica.uuid = %replica_state.uuid,
                            %error,
                            "Failed to set entity_id",
                        );
                    }
                }
            }
        }
        if nexus_node == &replica_state.node {
            // on the same node, so connect via the loopback bdev
            match replica.unshare(registry, &replica_state.into()).await {
                Ok(uri) => Ok(uri.into()),
                Err(SvcError::NotShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        } else {
            // on a different node, so connect via an nvmf target
            let allowed_hosts = if registry.target_acc_disabled() {
                vec![]
            } else {
                registry.node_nqn(nexus_node).await?
            };
            let request = ShareReplica::from(replica_state).with_hosts(allowed_hosts);
            match replica.share(registry, &request).await {
                Ok(uri) => Ok(ChildUri::from(uri)),
                Err(SvcError::AlreadyShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        }
    }

    /// Add the given request replica to the nexus.
    pub(super) async fn add_replica(
        &mut self,
        registry: &Registry,
        request: &AddNexusReplica,
    ) -> Result<Child, SvcError> {
        let node = registry.node_wrapper(&request.node).await?;
        let replica = registry.specs().replica(request.replica.uuid()).await?;
        // we don't persist nexus owners to pstor anymore, instead we rebuild at startup
        replica.lock().owners.add_owner(&request.nexus);

        let status = registry.nexus(&request.nexus).await?;
        let spec_clone = self
            .start_update(
                registry,
                &status,
                NexusOperation::AddChild(NexusChild::from(&request.replica)),
            )
            .await?;

        let result = node.add_child(&request.into()).await;
        self.complete_update(registry, result, spec_clone).await
    }

    /// On successful removal of replica from nexus, disown the replica from the nexus.
    pub(super) async fn on_remove_disown_replica(
        &self,
        registry: &Registry,
        request: &RemoveNexusReplica,
        result: &Result<(), SvcError>,
    ) {
        if result.is_ok() {
            if let Some(replica) = registry.specs().replica_rsc(request.replica.uuid()) {
                if let Ok(mut replica) = replica.operation_guard() {
                    let disowner = ReplicaOwners::new(None, vec![request.nexus.clone()]);
                    let _ = replica.remove_owners(registry, &disowner, true).await;
                }
            }
        }
    }
}
