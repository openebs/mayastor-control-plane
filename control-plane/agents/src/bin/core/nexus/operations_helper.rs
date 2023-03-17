use crate::controller::{
    io_engine::NexusChildApi,
    registry::Registry,
    resources::{
        operations::ResourceOffspring,
        operations_helper::{GuardedOperationsHelper, OperationSequenceGuard, ResourceSpecsLocked},
        OperationGuardArc, TraceSpan,
    },
};
use agents::errors::SvcError;

use crate::controller::resources::operations::{ResourceOwnerUpdate, ResourceSharing};
use stor_port::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            nexus::{NexusOperation, NexusSpec, ReplicaUri},
            nexus_child::NexusChild,
        },
        transport::{
            AddNexusReplica, Child, ChildUri, Nexus, NodeId, RemoveNexusChild, RemoveNexusReplica,
            Replica, ReplicaOwners, ShareReplica,
        },
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

    /// Remove a nexus child uri.
    /// If it's a replica it also disowns the replica from the volume and attempts to destroy it,
    /// if requested.
    pub(crate) async fn remove_child_by_uri(
        &mut self,
        registry: &Registry,
        nexus: &Nexus,
        uri: &ChildUri,
        destroy_replica: bool,
    ) -> Result<(), SvcError> {
        let nexus_children = &self.as_ref().children;
        match nexus_children.iter().find(|c| &c.uri() == uri).cloned() {
            Some(NexusChild::Replica(replica)) => {
                match self.remove_replica(registry, &replica).await {
                    Ok(_) if destroy_replica => {
                        let mut replica = registry
                            .specs()
                            .replica_rsc(replica.uuid())
                            .ok_or(SvcError::ReplicaNotFound {
                                replica_id: replica.uuid().clone(),
                            })?
                            .operation_guard()?;
                        let pool_ref = replica.as_ref().pool.pool_name();
                        match ResourceSpecsLocked::pool_node(registry, pool_ref).await {
                            Some(node) => {
                                if let Err(error) =
                                    replica.disown_and_destroy_replica(registry, &node).await
                                {
                                    self.error_span(|| {
                                        tracing::error!(
                                            replica.uuid = %replica.uuid(),
                                            error = %error.full_string(),
                                            "Failed to disown and destroy the replica"
                                        )
                                    });
                                }
                            }
                            None => {
                                // The replica node was not found (perhaps because it is offline).
                                // The replica can't be destroyed because the node isn't there.
                                // Instead, disown the replica from the volume and let the garbage
                                // collector destroy it later.
                                self.warn_span(|| {
                                    tracing::warn!(
                                        replica.uuid = %replica.uuid(),
                                        "Failed to find the node where the replica is hosted"
                                    )
                                });
                                let disowner = ReplicaOwners::new_disown_all();
                                let _ = replica.remove_owners(registry, &disowner, true).await;
                            }
                        }

                        Ok(())
                    }
                    result => result,
                }
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
    /// Disown nexus from its owner.
    pub(crate) async fn disown(&mut self, registry: &Registry) -> Result<(), SvcError> {
        self.lock().disowned_by_volume();
        registry.store_obj(self.as_ref()).await
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
        if nexus_node == &replica_state.node {
            // on the same node, so connect via the loopback bdev
            let mut replica = registry.specs().replica(&replica_state.uuid).await?;
            match replica.unshare(registry, &replica_state.into()).await {
                Ok(uri) => Ok(uri.into()),
                Err(SvcError::NotShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        } else {
            // on a different node, so connect via an nvmf target
            let mut replica = registry.specs().replica(&replica_state.uuid).await?;
            let allowed_hosts = registry.node_nqn(nexus_node).await?;
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
