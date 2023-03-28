use crate::controller::{
    io_engine::ReplicaApi,
    registry::Registry,
    resources::{
        operations::{ResourceLifecycle, ResourceOffspring, ResourceOwnerUpdate, ResourceSharing},
        operations_helper::{GuardedOperationsHelper, OnCreateFail, OperationSequenceGuard},
        OperationGuardArc, UpdateInnerValue,
    },
};
use agents::errors::{SvcError, SvcError::CordonedNode};
use stor_port::types::v0::{
    store::{
        nexus::NexusSpec,
        replica::{PoolRef, ReplicaOperation, ReplicaSpec},
    },
    transport::{
        CreateReplica, DestroyReplica, NodeId, RemoveNexusChild, Replica, ReplicaOwners,
        ShareReplica, UnshareReplica,
    },
};

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<ReplicaSpec> {
    type Create = CreateReplica;
    type CreateOutput = Replica;
    type Destroy = DestroyReplica;

    async fn create(
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let specs = registry.specs();

        if registry.node_cordoned(&request.node)? {
            return Err(CordonedNode {
                node_id: request.node.to_string(),
            });
        }

        let node = registry.node_wrapper(&request.node).await?;

        let replica = specs
            .get_or_create_replica(request)
            .operation_guard_wait()
            .await?;
        let _ = replica.start_create(registry, request).await?;

        let result = node.create_replica(request).await;
        let on_fail = OnCreateFail::eeinval_delete(&result);

        replica.complete_create(result, registry, on_fail).await
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        Some(self).destroy(registry, request).await
    }
}

#[async_trait::async_trait]
impl ResourceLifecycle for Option<&mut OperationGuardArc<ReplicaSpec>> {
    type Create = CreateReplica;
    type CreateOutput = Replica;
    type Destroy = DestroyReplica;

    async fn create(
        _registry: &Registry,
        _request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        unimplemented!()
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        let node = registry.node_wrapper(&request.node).await?;

        if let Some(replica) = self {
            replica
                .start_destroy_by(registry, &request.disowners)
                .await?;

            let result = match node.destroy_replica(request).await {
                Ok(()) => Ok(()),
                Err(error) if error.tonic_code() == tonic::Code::NotFound => Ok(()),
                Err(error) => Err(error),
            };
            replica.complete_destroy(result, registry).await
        } else {
            node.destroy_replica(request).await
        }
    }
}

#[async_trait::async_trait]
impl ResourceSharing for OperationGuardArc<ReplicaSpec> {
    type Share = ShareReplica;
    type ShareOutput = String;
    type Unshare = UnshareReplica;
    type UnshareOutput = String;

    async fn share(
        &mut self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<Self::ShareOutput, SvcError> {
        Some(self).share(registry, request).await
    }

    async fn unshare(
        &mut self,
        registry: &Registry,
        request: &Self::Unshare,
    ) -> Result<Self::UnshareOutput, SvcError> {
        Some(self).unshare(registry, request).await
    }
}

#[async_trait::async_trait]
impl ResourceSharing for Option<&mut OperationGuardArc<ReplicaSpec>> {
    type Share = ShareReplica;
    type ShareOutput = String;
    type Unshare = UnshareReplica;
    type UnshareOutput = String;

    async fn share(
        &mut self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<Self::ShareOutput, SvcError> {
        let node = registry.node_wrapper(&request.node).await?;

        if let Some(replica) = self {
            let status = registry.get_replica(&request.uuid).await?;
            let update = ReplicaOperation::Share(request.protocol, request.allowed_hosts.clone());
            let spec_clone = replica.start_update(registry, &status, update).await?;

            let result = node.share_replica(request).await;
            replica.complete_update(registry, result, spec_clone).await
        } else {
            node.share_replica(request).await
        }
    }

    async fn unshare(
        &mut self,
        registry: &Registry,
        request: &Self::Unshare,
    ) -> Result<Self::UnshareOutput, SvcError> {
        let node = registry.node_wrapper(&request.node).await?;

        if let Some(replica) = self {
            let status = registry.get_replica(&request.uuid).await?;
            let spec_clone = replica
                .start_update(registry, &status, ReplicaOperation::Unshare)
                .await?;

            let result = node.unshare_replica(request).await;
            replica.complete_update(registry, result, spec_clone).await
        } else {
            node.unshare_replica(request).await
        }
    }
}

#[async_trait::async_trait]
impl ResourceOwnerUpdate for OperationGuardArc<ReplicaSpec> {
    type Update = ReplicaOwners;

    async fn remove_owners(
        &mut self,
        registry: &Registry,
        request: &Self::Update,
        update_on_commit: bool,
    ) -> Result<(), SvcError> {
        // we don't really need the state, this is a configuration-only change.
        let state = Default::default();
        let mut current = self.lock().owners.clone();
        current.disown(request);
        let spec_clone = self
            .start_update(registry, &state, ReplicaOperation::OwnerUpdate(current))
            .await?;

        match self.complete_update(registry, Ok(()), spec_clone).await {
            Ok(_) => Ok(()),
            Err(SvcError::Store { .. }) if update_on_commit => {
                self.lock().owners.disown(request);
                self.update();
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

impl OperationGuardArc<ReplicaSpec> {
    /// Faults this replica.
    /// The replica is first removed from the nexus, which will let us know if it's safe to destroy
    /// it.
    /// Then we can destroy the replica knowing it's not integral part of a volume.
    #[tracing::instrument(level = "info", skip(self, nexus, registry), fields(volume.uuid = %self.uuid(), replica.uuid = %self.uuid()))]
    pub(crate) async fn fault(
        &mut self,
        nexus: &mut OperationGuardArc<NexusSpec>,
        registry: &Registry,
    ) -> Result<(), SvcError> {
        // First we must remove the child from thus nexus, thus rendering it a "non-healthy" child.
        // The nexus should fail the removal if there are no other healthy children.
        if let Some(uri) = nexus.as_ref().replica_uuid_uri(self.uuid()) {
            let nexus_node = nexus.as_ref().node.clone();
            let nexus_uuid = nexus.uuid().clone();

            nexus
                .remove_child(
                    registry,
                    &RemoveNexusChild {
                        node: nexus_node,
                        nexus: nexus_uuid,
                        uri: uri.uri().clone(),
                    },
                )
                .await?;
        }

        let replica = registry.get_replica(self.uuid()).await?;

        // Now it's safe to delete the replica.
        let request = destroy_replica_request(
            self.as_ref(),
            &replica.node,
            ReplicaOwners::new_disown_all(),
        );
        self.destroy(registry, &request).await?;

        Ok(())
    }
}

fn destroy_replica_request(
    spec: &ReplicaSpec,
    node: &NodeId,
    disowners: ReplicaOwners,
) -> DestroyReplica {
    let pool_id = match spec.pool.clone() {
        PoolRef::Named(id) => id,
        PoolRef::Uuid(id, _) => id,
    };
    let pool_uuid = match spec.pool.clone() {
        PoolRef::Named(_) => None,
        PoolRef::Uuid(_, uuid) => Some(uuid),
    };
    DestroyReplica {
        node: node.clone(),
        pool_id,
        pool_uuid,
        uuid: spec.uuid.clone(),
        name: spec.name.clone().into(),
        disowners,
    }
}
