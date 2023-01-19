use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceLifecycle, ResourceOwnerUpdate, ResourceSharing},
        operations_helper::{GuardedOperationsHelper, OperationSequenceGuard},
        OperationGuardArc, UpdateInnerValue,
    },
    wrapper::ClientOps,
};
use agents::errors::{SvcError, SvcError::CordonedNode};
use common_lib::types::v0::{
    store::replica::{ReplicaOperation, ReplicaSpec},
    transport::{
        CreateReplica, DestroyReplica, Replica, ReplicaOwners, ShareReplica, UnshareReplica,
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
        replica.complete_create(result, registry).await
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

            let result = node.destroy_replica(request).await;
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
