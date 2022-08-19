use crate::controller::{
    operations::{ResourceLifecycle, ResourceSharing},
    registry::Registry,
    specs::{GuardedOperationsHelper, OperationSequenceGuard},
    wrapper::ClientOps,
};
use common::errors::{SvcError, SvcError::CordonedNode};
use common_lib::types::v0::{
    store::{
        replica::{ReplicaOperation, ReplicaSpec},
        OperationGuardArc,
    },
    transport::{CreateReplica, DestroyReplica, Replica, ShareReplica, UnshareReplica},
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

        let node = registry.get_node_wrapper(&request.node).await?;

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
        let node = registry.get_node_wrapper(&request.node).await?;

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
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(replica) = self {
            let status = registry.get_replica(&request.uuid).await?;
            let spec_clone = replica
                .start_update(registry, &status, ReplicaOperation::Share(request.protocol))
                .await?;

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
        let node = registry.get_node_wrapper(&request.node).await?;

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
