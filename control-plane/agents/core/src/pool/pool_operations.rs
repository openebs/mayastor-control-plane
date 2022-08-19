use crate::controller::{
    operations::ResourceLifecycle,
    registry::Registry,
    specs::{GuardedOperationsHelper, OperationSequenceGuard},
    wrapper::ClientOps,
};
use common::errors::{SvcError, SvcError::CordonedNode};
use common_lib::types::v0::{
    store::{pool::PoolSpec, OperationGuardArc},
    transport::{CreatePool, DestroyPool, Pool},
};

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<PoolSpec> {
    type Create = CreatePool;
    type CreateOutput = Pool;
    type Destroy = DestroyPool;

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
        let pool = specs
            .get_or_create_pool(request)
            .operation_guard_wait()
            .await?;
        let _ = pool.start_create(registry, request).await?;

        let result = node.create_pool(request).await;

        let pool_state = pool.complete_create(result, registry).await?;
        let spec = pool.lock().clone();
        Ok(Pool::new(spec, pool_state))
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        // what if the node is never coming back?
        // do we need a way to forcefully "delete" things?
        let node = registry.get_node_wrapper(&request.node).await?;

        self.start_destroy(registry).await?;

        let result = node.destroy_pool(request).await;
        self.complete_destroy(result, registry).await
    }
}

#[async_trait::async_trait]
impl ResourceLifecycle for Option<OperationGuardArc<PoolSpec>> {
    type Create = CreatePool;
    type CreateOutput = Pool;
    type Destroy = DestroyPool;

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
        if let Some(pool) = self {
            pool.destroy(registry, request).await
        } else {
            // what if the node is never coming back?
            // do we need a way to forcefully "delete" things?
            let node = registry.get_node_wrapper(&request.node).await?;

            node.destroy_pool(request).await
        }
    }
}
