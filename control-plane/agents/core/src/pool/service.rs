use crate::core::registry::Registry;
use common::errors::SvcError;
use mbus_api::v0::{
    CreatePool,
    CreateReplica,
    DestroyPool,
    DestroyReplica,
    Filter,
    GetPools,
    GetReplicas,
    Pool,
    Pools,
    Replica,
    Replicas,
    ShareReplica,
    UnshareReplica,
};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            registry,
        }
    }

    /// Get pools according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_pools(
        &self,
        request: &GetPools,
    ) -> Result<Pools, SvcError> {
        let filter = request.filter.clone();
        let mut pools = match filter {
            Filter::None => self.registry.get_node_opt_pools(None).await?,
            Filter::Node(node_id) => {
                self.registry.get_node_pools(&node_id).await?
            }
            Filter::NodePool(node_id, pool_id) => {
                let pool = self
                    .registry
                    .get_node_pool_wrapper(&node_id, &pool_id)
                    .await?;
                vec![pool.into()]
            }
            Filter::Pool(pool_id) => {
                let pool = self.registry.get_pool_wrapper(&pool_id).await?;
                vec![pool.into()]
            }
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
        };
        let specs = self.registry.specs.read().await;
        let pool_specs = specs.get_created_pools().await;
        pool_specs.iter().for_each(|spec| {
            // if we can't find a pool state, then report the pool with unknown
            // state
            if !pools.iter().any(|p| p.id == spec.id) {
                pools.push(Pool::from(spec));
            }
        });

        Ok(Pools(pools))
    }

    /// Get replicas according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_replicas(
        &self,
        request: &GetReplicas,
    ) -> Result<Replicas, SvcError> {
        let filter = request.filter.clone();
        let mut replicas = match filter {
            Filter::None => self.registry.get_node_opt_replicas(None).await?,
            Filter::Node(node_id) => {
                self.registry.get_node_opt_replicas(Some(node_id)).await?
            }
            Filter::NodePool(node_id, pool_id) => {
                let pool = self
                    .registry
                    .get_node_pool_wrapper(&node_id, &pool_id)
                    .await?;
                pool.into()
            }
            Filter::Pool(pool_id) => {
                let pool = self.registry.get_pool_wrapper(&pool_id).await?;
                pool.into()
            }
            Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
                vec![
                    self.registry
                        .get_node_pool_replica(&node_id, &pool_id, &replica_id)
                        .await?,
                ]
            }
            Filter::NodeReplica(node_id, replica_id) => {
                vec![
                    self.registry
                        .get_node_replica(&node_id, &replica_id)
                        .await?,
                ]
            }
            Filter::PoolReplica(pool_id, replica_id) => {
                vec![
                    self.registry
                        .get_pool_replica(&pool_id, &replica_id)
                        .await?,
                ]
            }
            Filter::Replica(replica_id) => {
                vec![self.registry.get_replica(&replica_id).await?]
            }
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
        };
        let specs = self.registry.specs.read().await;
        let replica_specs = specs.get_created_replicas().await;
        let pool_specs = specs.get_created_pools().await;
        replica_specs.iter().for_each(|spec| {
            // if we can't find a pool state, then report the pool with unknown
            // state
            if !replicas.iter().any(|r| r.uuid == spec.uuid) {
                let mut replica = Replica::from(spec);
                if let Some(pool) =
                    pool_specs.iter().find(|p| p.id == replica.pool)
                {
                    replica.node = pool.node.clone();
                }
                replicas.push(replica);
            }
        });
        Ok(Replicas(replicas))
    }

    /// Create pool
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_pool(
        &self,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        self.registry
            .specs
            .create_pool(&self.registry, request)
            .await
    }

    /// Destroy pool
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_pool(
        &self,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_pool(&self.registry, request)
            .await
    }

    /// Create replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        self.registry
            .specs
            .create_replica(&self.registry, request)
            .await
    }

    /// Destroy replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_replica(&self.registry, request, true)
            .await
    }

    /// Share replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        self.registry
            .specs
            .share_replica(&self.registry, request)
            .await
    }

    /// Unshare replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .unshare_replica(&self.registry, request)
            .await
    }
}
