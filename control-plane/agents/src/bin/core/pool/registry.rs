use crate::controller::{
    registry::Registry,
    wrapper::{GetterOps, *},
};
use agents::errors::{self, SvcError, SvcError::PoolNotFound};
use common_lib::types::v0::transport::{NodeId, Pool, PoolId, PoolState, Replica, ReplicaId};
use snafu::OptionExt;

/// Pool helpers
impl Registry {
    /// Get all pools from node `node_id` or from all nodes
    pub(crate) async fn get_node_opt_pools(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Pool>, SvcError> {
        match node_id {
            None => {
                let mut pools = vec![];
                let pools_from_state = self.pool_states_inner().await.into_iter().map(|state| {
                    let spec = self.specs().pool(&state.id).ok();
                    Pool::from_state(state, spec)
                });

                pools.extend(pools_from_state);

                let pools_from_spec = self
                    .specs()
                    .pools()
                    .into_iter()
                    .filter(|p| !pools.iter().any(|i| i.id() == &p.id))
                    .map(Pool::from_spec)
                    .collect::<Vec<_>>();

                pools.extend(pools_from_spec);
                Ok(pools)
            }
            Some(node_id) => {
                let mut pools = vec![];
                let pools_from_state = self
                    .get_node_pools(&node_id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|state| {
                        let spec = self.specs().pool(&state.id).ok();
                        Pool::from_state(state, spec)
                    });

                pools.extend(pools_from_state);

                let pools_from_spec = self
                    .specs()
                    .pools()
                    .into_iter()
                    .filter(|p| p.node == node_id)
                    .filter(|p| !pools.iter().any(|i| i.id() == &p.id))
                    .map(Pool::from_spec)
                    .collect::<Vec<_>>();

                pools.extend(pools_from_spec);
                Ok(pools)
            }
        }
    }

    /// Get pool wrappers for the pool ID.
    pub(crate) async fn get_node_pool_wrapper(
        &self,
        pool_id: PoolId,
    ) -> Result<PoolWrapper, SvcError> {
        let nodes = self.node_wrappers().await;
        for node in nodes {
            if let Some(pool) = node.pool_wrapper(&pool_id).await {
                return Ok(pool);
            }
        }
        Err(PoolNotFound { pool_id })
    }

    /// Get all pools
    pub(crate) async fn pool_states_inner(&self) -> Vec<PoolState> {
        let nodes = self.node_wrappers().await;
        let mut pools = Vec::with_capacity(nodes.len());
        for node in nodes {
            pools.append(&mut node.pools().await)
        }
        pools
    }

    /// Get all pool wrappers
    pub(crate) async fn get_pool_wrappers(&self) -> Vec<PoolWrapper> {
        let nodes = self.node_wrappers().await;
        let mut pools = Vec::with_capacity(nodes.len());
        for node in nodes {
            pools.append(&mut node.pool_wrappers().await)
        }
        pools
    }

    /// Get all pools from node `node_id`
    pub(crate) async fn get_node_pools(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<PoolState>, SvcError> {
        let node = self.node_wrapper(node_id).await?;
        Ok(node.pools().await)
    }

    /// Get the pool state for the specified id.
    pub(crate) async fn get_pool_state(&self, id: &PoolId) -> Result<PoolState, SvcError> {
        let pools = self.get_pool_wrappers().await;
        let pool_wrapper = pools.iter().find(|p| &p.id == id);
        pool_wrapper
            .context(errors::PoolNotFound {
                pool_id: id.to_owned(),
            })
            .map(|p| p.state().clone())
    }

    /// Get the pool object corresponding to the id.
    pub(crate) async fn get_pool(&self, id: &PoolId) -> Result<Pool, SvcError> {
        Pool::try_new(
            self.specs().pool(id).ok(),
            self.get_pool_state(id).await.ok(),
        )
        .ok_or(PoolNotFound {
            pool_id: id.to_owned(),
        })
    }
}

/// Replica helpers
impl Registry {
    /// Get all replicas
    pub(crate) async fn get_replicas(&self) -> Vec<Replica> {
        let nodes = self.node_wrappers().await;
        let mut replicas = vec![];
        for node in nodes {
            replicas.append(&mut node.replicas().await);
        }
        replicas
    }

    /// Get replica `replica_id`
    pub(crate) async fn get_replica(&self, replica_id: &ReplicaId) -> Result<Replica, SvcError> {
        let nodes = self.node_wrappers().await;
        for node in nodes {
            if let Some(replica) = node.replica(replica_id).await {
                return Ok(replica);
            }
        }
        Err(SvcError::ReplicaNotFound {
            replica_id: replica_id.clone(),
        })
    }

    /// Get all replicas from node `node_id`
    pub(crate) async fn get_node_replicas(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<Replica>, SvcError> {
        let node = self.node_wrapper(node_id).await?;
        Ok(node.replicas().await)
    }
}
