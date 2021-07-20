use crate::core::{registry::Registry, wrapper::*};
use common::errors::{NodeNotFound, ReplicaNotFound, SvcError, SvcError::PoolNotFound};
use common_lib::types::v0::message_bus::{NodeId, Pool, PoolId, Replica, ReplicaId};
use snafu::OptionExt;

/// Pool helpers
impl Registry {
    /// Get all pools from node `node_id` or from all nodes
    pub(crate) async fn get_node_opt_pools(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Pool>, SvcError> {
        match node_id {
            None => self.get_pools_inner().await,
            Some(node_id) => self.get_node_pools(&node_id).await,
        }
    }

    /// Get pool wrappers for the pool ID.
    pub(crate) async fn get_node_pool_wrapper(
        &self,
        pool_id: PoolId,
    ) -> Result<PoolWrapper, SvcError> {
        let nodes = self.get_nodes_wrapper().await;
        for node in nodes {
            if let Some(pool) = node.pool_wrapper(&pool_id).await {
                return Ok(pool);
            }
        }
        Err(PoolNotFound { pool_id })
    }

    /// Get all pools
    pub(crate) async fn get_pools_inner(&self) -> Result<Vec<Pool>, SvcError> {
        let nodes = self.get_nodes_wrapper().await;
        let mut pools = vec![];
        for node in nodes {
            pools.append(&mut node.pools().await)
        }
        Ok(pools)
    }

    /// Get all pools from node `node_id`
    pub(crate) async fn get_node_pools(&self, node_id: &NodeId) -> Result<Vec<Pool>, SvcError> {
        let node = self.get_node_wrapper(node_id).await.context(NodeNotFound {
            node_id: node_id.clone(),
        })?;
        Ok(node.pools().await)
    }
}

/// Replica helpers
impl Registry {
    /// Get all replicas
    pub(crate) async fn get_replicas(&self) -> Result<Vec<Replica>, SvcError> {
        let nodes = self.get_nodes_wrapper().await;
        let mut replicas = vec![];
        for node in nodes {
            replicas.append(&mut node.replicas().await);
        }
        Ok(replicas)
    }

    /// Get replica `replica_id`
    pub(crate) async fn get_replica(&self, replica_id: &ReplicaId) -> Result<Replica, SvcError> {
        let replicas = self.get_replicas().await?;
        let replica = replicas
            .iter()
            .find(|r| &r.uuid == replica_id)
            .context(ReplicaNotFound {
                replica_id: replica_id.clone(),
            })?;
        Ok(replica.clone())
    }

    /// Get all replicas from node `node_id`
    pub(crate) async fn get_node_replicas(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<Replica>, SvcError> {
        let node = self.get_node_wrapper(node_id).await.context(NodeNotFound {
            node_id: node_id.clone(),
        })?;
        Ok(node.replicas().await)
    }
}
