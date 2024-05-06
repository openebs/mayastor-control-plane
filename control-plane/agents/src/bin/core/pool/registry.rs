use crate::controller::{
    registry::Registry,
    wrapper::{GetterOps, *},
};
use agents::errors::{SvcError, SvcError::PoolNotFound};
use std::collections::HashMap;
use stor_port::types::v0::transport::{
    CtrlPoolState, Labels, NodeId, Pool, PoolId, Replica, ReplicaId,
};

/// Pool helpers
impl Registry {
    pub(crate) async fn get_label_opts_pools(
        &self,
        labels: Option<Labels>,
    ) -> Result<Vec<Pool>, SvcError> {
        let pools = self.get_node_opt_pools(None).await?;
        let pools_after_label_filtering = match labels {
            Some(label) => {
                let mut filtered_pool: Vec<Pool> = vec![];
                for pool in pools.iter() {
                    match pool.spec() {
                        Some(spec) => match spec.labels {
                            Some(pool_labels) => {
                                let pool_label_match =
                                    Self::labels_matched(pool_labels, label.clone())?;
                                if pool_label_match {
                                    filtered_pool.push(pool.clone());
                                }
                            }
                            None => continue,
                        },
                        None => continue,
                    }
                }
                return Ok(filtered_pool);
            }
            None => pools,
        };
        Ok(pools_after_label_filtering)
    }

    pub(crate) fn labels_matched(
        pool_labels: HashMap<String, String>,
        labels: Labels,
    ) -> Result<bool, SvcError> {
        for label in labels.split(',') {
            let [key, value] = label.split('=').collect::<Vec<_>>()[..] else {
                return Err(SvcError::LabelNodeFilter {});
            };
            if pool_labels.get(key) != Some(&value.to_string()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Get all pools from node `node_id` or from all nodes.
    pub(crate) async fn get_node_opt_pools(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Pool>, SvcError> {
        match node_id {
            None => {
                let mut pools = vec![];
                let pools_from_state = self.ctrl_pool_states().await.into_iter().map(|state| {
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
                    .node_pool_wrappers(&node_id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|wrapper| {
                        let spec = self.specs().pool(&wrapper.id).ok();
                        Pool::from_state(wrapper.ctrl_state(), spec)
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
    pub(crate) async fn pool_wrapper(&self, pool_id: &PoolId) -> Result<PoolWrapper, SvcError> {
        let nodes = self.node_wrappers().await;
        for node in nodes {
            if let Some(pool) = node.pool_wrapper(pool_id).await {
                return Ok(pool);
            }
        }
        Err(PoolNotFound {
            pool_id: pool_id.to_owned(),
        })
    }

    /// Get all controller pools states (state + metadata).
    pub(crate) async fn ctrl_pool_states(&self) -> Vec<CtrlPoolState> {
        let nodes = self.node_wrappers().await;
        let mut pools = Vec::with_capacity(nodes.len());
        for node in nodes {
            let pool_w = node.pool_wrappers().await;
            let pool_cs = pool_w.into_iter().map(|p| p.ctrl_state());
            pools.extend(pool_cs);
        }
        pools
    }

    /// Get the `NodeId` where `pool` lives.
    pub(crate) async fn pool_node(&self, pool: &PoolId) -> Option<NodeId> {
        for node in self.node_wrappers().await {
            if let Some(pool) = node.pool(pool).await {
                return Some(pool.node);
            }
        }
        None
    }

    /// Get all pool wrappers.
    pub(crate) async fn pool_wrappers(&self) -> Vec<PoolWrapper> {
        let nodes = self.node_wrappers().await;
        let mut pools = Vec::with_capacity(nodes.len());
        for node in nodes {
            pools.append(&mut node.pool_wrappers().await)
        }
        pools
    }

    /// Get all pools from node `node_id`.
    pub(crate) async fn node_pool_wrappers(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<PoolWrapper>, SvcError> {
        let node = self.node_wrapper(node_id).await?;
        Ok(node.pool_wrappers().await)
    }

    /// Get the pool state for the specified id.
    pub(crate) async fn has_pool_state(&self, id: &PoolId) -> bool {
        let pools = self.pool_wrappers().await;
        pools.iter().any(|p| &p.id == id)
    }

    /// Get the controller pool state for the specified id.
    pub(crate) async fn ctrl_pool_state(&self, id: &PoolId) -> Result<CtrlPoolState, SvcError> {
        let pool_wrapper = self.pool_wrapper(id).await?;
        Ok(pool_wrapper.ctrl_state())
    }

    /// Get the pool object corresponding to the id.
    pub(crate) async fn ctrl_pool(&self, id: &PoolId) -> Result<Pool, SvcError> {
        Pool::try_new(
            self.specs().pool(id).ok(),
            self.ctrl_pool_state(id).await.ok(),
        )
        .ok_or(PoolNotFound {
            pool_id: id.to_owned(),
        })
    }
}

/// Replica helpers
impl Registry {
    /// Get all replicas
    pub(crate) async fn replicas(&self) -> Vec<Replica> {
        let nodes = self.node_wrappers().await;
        let mut replicas = vec![];
        for node in nodes {
            replicas.append(&mut node.replicas().await);
        }
        replicas
    }

    /// Get replica `replica_id`.
    pub(crate) async fn replica(&self, replica_id: &ReplicaId) -> Result<Replica, SvcError> {
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

    /// Get all replicas from node `node_id`.
    pub(crate) async fn node_replicas(&self, node_id: &NodeId) -> Result<Vec<Replica>, SvcError> {
        let node = self.node_wrapper(node_id).await?;
        Ok(node.replicas().await)
    }
}
