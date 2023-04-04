use crate::controller::{registry::Registry, wrapper::NodeWrapper};
use agents::errors::SvcError;
use stor_port::types::v0::transport::{NodeId, NodeState, Register};

use std::sync::Arc;
use tokio::sync::RwLock;

impl Registry {
    /// Get all node wrappers
    pub(crate) async fn node_wrappers(&self) -> Vec<Arc<RwLock<NodeWrapper>>> {
        let nodes = self.nodes().read().await;
        nodes.values().cloned().collect()
    }

    /// Get all node states
    pub(crate) async fn node_states(&self) -> Vec<NodeState> {
        let nodes = self.nodes().read().await;
        let mut nodes_vec = vec![];
        for node in nodes.values() {
            nodes_vec.push(node.read().await.node_state().clone());
        }
        nodes_vec
    }

    /// Get node wrapper by its `NodeId`
    pub(crate) async fn node_wrapper(
        &self,
        node_id: &NodeId,
    ) -> Result<Arc<RwLock<NodeWrapper>>, SvcError> {
        match self.nodes().read().await.get(node_id).cloned() {
            None => {
                if self.specs().node(node_id).is_ok() {
                    Err(SvcError::NodeNotOnline {
                        node: node_id.to_owned(),
                    })
                } else {
                    Err(SvcError::NodeNotFound {
                        node_id: node_id.to_owned(),
                    })
                }
            }
            Some(node) => Ok(node),
        }
    }

    /// Get node state by its `NodeId`
    pub(crate) async fn node_state(&self, node_id: &NodeId) -> Result<NodeState, SvcError> {
        match self.nodes().read().await.get(node_id).cloned() {
            None => {
                if self.specs().node(node_id).is_ok() {
                    Err(SvcError::NodeNotOnline {
                        node: node_id.to_owned(),
                    })
                } else {
                    Err(SvcError::NodeNotFound {
                        node_id: node_id.to_owned(),
                    })
                }
            }
            Some(node) => Ok(node.read().await.node_state().clone()),
        }
    }

    /// Register new NodeSpec for the given `Register` Request
    pub(super) async fn register_node_spec(&self, request: &Register) {
        let automatic = self.config().node_registration().automatic();
        if automatic {
            self.specs().register_node(self, request).await.ok();
        }
    }
}
