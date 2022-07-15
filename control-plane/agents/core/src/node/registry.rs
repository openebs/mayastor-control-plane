use crate::core::{registry::Registry, wrapper::NodeWrapper};
use common::errors::SvcError;
use common_lib::types::v0::transport::{DrainStatus, NodeId, NodeState, Register};

use std::sync::Arc;
use tokio::sync::RwLock;

impl Registry {
    /// Get all node wrappers
    pub(crate) async fn get_node_wrappers(&self) -> Vec<Arc<RwLock<NodeWrapper>>> {
        let nodes = self.nodes().read().await;
        nodes.values().cloned().collect()
    }

    /// Get all node states
    pub(crate) async fn get_node_states(&self) -> Result<Vec<NodeState>, SvcError> {
        let nodes = self.nodes().read().await;
        let mut nodes_vec = vec![];
        for node in nodes.values() {
            let mut copy_node_state = node.read().await.node_state().clone();
            copy_node_state.drain_status =
                match self.get_node_drain_status(copy_node_state.id()).await {
                    Ok(ds) => ds,
                    Err(e) => {
                        return Err(e);
                    }
                };
            nodes_vec.push(copy_node_state);
        }
        Ok(nodes_vec)
    }

    /// Get node wrapper by its `NodeId`
    pub(crate) async fn get_node_wrapper(
        &self,
        node_id: &NodeId,
    ) -> Result<Arc<RwLock<NodeWrapper>>, SvcError> {
        match self.nodes().read().await.get(node_id).cloned() {
            None => {
                if self.specs().get_node(node_id).is_ok() {
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
    pub(crate) async fn get_node_state(&self, node_id: &NodeId) -> Result<NodeState, SvcError> {
        match self.nodes().read().await.get(node_id).cloned() {
            None => {
                if self.specs().get_node(node_id).is_ok() {
                    Err(SvcError::NodeNotOnline {
                        node: node_id.to_owned(),
                    })
                } else {
                    Err(SvcError::NodeNotFound {
                        node_id: node_id.to_owned(),
                    })
                }
            }
            Some(node) => {
                let mut copy_node_state = node.read().await.node_state().clone();
                copy_node_state.drain_status = match self.get_node_drain_status(node_id).await {
                    Ok(ds) => ds,
                    Err(e) => {
                        return Err(e);
                    }
                };
                Ok(copy_node_state)
            }
        }
    }

    // deduce the drain state from the presence of drain labels and nexus instances
    async fn get_node_drain_status(&self, node_id: &NodeId) -> Result<DrainStatus, SvcError> {
        let spec = match self.specs().get_node(node_id) {
            Ok(sp) => sp,
            Err(e) => return Err(e),
        };
        let cordoned_for_drain = spec.cordoned_for_drain();
        let mut drain_status = DrainStatus::NotDraining;
        if cordoned_for_drain {
            let nexuses = self.get_node_nexuses(node_id).await;
            if nexuses.unwrap().is_empty() {
                drain_status = DrainStatus::Drained;
            } else {
                drain_status = DrainStatus::Draining;
            }
        }
        Ok(drain_status)
    }

    /// Register new NodeSpec for the given `Register` Request
    pub(super) async fn register_node_spec(&self, request: &Register) {
        if self.config().node_registration().automatic() {
            self.specs().register_node(self, request).await.ok();
        }
    }
}
