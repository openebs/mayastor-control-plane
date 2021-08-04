use crate::core::{registry::Registry, specs::ResourceSpecsLocked};
use common::errors::{NodeNotFound, SvcError};
use common_lib::types::v0::{
    message_bus::{NodeId, Register},
    store::node::{NodeLabels, NodeSpec},
};
use parking_lot::Mutex;
use snafu::OptionExt;
use std::sync::Arc;

impl ResourceSpecsLocked {
    /// Create a node spec for the register request
    pub(crate) async fn register_node(
        &self,
        registry: &Registry,
        node: &Register,
    ) -> Result<NodeSpec, SvcError> {
        let node = {
            let mut specs = self.write();
            match specs.nodes.get(&node.id) {
                Some(node_spec) => {
                    let mut node_spec = node_spec.lock();
                    node_spec.set_endpoint(node.grpc_endpoint.clone());
                    Ok(node_spec.clone())
                }
                None => {
                    let node = NodeSpec::new(
                        node.id.clone(),
                        node.grpc_endpoint.clone(),
                        NodeLabels::new(),
                    );
                    specs.nodes.insert(node.clone());
                    Ok(node)
                }
            }
        };
        if let Ok(node) = &node {
            registry.store_obj(node).await?;
        }
        node
    }

    /// Get node spec by its `NodeId`
    pub(crate) fn get_locked_node(
        &self,
        node_id: &NodeId,
    ) -> Result<Arc<Mutex<NodeSpec>>, SvcError> {
        self.read()
            .nodes
            .get(node_id)
            .cloned()
            .context(NodeNotFound {
                node_id: node_id.to_owned(),
            })
    }

    /// Get cloned node spec by its `NodeId`
    pub(crate) fn get_node(&self, node_id: &NodeId) -> Result<NodeSpec, SvcError> {
        self.get_locked_node(node_id).map(|n| n.lock().clone())
    }

    /// Get all locked node specs
    pub(crate) fn get_locked_nodes(&self) -> Vec<Arc<Mutex<NodeSpec>>> {
        self.read().nodes.to_vec()
    }

    /// Get all node specs cloned
    pub(crate) fn get_nodes(&self) -> Vec<NodeSpec> {
        self.get_locked_nodes()
            .into_iter()
            .map(|n| n.lock().clone())
            .collect()
    }
}
