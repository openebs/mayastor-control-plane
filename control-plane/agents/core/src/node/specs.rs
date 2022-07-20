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
        let (changed, node) = {
            let mut specs = self.write();
            match specs.nodes.get(&node.id) {
                Some(node_spec) => {
                    let mut node_spec = node_spec.lock();
                    let changed = node_spec.endpoint() != node.grpc_endpoint;

                    node_spec.set_endpoint(node.grpc_endpoint.clone());
                    (changed, node_spec.clone())
                }
                None => {
                    let node = NodeSpec::new(
                        node.id.clone(),
                        node.grpc_endpoint.clone(),
                        NodeLabels::new(),
                        None,
                    );
                    specs.nodes.insert(node.clone());
                    (true, node)
                }
            }
        };
        if changed {
            registry.store_obj(&node).await?;
        }
        Ok(node)
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
    fn get_locked_nodes(&self) -> Vec<Arc<Mutex<NodeSpec>>> {
        self.read().nodes.to_vec()
    }

    /// Get all node specs cloned
    pub(crate) fn get_nodes(&self) -> Vec<NodeSpec> {
        self.get_locked_nodes()
            .into_iter()
            .map(|n| n.lock().clone())
            .collect()
    }

    /// Cordon the node with the given ID.
    /// Return the NodeSpec after cordoning.
    pub(crate) async fn cordon_node(
        &self,
        registry: &Registry,
        node_id: &NodeId,
        label: String,
    ) -> Result<NodeSpec, SvcError> {
        let node = self.get_locked_node(node_id)?;
        let cordoned_node_spec = {
            let mut locked_node = node.lock();
            // Do not allow the same label to be applied more than once.
            if locked_node.cordon_labels().contains(&label) {
                return Err(SvcError::CordonLabel {
                    node_id: node_id.to_string(),
                    label,
                });
            }
            locked_node.cordon(label);
            locked_node.clone()
        };
        registry.store_obj(&cordoned_node_spec).await?;
        Ok(cordoned_node_spec.clone())
    }

    /// Uncordon the node with the given ID.
    /// Return the NodeSpec after uncordoning.
    pub(crate) async fn uncordon_node(
        &self,
        registry: &Registry,
        node_id: &NodeId,
        label: String,
    ) -> Result<NodeSpec, SvcError> {
        let node = self.get_locked_node(node_id)?;
        // Return an error if the uncordon label doesn't exist.
        if !node.lock().cordon_labels().contains(&label) {
            return Err(SvcError::UncordonLabel {
                node_id: node_id.to_string(),
                label,
            });
        }
        let uncordoned_node_spec = {
            let mut locked_node = node.lock();
            locked_node.uncordon(label);
            locked_node.clone()
        };
        registry.store_obj(&uncordoned_node_spec).await?;
        Ok(uncordoned_node_spec.clone())
    }

    /// Get all cordoned nodes.
    pub fn get_cordoned_nodes(&self) -> Vec<NodeSpec> {
        self.read()
            .nodes
            .to_vec()
            .into_iter()
            .filter_map(|node_spec| {
                if node_spec.lock().cordoned() {
                    Some(node_spec.lock().clone())
                } else {
                    None
                }
            })
            .collect()
    }
}
