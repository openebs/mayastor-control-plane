use crate::controller::{
    registry::Registry,
    resources::{
        operations_helper::{OperationSequenceGuard, ResourceSpecsLocked},
        OperationGuardArc, ResourceMutex,
    },
};
use agents::errors::{NodeNotFound, SvcError};
use snafu::OptionExt;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            node::{NodeLabelOp, NodeLabels, NodeOperation, NodeSpec, NodeUnLabelOp},
            SpecStatus, SpecTransaction,
        },
        transport::{NodeBugFix, NodeId, Register},
    },
};

use crate::controller::resources::operations_helper::{
    GuardedOperationsHelper, ResourceSpecs, SpecOperationsHelper,
};

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
                    // todo: add custom PartialEq?
                    let changed = node_spec.endpoint() != node.grpc_endpoint
                        || node_spec.node_nqn() != &node.node_nqn
                        || node_spec.features() != &node.features
                        || node_spec.bugfixes() != &node.bugfixes
                        || node_spec.version() != &node.version;

                    if changed {
                        node_spec.set_endpoint(node.grpc_endpoint);
                        node_spec.set_nqn(node.node_nqn.clone());
                        node_spec.set_features(node.features.clone());
                        node_spec.set_bugfixes(node.bugfixes.clone());
                        node_spec.set_version(node.version.clone());
                    }
                    (changed, node_spec.clone())
                }
                None => {
                    let node = NodeSpec::new(
                        node.id.clone(),
                        node.grpc_endpoint,
                        NodeLabels::new(),
                        None,
                        node.node_nqn.clone(),
                        node.features.clone(),
                        node.bugfixes.clone(),
                        node.version.clone(),
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
    pub(crate) fn node_rsc(&self, node_id: &NodeId) -> Result<ResourceMutex<NodeSpec>, SvcError> {
        self.read()
            .nodes
            .get(node_id)
            .cloned()
            .context(NodeNotFound {
                node_id: node_id.to_owned(),
            })
    }

    /// Get cloned node spec by its `NodeId`
    pub(crate) fn node(&self, node_id: &NodeId) -> Result<NodeSpec, SvcError> {
        self.node_rsc(node_id).map(|n| n.lock().clone())
    }

    /// Get guarded cloned node spec by its `NodeId`
    pub async fn guarded_node(
        &self,
        node_id: &NodeId,
    ) -> Result<OperationGuardArc<NodeSpec>, SvcError> {
        let node = self.node_rsc(node_id)?;
        node.operation_guard_wait().await
    }

    /// Get all locked node specs
    fn nodes_rsc(&self) -> Vec<ResourceMutex<NodeSpec>> {
        self.read().nodes.to_vec()
    }

    /// Get all node specs cloned
    pub(crate) fn nodes(&self) -> Vec<NodeSpec> {
        self.nodes_rsc()
            .into_iter()
            .map(|n| n.lock().clone())
            .collect()
    }

    /// Get all cordoned nodes.
    pub(crate) fn cordoned_nodes(&self) -> Vec<NodeSpec> {
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

impl ResourceSpecs {
    /// Check if the given node has the given fix.
    pub(crate) fn node_has_fix(&self, node_id: &NodeId, fix: &NodeBugFix) -> Result<(), SvcError> {
        if let Some(node) = self.nodes.get(node_id) {
            if !node.lock().has_fix(fix) {
                return Err(SvcError::UpgradeRequiredToRebuild {});
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl GuardedOperationsHelper for OperationGuardArc<NodeSpec> {
    type Create = NodeSpec;
    type Owners = ();
    type Status = ();
    type State = NodeSpec;
    type UpdateOp = NodeOperation;
    type Inner = NodeSpec;

    fn remove_spec(&self, _registry: &Registry) {
        unimplemented!();
    }
}

#[async_trait::async_trait]
impl SpecOperationsHelper for NodeSpec {
    type Create = NodeSpec;
    type Owners = ();
    type Status = ();
    type State = NodeSpec;
    type UpdateOp = NodeOperation;

    async fn start_update_op(
        &mut self,
        _: &Registry,
        _state: &Self::State,
        op: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        match &op {
            NodeOperation::Cordon(label) | NodeOperation::Drain(label) => {
                // Do not allow the same label to be applied more than once.
                if self.has_cordon_label(label) {
                    Err(SvcError::CordonLabel {
                        node_id: self.id().to_string(),
                        label: label.clone(),
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            NodeOperation::Uncordon(label) => {
                // Check that the label is present.
                if !self.has_cordon_label(label) {
                    Err(SvcError::UncordonLabel {
                        node_id: self.id().to_string(),
                        label: label.clone(),
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            NodeOperation::Label(NodeLabelOp { labels, overwrite }) => {
                let (existing, conflict) = self.label_collisions(labels);
                if !*overwrite && !existing.is_empty() {
                    Err(SvcError::LabelsExists {
                        node_id: self.id().to_string(),
                        labels: format!("{existing:?}"),
                        conflict,
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            NodeOperation::Unlabel(NodeUnLabelOp { label_key }) => {
                // Check that the label is present.
                if !self.has_labels_key(label_key) {
                    Err(SvcError::LabelNotFound {
                        node_id: self.id().to_string(),
                        label_key: label_key.to_string(),
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            _ => {
                self.start_op(op);
                Ok(())
            }
        }
    }
    fn start_create_op(&mut self, _request: &Self::Create) {
        unimplemented!();
    }
    fn start_destroy_op(&mut self) {
        unimplemented!();
    }

    fn dirty(&self) -> bool {
        self.has_pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Node
    }
    fn uuid_str(&self) -> String {
        self.id().to_string()
    }
    fn status(&self) -> SpecStatus<Self::Status> {
        SpecStatus::Created(())
    }
    fn set_status(&mut self, _status: SpecStatus<Self::Status>) {
        unimplemented!();
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}
