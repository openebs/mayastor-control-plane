use crate::controller::{
    registry::Registry,
    resources::{
        operations_helper::{
            GuardedOperationsHelper, OperationSequenceGuard, ResourceSpecs, ResourceSpecsLocked,
            SpecOperationsHelper,
        },
        OperationGuardArc, ResourceMutex,
    },
};
use agents::errors::{NodeNotFound, SvcError};
use snafu::OptionExt;
use std::sync::Arc;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            node::{NodeLabelOp, NodeLabels, NodeOperation, NodeSpec, NodeUnLabelOp},
            OperationMode, SpecStatus, SpecTransaction,
        },
        transport::{Deregister, NodeBugFix, NodeId, Register},
    },
};

impl ResourceSpecsLocked {
    /// Create a node spec for the register request
    pub(crate) async fn register_node(
        &self,
        registry: &Registry,
        node: &Register,
    ) -> Result<bool, SvcError> {
        let _registration = registry.registration_guard(node.id.as_str()).await;
        // A routine heartbeat does not need to wait for cordon, drain or purge.
        let unchanged = {
            let specs = self.read();
            !specs.unconfirmed_nodes.contains(&node.id)
                && specs.nodes.get(&node.id).is_some_and(|spec| {
                    let spec = spec.lock();
                    spec.endpoint() == node.grpc_endpoint
                        && spec.node_nqn() == &node.node_nqn
                        && spec.features() == &node.features
                        && spec.bugfixes() == &node.bugfixes
                        && spec.version() == &node.version
                        && spec.transport_caps() == &node.transport_caps
                        && spec.nvmf_target() == &node.nvmf_target
                        && !spec.is_shutdown()
                })
        };
        if unchanged {
            return Ok(false);
        }

        match self.guarded_node(&node.id).await {
            Ok(guard) => {
                let update = |spec: &mut NodeSpec| {
                    spec.set_endpoint(node.grpc_endpoint);
                    spec.set_nqn(node.node_nqn.clone());
                    spec.set_features(node.features.clone());
                    spec.set_bugfixes(node.bugfixes.clone());
                    spec.set_version(node.version.clone());
                    spec.set_transport_caps(node.transport_caps.clone());
                    spec.set_nvmf_target(node.nvmf_target.clone());
                    spec.set_shutdown(false);
                };
                let mut next = guard.as_ref().clone();
                update(&mut next);
                self.store_node_registration(registry, &next).await?;
                // Runtime resource counts can change independently during the write.
                update(&mut guard.lock());
            }
            Err(SvcError::NodeNotFound { .. }) => {
                let spec = NodeSpec::new(
                    node.id.clone(),
                    node.grpc_endpoint,
                    NodeLabels::new(),
                    None,
                    node.node_nqn.clone(),
                    node.features.clone(),
                    node.bugfixes.clone(),
                    node.version.clone(),
                    false,
                )
                .with_transport_caps(node.transport_caps.clone())
                .with_nvmf_target(node.nvmf_target.clone());
                registry.store_obj(&spec).await?;
                self.write().nodes.insert(spec);
            }
            Err(error) => return Err(error),
        }
        Ok(true)
    }

    /// Deregister the node by marking it as shutdown.
    pub(crate) async fn deregister_node(
        &self,
        registry: &Registry,
        node: &Deregister,
    ) -> Result<(), SvcError> {
        let _registration = registry.registration_guard(node.id.as_str()).await;
        let Some(spec) = self.read().nodes.get(&node.id).cloned() else {
            return Ok(());
        };
        // The io-engine may only send this once as it exits. Let a drain or purge finish.
        let guard = spec.shutdown_guard().await;
        if !self.current_node(&guard)
            || (guard.as_ref().is_shutdown() && !self.read().unconfirmed_nodes.contains(&node.id))
        {
            return Ok(());
        }
        let mut next = guard.as_ref().clone();
        next.set_shutdown(true);
        self.store_node_registration(registry, &next).await?;
        guard.lock().set_shutdown(true);
        Ok(())
    }

    /// A write can reach etcd even if its response never reaches the caller.
    async fn store_node_registration(
        &self,
        registry: &Registry,
        spec: &NodeSpec,
    ) -> Result<(), SvcError> {
        self.write().unconfirmed_nodes.insert(spec.id().clone());
        registry.store_obj(spec).await?;
        self.write().unconfirmed_nodes.remove(spec.id());
        Ok(())
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
        let guard = node.operation_guard_wait().await?;
        // A purge can remove this spec while the caller waits for the guard.
        if self.current_node(&guard) {
            Ok(guard)
        } else {
            Err(SvcError::NodeNotFound {
                node_id: node_id.clone(),
            })
        }
    }

    fn current_node(&self, guard: &OperationGuardArc<NodeSpec>) -> bool {
        self.read()
            .nodes
            .get(guard.as_ref().id())
            .is_some_and(|node| Arc::ptr_eq(node.immutable_ref(), guard.immutable_ref()))
    }

    /// Get all locked node specs
    pub(crate) fn nodes_rsc(&self) -> Vec<ResourceMutex<NodeSpec>> {
        self.read().nodes.to_vec()
    }

    /// Get all node specs cloned
    pub(crate) fn nodes(&self) -> Vec<NodeSpec> {
        self.nodes_rsc()
            .into_iter()
            .map(|n| n.lock().clone())
            .collect()
    }

    /// Worker that reconciles dirty NodeSpec's with the persistent store.
    /// This is useful when node operations are performed but we fail to
    /// update the spec with the persistent store.
    pub(crate) async fn reconcile_dirty_nodes(&self, registry: &Registry) -> bool {
        let mut pending_ops = false;

        let nodes = self.nodes_rsc();
        for node in nodes {
            // Skip nodes with no pending operation to avoid unnecessary guard
            // acquisition. This keeps the reconciler low-cost for healthy nodes.
            if !node.lock().has_pending_op() {
                continue;
            }
            if let Ok(mut guard) = node.operation_guard() {
                if self.current_node(&guard) && !guard.handle_incomplete_ops(registry).await {
                    pending_ops = true;
                }
            }
        }
        pending_ops
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

    /// Remove the Node `id` from the spec list.
    pub(crate) fn remove_node(&self, id: &NodeId) {
        let mut specs = self.write();
        specs.nodes.remove(id);
        specs.unconfirmed_nodes.remove(id);
    }

    /// On replica creation, update the node's replica count.
    pub(crate) fn node_on_repl_create(&self, node_id: &NodeId) {
        let Ok(node) = self.node_rsc(node_id) else {
            return;
        };

        let mut node = node.lock();
        node.metadata.runtime.replica_count += 1;
    }
    /// On replica deletion, update the node's replica count.
    pub(crate) fn node_on_repl_destroy(&self, node_id: &NodeId) {
        let Ok(node) = self.node_rsc(node_id) else {
            return;
        };

        let mut node = node.lock();
        node.metadata.runtime.replica_count = node.metadata.runtime.replica_count.saturating_sub(1);
    }

    /// On replica snapshot creation, update the node's snapshot count.
    pub(crate) fn node_on_snap_create(&self, node_id: &NodeId) {
        let Ok(node) = self.node_rsc(node_id) else {
            return;
        };

        let mut node = node.lock();
        node.metadata.runtime.snapshot_count += 1;
    }
    /// On replica snapshot deletion, update the node's snapshot count.
    pub(crate) fn node_on_snap_destroy(&self, node_id: &NodeId) {
        let Ok(node) = self.node_rsc(node_id) else {
            return;
        };

        let mut node = node.lock();
        node.metadata.runtime.snapshot_count =
            node.metadata.runtime.snapshot_count.saturating_sub(1);
    }

    /// On pool creation, update the node's pool count.
    pub(crate) fn on_pool_create(&self, node_id: &NodeId) {
        let Ok(node) = self.node_rsc(node_id) else {
            return;
        };

        let mut node = node.lock();
        node.metadata.runtime.pool_count += 1;
    }
    /// On pool deletion, update the node's pool count.
    pub(crate) fn on_pool_destroy(&self, node_id: &NodeId) {
        let Ok(node) = self.node_rsc(node_id) else {
            return;
        };

        let mut node = node.lock();
        node.metadata.runtime.pool_count = node.metadata.runtime.pool_count.saturating_sub(1);
    }
}

impl ResourceMutex<NodeSpec> {
    /// Wait for the same exclusive guard used by drain, including during a long republish.
    async fn shutdown_guard(&self) -> OperationGuardArc<NodeSpec> {
        loop {
            let mut waiter = match self.try_guard_or_register(OperationMode::Exclusive) {
                Ok(guard) => return guard,
                Err(waiter) => waiter,
            };
            // A waiter ahead of us can be cancelled after it was woken. Recheck in that
            // case without adding another entry to the FIFO while the guard is held.
            while tokio::time::timeout(std::time::Duration::from_secs(1), &mut waiter)
                .await
                .is_err()
            {
                if let Ok(guard) = self.operation_guard() {
                    return guard;
                }
            }
        }
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

    fn remove_spec(&self, registry: &Registry) {
        let id = self.lock().id().clone();
        registry.specs().remove_node(&id);
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
                        resource: ResourceKind::Node,
                        id: self.id().to_string(),
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
                match self.has_labels_key(label_key) {
                    false => Err(SvcError::LabelNotFound {
                        resource: ResourceKind::Node,
                        id: self.id().to_string(),
                        label_key: label_key.to_string(),
                    }),
                    true => {
                        self.start_op(op);
                        Ok(())
                    }
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
        self.start_op(NodeOperation::Destroy);
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
        self.status.clone()
    }
    fn set_status(&mut self, status: SpecStatus<Self::Status>) {
        self.status = status;
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}
