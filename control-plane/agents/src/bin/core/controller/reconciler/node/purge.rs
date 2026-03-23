use crate::controller::{
    resources::{operations::ResourceLifecycle, OperationGuardArc, ResourceUid},
    task_poller::{PollContext, PollResult, PollTimer, PollerState, TaskPoller},
};
use stor_port::types::v0::{store::node::NodeSpec, transport::DestroyNode};

/// Node purge reconciler — resumes interrupted node purge operations.
///
/// When a node delete (purge) is interrupted mid-way (e.g. control-plane restart),
/// the node spec remains in `Purging` state. This reconciler detects such nodes
/// and resumes the purge with all accepts set to true (matching the pool purge
/// reconciler pattern).
#[derive(Debug)]
pub(super) struct NodePurgeReconciler {
    counter: PollTimer,
}

impl NodePurgeReconciler {
    /// Return a new `Self`.
    pub(super) fn new() -> Self {
        Self {
            counter: PollTimer::from(1),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for NodePurgeReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let nodes = context.specs().nodes();
        let mut results = Vec::with_capacity(nodes.len());

        for node in nodes {
            if !node.status.purging() {
                continue;
            }
            let mut node_guard = context.registry().specs().guarded_node(node.id()).await?;
            results.push(resume_node_purge(context, &mut node_guard).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}

/// Resume an interrupted node purge.
///
/// The node is already in Purging state. `purge_node` detects the resume case,
/// skips pre-flight validation, and re-runs the destroy lifecycle with all
/// accepts set to true.
#[tracing::instrument(skip_all, level = "trace", fields(node.id = %node_guard.uid(), request.reconcile = true))]
async fn resume_node_purge(
    context: &PollContext,
    node_guard: &mut OperationGuardArc<NodeSpec>,
) -> PollResult {
    let node_id = node_guard.uid().clone();

    // Build a request with all accepts=true — validation was already done
    // on the original request before the crash.
    let request = DestroyNode::purge(node_id.clone())
        .with_accept(true)
        .with_accept_volume_loss(true)
        .with_accept_snapshot_loss(true);

    node_guard.destroy(context.registry(), &request).await?;

    tracing::info!(
        node.id = %node_id,
        "Purging node reconciled successfully"
    );
    PollResult::Ok(PollerState::Idle)
}
