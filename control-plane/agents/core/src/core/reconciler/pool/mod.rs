use crate::core::{
    specs::{OperationSequenceGuard, SpecOperations},
    task_poller::{PollContext, PollPeriods, PollResult, PollTimer, PollerState, TaskPoller},
    wrapper::ClientOps,
};
use common_lib::types::v0::{
    message_bus::{CreatePool, NodeStatus},
    store::{pool::PoolSpec, OperationMode, TraceSpan},
};
use parking_lot::Mutex;
use std::sync::Arc;

/// Pool Reconciler loop which:
/// 1. recreates pools which are not present following a mayastor restart
#[derive(Debug)]
pub struct PoolReconciler {
    counter: PollTimer,
}
impl PoolReconciler {
    /// Return new `Self` with the provided period
    pub fn from(period: PollPeriods) -> Self {
        PoolReconciler {
            counter: PollTimer::from(period),
        }
    }
    /// Return new `Self` with the default period
    pub fn new() -> Self {
        Self::from(1)
    }
}

#[async_trait::async_trait]
impl TaskPoller for PoolReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        for pool in context.specs().get_locked_pools() {
            if pool.lock().status().created() {
                results.push(missing_pool_state_reconciler(pool, context).await)
            }
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}

/// If a pool has a spec but not state, it means that the mayastor instance where the pool should
/// exist does not have the pool open.
/// This can happen if the pool is destroyed under the control plane, or if mayastor
/// crashed/restarted.
/// In such a case, we issue a new create pool request against the mayastor instance where the pool
/// should exist.
#[tracing::instrument(skip(pool_spec, context), fields(pool.uuid = %pool_spec.lock().id))]
async fn missing_pool_state_reconciler(
    pool_spec: Arc<Mutex<PoolSpec>>,
    context: &PollContext,
) -> PollResult {
    if !pool_spec.lock().status().created() {
        // nothing to do here
        return PollResult::Ok(PollerState::Idle);
    }
    let pool_id = pool_spec.lock().id.clone();

    if context.registry().get_pool_state(&pool_id).await.is_err() {
        let _guard = match pool_spec.operation_guard(OperationMode::ReconcileStart) {
            Ok(guard) => guard,
            Err(_) => return PollResult::Ok(PollerState::Busy),
        };
        let pool = pool_spec.lock().clone();

        let warn_missing = |pool_spec: &Arc<Mutex<PoolSpec>>, node_status: NodeStatus| {
            let node_id = pool_spec.lock().node.clone();
            pool.trace_span(|| {
                tracing::trace!(
                    node.uuid = %node_id,
                    node.status = %node_status.to_string(),
                    "Attempted to recreate missing pool state, but the node is not online"
                )
            });
        };
        let node = match context.registry().get_node_wrapper(&pool.node).await {
            Ok(node) if !node.read().await.is_online() => {
                let node_status = node.read().await.status().clone();
                warn_missing(&pool_spec, node_status);
                return PollResult::Ok(PollerState::Idle);
            }
            Err(_) => {
                warn_missing(&pool_spec, NodeStatus::Unknown);
                return PollResult::Ok(PollerState::Idle);
            }
            Ok(node) => node,
        };

        pool.warn_span(|| tracing::warn!("Attempting to recreate missing pool"));

        let request = CreatePool::new(&pool.node, &pool.id, &pool.disks, &pool.labels);
        match node.create_pool(&request).await {
            Ok(_) => {
                pool.info_span(|| tracing::info!("Pool successfully recreated"));
                PollResult::Ok(PollerState::Idle)
            }
            Err(error) => {
                pool.error_span(|| tracing::error!(error=%error, "Failed to recreate the pool"));
                Err(error)
            }
        }
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}
