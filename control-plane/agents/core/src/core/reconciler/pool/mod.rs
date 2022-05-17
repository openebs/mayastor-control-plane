use crate::core::{
    specs::{OperationSequenceGuard, SpecOperations},
    task_poller::{PollContext, PollPeriods, PollResult, PollTimer, PollerState, TaskPoller},
    wrapper::ClientOps,
};
use common_lib::types::v0::{
    message_bus::{CreatePool, DestroyPool, NodeStatus},
    store::{pool::PoolSpec, OperationMode, TraceSpan},
};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::Instrument;

/// Pool Reconciler loop which:
/// 1. recreates pools which are not present following an io-engine restart
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
        let pools = context.specs().get_locked_pools();
        let mut results = Vec::with_capacity(pools.len() * 2);
        for pool in pools {
            let _guard = match pool.operation_guard(OperationMode::ReconcileStart) {
                Ok(guard) => guard,
                Err(_) => continue,
            };

            results.push(missing_pool_state_reconciler(&pool, context).await);
            results.push(deleting_pool_spec_reconciler(&pool, context).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}

/// If a pool has a spec but not state, it means that the io-engine instance where the pool should
/// exist does not have the pool open.
/// This can happen if the pool is destroyed under the control plane, or if the io-engine
/// crashed/restarted.
/// In such a case, we issue a new create pool request against the io-engine instance where the pool
/// should exist.
#[tracing::instrument(skip(pool_spec, context), level = "trace", fields(pool.uuid = %pool_spec.lock().id, request.reconcile = true))]
async fn missing_pool_state_reconciler(
    pool_spec: &Arc<Mutex<PoolSpec>>,
    context: &PollContext,
) -> PollResult {
    if !pool_spec.lock().status().created() {
        // nothing to do here
        return PollResult::Ok(PollerState::Idle);
    }
    let pool_id = pool_spec.lock().id.clone();

    if context.registry().get_pool_state(&pool_id).await.is_err() {
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
                let node_status = node.read().await.status();
                warn_missing(pool_spec, node_status);
                return PollResult::Ok(PollerState::Idle);
            }
            Err(_) => {
                warn_missing(pool_spec, NodeStatus::Unknown);
                return PollResult::Ok(PollerState::Idle);
            }
            Ok(node) => node,
        };

        async {
            pool.warn_span(|| tracing::warn!("Attempting to recreate missing pool"));

            let request = CreatePool::new(&pool.node, &pool.id, &pool.disks, &pool.labels);
            match node.create_pool(&request).await {
                Ok(_) => {
                    pool.info_span(|| tracing::info!("Pool successfully recreated"));
                    PollResult::Ok(PollerState::Idle)
                }
                Err(error) => {
                    pool.error_span(
                        || tracing::error!(error=%error, "Failed to recreate the pool"),
                    );
                    Err(error)
                }
            }
        }
        .instrument(tracing::info_span!("missing_pool_state_reconciler", pool.uuid = %pool.id, request.reconcile = true))
        .await
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

/// If a pool is tried to be deleted after its corresponding io-engine node is down,
/// the pool deletion gets struck in Deleting state, this creates a problem as when
/// the node comes up we cannot create a pool with same specs, the deleting_pool_spec_reconciler
/// cleans up any such pool when node comes up.
#[tracing::instrument(skip(pool_spec, context), level = "trace", fields(pool.uuid = %pool_spec.lock().id, request.reconcile = true))]
async fn deleting_pool_spec_reconciler(
    pool_spec: &Arc<Mutex<PoolSpec>>,
    context: &PollContext,
) -> PollResult {
    if !pool_spec.lock().status().deleting() {
        // nothing to do here
        return PollResult::Ok(PollerState::Idle);
    }

    let pool = pool_spec.lock().clone();
    match context
        .registry()
        .get_node_wrapper(&pool.node.clone())
        .await
    {
        Ok(node) => {
            if !node.read().await.is_online() {
                return PollResult::Ok(PollerState::Idle);
            }
        }
        Err(_) => return PollResult::Ok(PollerState::Idle),
    };

    async {
        let request = DestroyPool {
            node: pool.node.clone(),
            id: pool.id.clone(),
        };
        match context
            .specs()
            .destroy_pool(context.registry(), &request, OperationMode::ReconcileStep)
            .await
        {
            Ok(_) => {
                pool.info_span(|| tracing::info!("Pool deleted successfully"));
                PollResult::Ok(PollerState::Idle)
            }
            Err(error) => {
                pool.error_span(|| tracing::error!(error=%error, "Failed to delete the pool"));
                Err(error)
            }
        }
    }
    .instrument(tracing::info_span!("deleting_pool_spec_reconciler", pool.uuid = %pool.id, request.reconcile = true))
    .await
}
