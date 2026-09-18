use crate::controller::task_poller::PollContext;
use stor_port::types::v0::store::pool::{DrainPhase, DrainPhaseOp, DrainProgressOp, PoolSpec};

/// Enqueue pools for draining if they are not already in Draining and the max inflight limit is not reached.
/// Filters offline pools from the enqueue list before marking state as Draining.
pub(crate) async fn pool_drain_enqueuer(context: &PollContext) {
    let queued_online = list_queued_pools(context).await;
    let free_slots = get_free_slots(context);
    for pool in queued_online.iter().take(free_slots) {
        if let Ok(mut p) = context.specs().guarded_pool(&pool.id).await {
            if let Ok(usage) = context.registry().pool_usage(&pool.id).await {
                let phase_op = DrainPhaseOp::new(DrainPhase::Draining, None, Some(usage));
                let request = DrainProgressOp::PhaseUpdate(phase_op);
                let _ = p.update_drain_record(context.registry(), request).await;
            }
        }
    }
}

/// Returns the number of pools that can be promoted to Draining state.
fn get_free_slots(context: &PollContext) -> usize {
    let draining_pool = context.specs().pools_rsc_draining();
    let max_inflight = context.registry().max_concurrent_pool_drain() as usize;
    max_inflight.saturating_sub(draining_pool.len())
}

/// Lists pools which are online and queued for drain. ordered by the drain request timestamp.
async fn list_queued_pools(context: &PollContext) -> Vec<PoolSpec> {
    let drain_queued = context.specs().pools_rsc_drain_queued();
    let mut queued_online = Vec::new();
    for pool in drain_queued {
        if context.registry().has_pool_state(&pool.id).await {
            queued_online.push(pool);
        }
    }
    queued_online
}
