use crate::controller::task_poller::PollContext;
use stor_port::types::v0::transport::pool::PoolId;
use tracing::{error, info};

/// Promotes pools for draining if they are not already in Draining and the max inflight limit is not reached.
/// Filters offline pools from the enqueue list before marking state as Draining.
pub(crate) async fn drain_state_promoter(context: &PollContext) {
    let queued_online = queued_pools(context).await;
    let free_slots = free_slots(context);
    for pool in queued_online.iter().take(free_slots) {
        if let Ok(mut p) = context.specs().guarded_pool(pool).await {
            if let Err(e) = p.set_draining(context.registry()).await {
                error!(
                    pool.id = %pool,
                    error = %e,
                    "Failed to promote pool to Draining phase"
                );
            } else {
                info!(pool.id = %pool, "Pool promoted to Draining phase successfully");
            }
        }
    }
}

/// Returns the number of pools that can be promoted to Draining state.
fn free_slots(context: &PollContext) -> usize {
    let num_draining_pool = context.specs().pools_rsc_draining_len();
    let max_inflight = context.registry().max_concurrent_pool_drain() as usize;
    max_inflight.saturating_sub(num_draining_pool)
}

/// Lists pools which are online and queued for drain. ordered by the drain request timestamp.
async fn queued_pools(context: &PollContext) -> Vec<PoolId> {
    let drain_queued = context.specs().pools_rsc_drain_queued();
    let mut queued_online = Vec::new();
    for pool in drain_queued {
        if context.registry().has_pool_state(&pool).await {
            queued_online.push(pool);
        }
    }
    queued_online
}
