use crate::controller::task_poller::PollContext;
use stor_port::types::v0::{
    store::pool::{DrainPhase, PhaseReason},
    transport::pool::PoolId,
};
use tracing::{error, info};

/// Promotes pools for draining if they are not already in Draining and the max inflight limit is not reached.
/// Filters offline pools from the enqueue list before marking state as Draining.
pub(crate) async fn drain_state_promoter(context: &PollContext) {
    let queued_online = queued_pools(context).await;
    let free_slots = free_slots(context);
    for pool in queued_online.iter().take(free_slots) {
        if let Ok(mut p) = context.specs().guarded_pool(pool).await {
            if let Err(e) = p
                .set_phase_details(context.registry(), DrainPhase::Draining, None)
                .await
            {
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

/// Checks if there are any draining pool which is Offline.
/// Also, based on the resources present on the pool take appropriate
/// step.
pub(crate) async fn inspect_draining_pools(context: &PollContext) {
    let draining_pool = context.specs().pools_rsc_draining();
    for pool in draining_pool {
        if !context.registry().has_pool_state(&pool.id).await {
            let reason = match pool.effective_cordon() {
                Some(ec) if ec.import => PhaseReason::ImportCordoned,
                _ => PhaseReason::OfflinePool,
            };
            set_phase(
                context,
                &pool.id,
                DrainPhase::PartiallyDrained,
                Some(reason),
            )
            .await;
        } else {
            let mut volume_owned_replicas = Vec::new();
            for replica in context.registry().specs().pool_replicas(&pool.id).to_vec() {
                let replica_lock = replica.lock();
                if replica_lock.owned_by_volume() {
                    volume_owned_replicas.push(replica_lock.clone());
                }
            }
            if let Ok(usage) = context.registry().pool_usage(&pool.id).await {
                match (
                    volume_owned_replicas.iter().len(),
                    usage.repl_count,
                    usage.snap_count,
                ) {
                    (0, 0, 0) => {
                        set_phase(context, &pool.id, DrainPhase::Drained, None).await;
                    }
                    (0, 0, _) => {
                        if pool.ignore_snapshot_policy() {
                            set_phase(
                                context,
                                &pool.id,
                                DrainPhase::PartiallyDrained,
                                Some(PhaseReason::SnapshotsRetained),
                            )
                            .await;
                        } else {
                            info!(pool.id = %pool.id, "cleanup all snapshots");
                        }
                    }
                    (0, _, _) => {
                        set_phase(context, &pool.id, DrainPhase::AwaitingCleanup, None).await;
                    }
                    (_, _, _) => {
                        info!("wait or create replica moves");
                    }
                }
            }
        }
    }
}

async fn set_phase(
    context: &PollContext,
    id: &PoolId,
    phase: DrainPhase,
    reason: Option<PhaseReason>,
) {
    if let Ok(mut p) = context.specs().guarded_pool(id).await {
        if p.lock().drain_record().map(|r| r.phase()) == Some(&phase) {
            return;
        }
        if let Err(e) = p
            .set_phase_details(context.registry(), phase.clone(), reason)
            .await
        {
            error!(
                pool.id = %id,
                error = %e,
                "Failed to set drain phase as {phase}"
            );
        } else {
            info!(pool.id = %id, "Pool phase set to {phase}");
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
