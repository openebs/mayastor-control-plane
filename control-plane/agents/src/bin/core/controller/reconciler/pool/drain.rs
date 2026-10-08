use crate::controller::{resources::OperationGuardArc, task_poller::PollContext};
use stor_port::types::v0::{
    store::{
        pool::{DrainConfig, DrainPhase, DrainProgressOp, PhaseReason, PoolSpec},
        replica::ReplicaSpec,
        volume::{ReplicaMoveRequester, VolumeSpec},
    },
    transport::pool::PoolId,
};
use tracing::{error, info, warn};

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
/// Also, based on the resources present on the pool take appropriate step.
pub(crate) async fn inspect_draining_pools(context: &PollContext) {
    let draining_pool = context.specs().pools_rsc_draining();
    for pool in draining_pool {
        let volume_owned_replicas = volume_owned_replica(context, &pool.id).await;
        let Ok(usage) = context.registry().pool_usage(&pool.id).await else {
            continue;
        };
        let Ok(mut p) = context.specs().guarded_pool(&pool.id).await else {
            continue;
        };
        match (
            volume_owned_replicas.len(),
            usage.repl_count,
            usage.snap_count,
        ) {
            (0, 0, 0) => {
                set_phase(context, &mut p, DrainPhase::Drained, None).await;
            }
            (0, 0, _) => {
                if let Some(reason) = check_pool_state(context, pool.id()).await {
                    set_phase(context, &mut p, DrainPhase::PartiallyDrained, Some(reason)).await;
                } else {
                    if pool.ignore_snapshot_policy() {
                        set_phase(
                            context,
                            &mut p,
                            DrainPhase::PartiallyDrained,
                            Some(PhaseReason::SnapshotsRetained),
                        )
                        .await;
                    } else {
                        info!(pool.id = %pool.id, "cleanup all snapshots");
                    }
                }
            }
            (0, _, _) => {
                let reason = check_pool_state(context, pool.id()).await;
                set_phase(context, &mut p, DrainPhase::AwaitingCleanup, reason).await;
            }
            (_, _, _) => {
                if let Some(reason) = check_pool_state(context, pool.id()).await {
                    set_phase(context, &mut p, DrainPhase::PartiallyDrained, Some(reason)).await;
                } else {
                    handle_replicas_in_pool(context, &mut p).await
                }
            }
        }
    }
}

/// Create move config for the replicas owned by volume.
/// Ensure max concurrent replica move limit is checked.
async fn handle_replicas_in_pool(context: &PollContext, pool: &mut OperationGuardArc<PoolSpec>) {
    let volume_owned_replicas = volume_owned_replica(context, pool.id()).await;
    for replica in volume_owned_replicas {
        let Some(vol) = replica.volume_owner() else {
            continue;
        };
        let Ok(mut volume_spec) = context.specs().volume(vol).await else {
            continue;
        };
        let num_replicas = volume_spec.as_ref().num_replicas;
        let has_replica_move = volume_spec.as_ref().metadata.has_replica_move();
        let Ok(pool_spec) = context.specs().pool(pool.id()) else {
            continue;
        };
        let Some(drain_spec) = pool_spec.drain_spec() else {
            continue;
        };
        let unsafe_move = drain_spec.policy.unsafe_evict
            || drain_spec.policy.unsafe_rebuild_otherwise_evict.is_some();
        // To keep handling simple, don't attempt to move replica at all belonging to
        // single replica volume and unsafe move is chosen by user.
        if !(unsafe_move && num_replicas == 1) && !has_replica_move {
            let Some(num_replica_moves) = pool_spec.metadata.persisted.num_replica_moves() else {
                continue;
            };
            let max_inflight = context.registry().pool_replica_move_limit() as usize;
            if max_inflight == num_replica_moves {
                warn!(pool.id = %&pool.id(), "concurrent replica move limit reached for ongoing drain");
                break;
            }
            info!(
                pool.id = %&pool.id(),
                volume.uuid = %vol,
                replica.uuid = %replica.uuid,
                "Enqueuing replica for move"
            );
            let drain_config = DrainConfig::new(
                vol.clone(),
                pool.id().clone(),
                replica.uuid,
                !drain_spec.policy.unsafe_evict,
            );
            update_move_config(context, drain_config, pool, &mut volume_spec).await;
        }
    }
}

/// Update replica move config to pool drain record and the owner volume metadata.
async fn update_move_config(
    context: &PollContext,
    drain_config: DrainConfig,
    pool: &mut OperationGuardArc<PoolSpec>,
    volume: &mut OperationGuardArc<VolumeSpec>,
) {
    let request = DrainProgressOp::AddReplicaMove(drain_config.clone());
    if let Err(err) = pool.update_drain_record(context.registry(), request).await {
        error!(
        pool.id = %pool.id(),
        error = %err,
        "Failed to add drain config"
        );
    } else {
        info!(pool.id = %pool.id(), "Drain config updated successfully");
        let move_requester = ReplicaMoveRequester::PoolDrain(drain_config);
        volume.update_move_config(move_requester).await;
    }
}

/// Return all replicas owned by volume for a given pool.
async fn volume_owned_replica(context: &PollContext, pool: &PoolId) -> Vec<ReplicaSpec> {
    let mut volume_owned_replicas = Vec::new();
    for replica in context.registry().specs().pool_replicas(pool).to_vec() {
        let replica_lock = replica.lock();
        if replica_lock.owned_by_volume() {
            volume_owned_replicas.push(replica_lock.clone());
        }
    }
    volume_owned_replicas
}

async fn check_pool_state(context: &PollContext, pool: &PoolId) -> Option<PhaseReason> {
    if !context.registry().has_pool_state(pool).await {
        let Ok(pool_spec) = context.specs().pool(pool) else {
            return None;
        };
        match pool_spec.effective_cordon() {
            Some(ec) if ec.import => Some(PhaseReason::ImportCordoned),
            _ => Some(PhaseReason::OfflinePool),
        }
    } else {
        None
    }
}

/// Sets drain phase for a given pool.
/// Returns early if the phase and reason are already set for the pool.
async fn set_phase(
    context: &PollContext,
    pool: &mut OperationGuardArc<PoolSpec>,
    phase: DrainPhase,
    reason: Option<PhaseReason>,
) {
    if pool
        .lock()
        .drain_record()
        .map(|r| r.phase() == &phase && r.reason() == &reason)
        .unwrap_or(false)
    {
        return;
    }
    if let Err(err) = pool
        .set_phase_details(context.registry(), phase.clone(), reason)
        .await
    {
        error!(
            pool.id = %pool.id(),
            error = %err,
            "Failed to set drain phase as {phase}"
        );
    } else {
        info!(pool.id = %pool.id(), "Pool phase set to {phase}");
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
