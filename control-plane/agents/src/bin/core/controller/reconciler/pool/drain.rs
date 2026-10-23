use crate::controller::{resources::OperationGuardArc, task_poller::PollContext};

use stor_port::types::v0::{
    store::{
        pool::{DrainConfig, DrainPhase, DrainProgressOp, PhaseReason, PoolSpec, UnwindSpare},
        replica::ReplicaSpec,
        volume::{OverReplicationRequester, VolumeSpec},
    },
    transport::{pool::PoolId, NexusStatus},
};
use tracing::{error, info, warn};

/// Promotes pools for draining if they are not already in Draining and the max inflight limit is not reached.
/// Filters offline pools from the enqueue list before marking state as Draining.
pub(crate) async fn drain_state_promoter(context: &PollContext) {
    let queued_online = queued_pools(context).await;
    let free_slots = free_slots(context);
    for pool in queued_online.iter().take(free_slots) {
        if let Ok(mut p) = context.specs().guarded_pool(pool).await {
            p.update_drain_phase(context.registry(), DrainPhase::Draining, None)
                .await
        }
    }
}

/// Restores the volume runtime replica move metadata from the persisted drain record of every
/// draining or cancelled pool.
///
/// For a cancelled pool, a move whose spare is placed is restored as `Cancelled` (`unwind_spare`
/// is runtime-only), so the hot-spare reconciler destroys the spare and ends the move. A move
/// without a placed spare has nothing to unwind and is dropped from the pool record instead.
pub(crate) async fn populate_missing_drain_config(context: &PollContext) {
    let pools = context
        .specs()
        .pools_rsc_draining()
        .into_iter()
        .chain(context.specs().pools_rsc_cancelled());
    for pool_id in pools {
        let Ok(mut pool_guard) = context.specs().guarded_pool(&pool_id).await else {
            continue;
        };
        let Ok(pool_spec) = context.specs().pool(&pool_id) else {
            continue;
        };
        let Some(moves) = pool_spec
            .drain_state()
            .map(|state| state.moving_replicas().clone())
        else {
            continue;
        };
        let is_cancelled = pool_spec.is_drain_cancelled();
        for mut drain_config in moves {
            let Ok(mut volume) = context.specs().volume(&drain_config.volume).await else {
                continue;
            };
            if volume.as_ref().has_over_replication() {
                continue;
            }
            if is_cancelled {
                if drain_config.spare_not_placed() {
                    info!(
                        pool.id = %pool_id,
                        volume.uuid = %drain_config.volume,
                        "Drain cancelled, spare not placed, dropping replica move"
                    );
                    let request = DrainProgressOp::RemoveReplicaMove(drain_config);
                    if let Err(error) = pool_guard
                        .update_drain_state(context.registry(), request)
                        .await
                    {
                        error!(
                            pool.id = %pool_id,
                            error = %error,
                            "Failed to drop replica move of cancelled drain"
                        );
                    }
                    continue;
                }
                drain_config.unwind_spare = Some(UnwindSpare::Cancelled);
            } else if let Some(replica) = drain_config.spare() {
                if let Ok(replica) = context.registry().replica(replica).await {
                    if context
                        .specs()
                        .pool(&replica.pool_id)
                        .is_ok_and(|p| p.drain_state().is_some())
                    {
                        drain_config.unwind_spare = Some(UnwindSpare::Respare);
                    }
                }
            }
            info!(
                pool.id = %pool_id,
                volume.uuid = %drain_config.volume,
                "Restoring over-replication config"
            );
            volume
                .update_over_replication(OverReplicationRequester::PoolDrain(drain_config))
                .await;
        }
    }
}

/// Advances the drain phase of every pool currently in `Draining`, based on what it still holds.
///
/// For each draining pool, the volume-owned replica count, total replica count and snapshot
/// count decide the next step:
/// - nothing left: `Drained`.
/// - only snapshots left: `PartiallyDrained` if the pool is unhealthy (e.g. offline) or its
///   drain policy ignores snapshots (`SnapshotsRetained`). Otherwise snapshot cleanup runs.
/// - only non-volume replicas left: `AwaitingCleanup`.
/// - volume-owned replicas left: `PartiallyDrained` if the pool is unhealthy. Otherwise
///   replica moves are started, within the concurrent move limit.
///
/// Pools whose usage or spec guard can't be obtained are skipped and retried on the next poll.
pub(crate) async fn advance_draining_pools(context: &PollContext) {
    for pool_id in context.specs().pools_rsc_draining() {
        let volume_owned_replicas = volume_owned_replica(context, &pool_id).await;
        let Ok(usage) = context.registry().pool_usage(&pool_id).await else {
            continue;
        };
        let Ok(mut pool_guard) = context.specs().guarded_pool(&pool_id).await else {
            continue;
        };
        match (
            volume_owned_replicas.len(),
            usage.repl_count,
            usage.snap_count,
        ) {
            (0, 0, 0) => {
                if let Some(state) = pool_guard.as_ref().drain_state() {
                    // This can happen if volumes were deleted by user while drain was in progress.
                    if !state.moving_replicas.is_empty() {
                        empty_move_config(context, &mut pool_guard).await;
                    }
                }
                pool_guard
                    .update_drain_phase(context.registry(), DrainPhase::Drained, None)
                    .await;
            }
            (0, 0, _) => {
                if let Some(reason) = is_offline_pool_reason(context, &pool_id).await {
                    pool_guard
                        .update_drain_phase(
                            context.registry(),
                            DrainPhase::PartiallyDrained,
                            Some(reason),
                        )
                        .await;
                } else {
                    if pool_guard.as_ref().ignore_snapshot_policy() {
                        pool_guard
                            .update_drain_phase(
                                context.registry(),
                                DrainPhase::PartiallyDrained,
                                Some(PhaseReason::SnapshotsRetained),
                            )
                            .await;
                    } else {
                        // TODO: implement snapshot cleanup. Just Log for now.
                        info!(pool.id = %pool_id, "cleanup all snapshots");
                    }
                }
            }
            (0, _, _) => {
                let reason = is_offline_pool_reason(context, &pool_id).await;
                pool_guard
                    .update_drain_phase(context.registry(), DrainPhase::AwaitingCleanup, reason)
                    .await;
            }
            (_, _, _) => {
                if let Some(reason) = is_offline_pool_reason(context, &pool_id).await {
                    pool_guard
                        .update_drain_phase(
                            context.registry(),
                            DrainPhase::PartiallyDrained,
                            Some(reason),
                        )
                        .await;
                } else if all_single_replica_volumes(context, &pool_id).await
                    && pool_guard.as_ref().is_drain_unsafe()
                {
                    pool_guard
                        .update_drain_phase(
                            context.registry(),
                            DrainPhase::PartiallyDrained,
                            Some(PhaseReason::SingleReplicaEviction),
                        )
                        .await;
                } else {
                    create_replica_move_config(context, &mut pool_guard).await
                }
            }
        }
    }
}

/// Finalises cancelled drains: once every replica move of a Cancelled pool is unwound, removes
/// the drain spec and state, reverting to the user's cordon, if any.
pub(crate) async fn handle_cancelled(context: &PollContext) {
    for pool in context.specs().pools_rsc_cancel_finished() {
        let Ok(mut pool_guard) = context.specs().guarded_pool(&pool).await else {
            continue;
        };
        match pool_guard
            .update_drain_state(context.registry(), DrainProgressOp::ClearDrain)
            .await
        {
            Ok(_) => info!(pool.id = %pool, "Cancelled drain finalised, drain state removed"),
            Err(error) => error!(
                pool.id = %pool,
                error = %error,
                "Failed to finalise cancelled drain"
            ),
        }
    }
}

/// Set drain phase to Drained if usage is 0 on AwaitingCleanup pool.
pub(crate) async fn handle_awaiting_cleanup(context: &PollContext) {
    for pool in context.specs().pools_rsc_awaiting_cleanup() {
        let Ok(mut pool_guard) = context.specs().guarded_pool(&pool).await else {
            continue;
        };
        let Ok(empty) = context.registry().is_pool_empty(&pool).await else {
            continue;
        };
        if empty {
            pool_guard
                .update_drain_phase(context.registry(), DrainPhase::Drained, None)
                .await;
        }
    }
}

/// Goes through all PartiallyDrained pools and checks if they can be moved back to Queued state.
pub(crate) async fn handle_partially_drained(context: &PollContext) {
    for pool in context.specs().pools_rsc_partially_drained() {
        let Ok(mut pool_guard) = context.specs().guarded_pool(&pool).await else {
            continue;
        };
        let Some(drain_state) = pool_guard.as_ref().drain_state() else {
            continue;
        };
        let Some(reason) = drain_state.reason.as_ref() else {
            continue;
        };
        match reason {
            PhaseReason::OfflinePool => {
                if context.registry().has_pool_state(&pool).await {
                    pool_guard
                        .update_drain_phase(
                            context.registry(),
                            DrainPhase::Queued,
                            Some(PhaseReason::WaitingForSlot),
                        )
                        .await
                }
                if context
                    .registry()
                    .is_node_online(&pool_guard.as_ref().node)
                    .await
                    && pool_guard
                        .as_ref()
                        .effective_cordon()
                        .is_some_and(|c| c.import)
                {
                    pool_guard
                        .update_drain_phase(
                            context.registry(),
                            DrainPhase::PartiallyDrained,
                            Some(PhaseReason::ImportCordoned),
                        )
                        .await
                }
            }
            PhaseReason::SingleReplicaEviction => {
                if !pool_guard.as_ref().is_drain_unsafe() {
                    pool_guard
                        .update_drain_phase(
                            context.registry(),
                            DrainPhase::Queued,
                            Some(PhaseReason::WaitingForSlot),
                        )
                        .await
                }
            }
            PhaseReason::SnapshotsRetained if pool_guard.as_ref().accept_snapshot_loss() => {
                pool_guard
                    .update_drain_phase(
                        context.registry(),
                        DrainPhase::Queued,
                        Some(PhaseReason::WaitingForSlot),
                    )
                    .await
            }
            _ => {}
        }
    }
}

/// Create move config for the replicas owned by volume.
/// Also, Ensure max concurrent replica move limit is checked.
async fn create_replica_move_config(context: &PollContext, pool: &mut OperationGuardArc<PoolSpec>) {
    let volume_owned_replicas = volume_owned_replica(context, pool.id()).await;
    for replica in volume_owned_replicas {
        let Some(volume_id) = replica.volume_owner() else {
            continue;
        };
        let Ok(mut volume_guard) = context.specs().volume(volume_id).await else {
            continue;
        };
        let num_replicas = volume_guard.as_ref().num_replicas;
        let has_over_replication = volume_guard.as_ref().has_over_replication();
        let Ok(pool_spec) = context.specs().pool(pool.id()) else {
            continue;
        };
        let Some(drain_spec) = pool_spec.drain_spec() else {
            continue;
        };
        if let Ok(volume) = context.registry().volume(volume_id).await {
            if volume.state().status != NexusStatus::Online {
                continue;
            }
        }
        let unsafe_move = pool_spec.is_drain_unsafe();
        // To keep handling simple, don't attempt to move replica at all belonging to
        // single replica volume and unsafe move is chosen by user. Do not proceed
        // if volume already has a replica move in progress.
        if !(unsafe_move && num_replicas == 1) && !has_over_replication {
            let Some(num_replica_moves) = pool_spec.metadata.persisted.num_replica_moves() else {
                continue;
            };
            let max_inflight = context.registry().pool_replica_move_limit() as usize;
            if num_replica_moves >= max_inflight {
                warn!(pool.id = %&pool.id(), "concurrent replica move limit reached for ongoing drain");
                break;
            }
            info!(
                pool.id = %&pool.id(),
                volume.uuid = %volume_id,
                replica.uuid = %replica.uuid,
                "Enqueuing replica for move"
            );
            let drain_config = DrainConfig::new(
                volume_id.clone(),
                pool.id().clone(),
                replica.uuid,
                !drain_spec.policy.unsafe_evict,
            );
            add_move_config(context, drain_config, pool, &mut volume_guard).await;
        }
    }
}

/// Update replica move config to pool drain record and the owner volume metadata.
async fn add_move_config(
    context: &PollContext,
    drain_config: DrainConfig,
    pool: &mut OperationGuardArc<PoolSpec>,
    volume: &mut OperationGuardArc<VolumeSpec>,
) {
    let request = DrainProgressOp::AddReplicaMove(drain_config.clone());
    if let Err(err) = pool.update_drain_state(context.registry(), request).await {
        error!(
            pool.id = %pool.id(),
            error = %err,
            "Failed to add drain config"
        );
    } else {
        info!(pool.id = %pool.id(), "Drain config updated successfully");
        let requester = OverReplicationRequester::PoolDrain(drain_config);
        volume.update_over_replication(requester).await;
    }
}

/// Clear all move replica configs from Pool metadata.
async fn empty_move_config(context: &PollContext, pool: &mut OperationGuardArc<PoolSpec>) {
    let request = DrainProgressOp::ClearReplicaMove();
    if let Err(err) = pool.update_drain_state(context.registry(), request).await {
        error!(
            pool.id = %pool.id(),
            error = %err,
            "Failed to clear drain config"
        );
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

/// Returns true if all replicas owned by volume for a given pool are single replica volumes.
async fn all_single_replica_volumes(context: &PollContext, pool: &PoolId) -> bool {
    for replica in context.registry().specs().pool_replicas(pool).to_vec() {
        let volume_id = replica.lock().volume_owner().cloned();
        if let Some(volume_id) = volume_id {
            if let Ok(v) = context.registry().specs().volume(&volume_id).await {
                if !v.lock().is_single_replica() {
                    return false;
                }
            }
        }
    }
    true
}

/// Returns `Some(PhaseReason::OfflinePool)` if the pool is offline, otherwise None.
async fn is_offline_pool_reason(context: &PollContext, pool: &PoolId) -> Option<PhaseReason> {
    if !context.registry().has_pool_state(pool).await {
        Some(PhaseReason::OfflinePool)
    } else {
        None
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
