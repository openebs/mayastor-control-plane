use crate::controller::{resources::OperationGuardArc, task_poller::PollContext};
use stor_port::types::v0::{
    store::{
        pool::{DrainConfig, DrainPhase, DrainProgressOp, PhaseReason, PoolSpec},
        replica::ReplicaSpec,
        volume::{ReplicaMoveRequester, VolumeSpec},
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
/// draining pool.
pub(crate) async fn populate_missing_replica_move(context: &PollContext) {
    for pool_id in context.specs().pools_rsc_draining() {
        let Ok(_pool_guard) = context.specs().guarded_pool(&pool_id).await else {
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
        for drain_config in moves {
            let Ok(mut volume) = context.specs().volume(&drain_config.volume).await else {
                continue;
            };
            if volume.as_ref().has_replica_move() {
                continue;
            }
            info!(
                pool.id = %pool_id,
                volume.uuid = %drain_config.volume,
                "Restoring replica move config"
            );
            volume
                .update_move_config(ReplicaMoveRequester::PoolDrain(drain_config))
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
        let has_replica_move = volume_guard.as_ref().has_replica_move();
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
        if !(unsafe_move && num_replicas == 1) && !has_replica_move {
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
            update_move_config(context, drain_config, pool, &mut volume_guard).await;
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
    if let Err(err) = pool.update_drain_state(context.registry(), request).await {
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
