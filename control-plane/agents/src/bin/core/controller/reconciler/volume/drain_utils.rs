use crate::controller::{
    resources::{OperationGuardArc, TraceSpan, TraceStrLog},
    task_poller::{PollContext, PollResult, PollerState},
};
use agents::errors::SvcError;
use stor_port::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            nexus::NexusSpec,
            nexus_persistence::NexusInfo,
            pool::{DrainConfig, DrainProgressOp},
            volume::{OverReplicationRequester, VolumeSpec},
        },
        transport::{ReplicaId, VolumeStatus},
    },
};
use tracing::{error, info};

/// If the drain config records a spare replica which is no longer a replica of this volume (its
/// child was removed and the replica destroyed), forget it so that a fresh
/// spare gets placed. If the drain was cancelled, the replica move is ended instead.
pub(crate) async fn forget_lost_spare(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
) {
    let Some(mut drain_cfg) = volume.as_ref().metadata.drain_config() else {
        return;
    };
    let Some(spare) = drain_cfg.spare() else {
        return;
    };
    let exists = context
        .specs()
        .volume_replicas(volume.uuid())
        .iter()
        .any(|replica| replica.lock().uuid == *spare);
    if !exists {
        if drain_cfg.drain_cancelled() {
            // Drain cancelled and its spare is gone: end the move instead of placing a new spare.
            volume.info_span(|| {
                tracing::info!(
                    replica.uuid = %spare,
                    pool.id = %drain_cfg.pool,
                    "Drain cancelled and spare replica no longer exists, ending the replica move"
                )
            });
            remove_drain_status(context, drain_cfg, volume).await;
            return;
        }
        volume.warn_span(|| {
            tracing::warn!(replica.uuid = %spare, "Spare replica no longer exists, removing it from the replica move")
        });
        drain_cfg.remove_spare();
        update_drain_status(context, drain_cfg, volume).await;
    }
}

/// Creates spare replica for the volume. If fails to do so, then check if it needs to be
/// evicted as per unsafe evict policy.
pub(crate) async fn create_spare_replica(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    mut drain_cfg: Option<DrainConfig>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
) -> PollResult {
    if let Some(drain_cfg) = drain_cfg.as_mut() {
        if drain_cfg.placement_started_at.is_none() {
            drain_cfg.update_placement_started_at();
            update_drain_status(context, drain_cfg.clone(), volume).await;
        }
    }
    match volume.create_volume_replicas(context.registry(), 1).await {
        Ok(result) => {
            if let Some(replica) = result.first() {
                volume
                    .info_span(|| tracing::info!(replica.uuid = %replica, "Spare replica created"));

                if let Some(drain_cfg) = drain_cfg.as_mut() {
                    drain_cfg.update_spare(Some(replica.clone()));
                    update_drain_status(context, drain_cfg.clone(), volume).await;
                }
                Ok(PollerState::Idle)
            } else {
                volume.debug("Spare replica not placed, none was created");
                if let Some(drain_cfg) = drain_cfg {
                    return evict_if_applicable(context, drain_cfg, nexus, volume).await;
                }
                Ok(PollerState::Idle)
            }
        }
        Err(error) => {
            let reason = error.full_string();
            volume.debug(&format!("Spare replica not placed: {reason}"));
            if let Some(drain_cfg) = drain_cfg {
                return evict_if_applicable(context, drain_cfg, nexus, volume).await;
            }
            Ok(PollerState::Idle)
        }
    }
}

/// Checks if the moving replica has to be evicted based on eviction policy.
async fn evict_if_applicable(
    context: &PollContext,
    drain_cfg: DrainConfig,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
) -> PollResult {
    let Ok(evict_now) = context
        .specs()
        .guarded_pool(&drain_cfg.pool)
        .await
        .map(|pool| pool.as_ref().evict_now(drain_cfg.placement_started_at))
    else {
        return Ok(PollerState::Busy);
    };
    if !evict_now {
        return Ok(PollerState::Busy);
    }
    let moving_replica = drain_cfg.moving_replica();
    match context.registry().volume(&volume.as_ref().uuid).await {
        Ok(vol) if vol.state().status == VolumeStatus::Online => {
            info!(
                volume.uuid = %volume.as_ref().uuid,
                replica.uuid = %moving_replica,
                pool.id = %drain_cfg.pool,
                "Destroying moving replica without a spare as duration to schedule spare has elapsed"
            );
            destroy_moving_replica(nexus, volume, context, moving_replica).await
        }
        _ => Ok(PollerState::Busy),
    }
}

/// Removes the replica from the volume. If the volume is published, the replica's child is first
/// removed from the nexus, otherwise the unused replica is simply removed from the volume.
async fn remove_replica(
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    replica: &ReplicaId,
) -> Result<(), SvcError> {
    match nexus {
        Some(nexus) => {
            let nexus_state = context.registry().nexus(nexus.uuid()).await?;
            volume
                .remove_volume_replica_in_use(context.registry(), nexus, &nexus_state, replica)
                .await
        }
        None => {
            volume
                .remove_unused_volume_replica(context.registry(), replica)
                .await
        }
    }
}

pub(crate) async fn destroy_moving_replica(
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    moving_replica: &ReplicaId,
) -> PollResult {
    let removed = remove_replica(nexus, volume, context, moving_replica).await;
    if let Err(error) = &removed {
        volume.warn_span(|| {
            tracing::warn!(
                "Failed to remove moving replica '{}': {}",
                moving_replica,
                error.full_string()
            )
        });
        return Ok(PollerState::Busy);
    } else {
        let drain_cfg = volume.as_ref().metadata.drain_config();
        if let Some(drain_cfg) = drain_cfg {
            remove_drain_status(context, drain_cfg.clone(), volume).await;
        }
    }
    Ok(PollerState::Idle)
}

async fn update_drain_status(
    context: &PollContext,
    drain_cfg: DrainConfig,
    volume: &mut OperationGuardArc<VolumeSpec>,
) {
    if let Ok(Some(mut pool)) = context.specs().pool_guard(&drain_cfg.pool).await {
        let request = DrainProgressOp::UpdateReplicaMove(drain_cfg.clone());
        if let Err(err) = pool.update_drain_state(context.registry(), request).await {
            error!(
                pool.id = %pool.id(),
                error = %err,
                "Failed to update drain config"
            );
        } else {
            info!(pool.id = %pool.id(), "Drain config updated successfully");
            let requester = OverReplicationRequester::PoolDrain(drain_cfg.clone());
            volume.update_over_replication(requester).await;
        }
    }
}

pub(crate) async fn remove_drain_status(
    context: &PollContext,
    drain_cfg: DrainConfig,
    volume: &mut OperationGuardArc<VolumeSpec>,
) {
    if let Ok(Some(mut pool)) = context.specs().pool_guard(&drain_cfg.pool).await {
        let request = DrainProgressOp::RemoveReplicaMove(drain_cfg.clone());
        if let Err(err) = pool.update_drain_state(context.registry(), request).await {
            error!(
                pool.id = %pool.id(),
                error = %err,
                "Failed to removed drain config"
            );
        } else {
            info!(pool.id = %pool.id(), "Drain config removed successfully");
            volume.remove_over_replication().await;
        }
    }
}

/// Handles the drain related operations for a volume which already has its effective number of
/// replicas: evicting the moving replica or destroying it once the spare is online.
pub(crate) async fn handle_drain_replica_move(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
    config: DrainConfig,
) -> PollResult {
    let vol = context.registry().volume(&volume.as_ref().uuid).await?;
    let moving_replica = config.moving_replica();
    let spare = config.spare();
    let state = vol.state();
    let nexus_id = state.target.as_ref().map(|n| &n.uuid);

    let nexus_info = context
        .registry()
        .nexus_info(Some(&volume.as_ref().uuid), nexus_id, true, None)
        .await?;
    let unrelated_unhealthy = unrelated_unhealthy_children(nexus_info, spare, moving_replica);
    if unrelated_unhealthy.is_empty() {
        if config.unsafe_evict() {
            info!(
                volume.uuid = %volume.as_ref().uuid,
                replica.uuid = %moving_replica,
                pool.id = %config.pool,
                "Unsafe evict drain, destroying moving replica without a spare"
            );
            return destroy_moving_replica(nexus, volume, context, moving_replica).await;
        }

        if vol.spare_online() {
            info!(
                volume.uuid = %volume.as_ref().uuid,
                replica.uuid = %moving_replica,
                pool.id = %config.pool,
                "Spare is online, destroying moving replica"
            );
            return destroy_moving_replica(nexus, volume, context, moving_replica).await;
        }
    }
    Ok(PollerState::Idle)
}

/// This returns list of unhealthy replicas which are not related of drain config. i.e. neither
/// moving nor spare replica.
fn unrelated_unhealthy_children(
    nexus_info: Option<NexusInfo>,
    spare: Option<&ReplicaId>,
    moving_replica: &ReplicaId,
) -> Vec<ReplicaId> {
    let mut unhealthy = vec![];
    if let Some(nexus) = nexus_info {
        for child in nexus.children {
            if child.uuid != *moving_replica && Some(&child.uuid) != spare && !child.healthy {
                unhealthy.push(child.uuid);
            }
        }
    }
    unhealthy
}
