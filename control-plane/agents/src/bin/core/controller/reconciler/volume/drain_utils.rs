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
            pool::{DrainConfig, DrainProgressOp, UnwindSpare},
            volume::{OverReplicationRequester, VolumeSpec},
        },
        transport::{ReplicaId, VolumeStatus},
    },
};
use tracing::{error, info};

/// If the drain config records a spare replica which is no longer a replica of this volume (its
/// child was removed and the replica destroyed), forget it so that a fresh
/// spare gets placed.
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
                volume.info_span(|| {
                    tracing::info!(
                        replicas = %replica,
                        "Successfully created '{}' new replica(s)",
                        result.len()
                    )
                });

                if let Some(drain_cfg) = drain_cfg.as_mut() {
                    drain_cfg.update_spare(Some(replica.clone()));
                    update_drain_status(context, drain_cfg.clone(), volume).await;
                }
                Ok(PollerState::Idle)
            } else {
                error!(volume.uuid = %volume.uuid(), "No spare replica was created for volume");
                if let Some(drain_cfg) = drain_cfg {
                    return evict_if_applicable(context, drain_cfg, nexus, volume).await;
                }
                Ok(PollerState::Busy)
            }
        }
        Err(error) => {
            let reason = error.full_string();
            error!(volume.uuid = %volume.uuid(), "No spare replica was created for volume: {reason}");
            if let Some(drain_cfg) = drain_cfg {
                return evict_if_applicable(context, drain_cfg, nexus, volume).await;
            }
            Ok(PollerState::Busy)
        }
    }
}

/// Create replicas for the volume to match user requested replica count.
pub(crate) async fn create_missing_replica(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    count: usize,
) -> usize {
    match volume
        .create_volume_replicas(context.registry(), count)
        .await
    {
        Ok(result) => {
            if !result.is_empty() {
                let replicas = result.iter().fold(String::new(), |acc, replica| {
                    if acc.is_empty() {
                        format!("{replica}")
                    } else {
                        format!("{acc},{replica}")
                    }
                });

                volume.info_span(|| {
                    tracing::info!(
                    replicas = %replicas,
                    "Successfully created '{}' new replica(s)",
                    result.len()
                    )
                });
                return result.len();
            } else {
                volume.error(&format!(
                    "Failed to create replicas for volume '{}': no replicas were created",
                    volume.uuid()
                ));
            }
        }
        Err(error) => {
            let reason = error.full_string();
            volume.error(&format!("Failed to create replicas: {reason}"));
        }
    }
    0
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
    let moving_replica = drain_cfg.moving_replica;
    match context.registry().volume(&volume.as_ref().uuid).await {
        Ok(vol) if vol.state().status == VolumeStatus::Online => {
            info!(
                volume.uuid = %volume.as_ref().uuid,
                replica.uuid = %moving_replica,
                pool.id = %drain_cfg.pool,
                "Destroying moving replica without a spare"
            );
            destroy_moving_replica(nexus, volume, context, &moving_replica).await
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

pub(crate) async fn destroy_spare_replica(
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    spare_replica: &ReplicaId,
) -> PollResult {
    let removed = remove_replica(nexus, volume, context, spare_replica).await;
    if let Err(error) = &removed {
        volume.warn_span(|| {
            tracing::warn!(
                "Failed to remove spare replica '{}': {}",
                spare_replica,
                error.full_string()
            )
        });
        return Ok(PollerState::Busy);
    } else {
        let drain_cfg = volume.as_ref().metadata.drain_config();
        if let Some(mut drain_cfg) = drain_cfg {
            if drain_cfg.unwind_spare == Some(UnwindSpare::Cancelled) {
                // Drain cancelled: end the move in the same step, so the effective replica count
                // drops with the spare and no new spare is created.
                remove_drain_status(context, drain_cfg, volume).await;
            } else {
                drain_cfg.remove_spare();
                update_drain_status(context, drain_cfg.clone(), volume).await;
            }
        }
    }
    Ok(PollerState::Idle)
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
/// replicas: evicting the moving replica, unwinding the spare or destroying the moving replica
/// once the spare is online.
pub(crate) async fn handle_drain_replica_move(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
    config: DrainConfig,
) -> PollResult {
    let vol = context.registry().volume(&volume.as_ref().uuid).await?;
    let moving_replica = &config.moving_replica;
    if config.unsafe_evict() && vol.state().status == VolumeStatus::Online {
        info!(
            "remove replica {:?} from volume {:?}",
            config.moving_replica,
            volume.as_ref().uuid
        );
        return destroy_moving_replica(nexus, volume, context, moving_replica).await;
    }
    if let Some(unwind) = &config.unwind_spare {
        match unwind {
            UnwindSpare::Respare => {
                if let Some(spare) = config.spare() {
                    return destroy_spare_replica(nexus, volume, context, spare).await;
                }
            }
            UnwindSpare::Cancelled => {
                // Drain cancelled: destroy the spare and end the move,
                // keeping the moving replica.
                return match config.spare() {
                    Some(spare) => destroy_spare_replica(nexus, volume, context, spare).await,
                    None => {
                        remove_drain_status(context, config.clone(), volume).await;
                        Ok(PollerState::Idle)
                    }
                };
            }
        }
    }
    if vol.spare_online() {
        return destroy_moving_replica(nexus, volume, context, moving_replica).await;
    }
    Ok(PollerState::Idle)
}
