use crate::controller::{
    reconciler::{nexus, PollContext, TaskPoller},
    resources::{
        operations_helper::OperationSequenceGuard, OperationGuardArc, ResourceMutex, TraceSpan,
        TraceStrLog,
    },
    task_poller::{squash_results, PollResult, PollerState},
};

use stor_port::{
    transport_api::ErrorChain,
    types::v0::{
        store::{nexus::NexusSpec, volume::VolumeSpec},
        transport::{Nexus, VolumeState, VolumeStatus},
    },
};

use std::cmp::Ordering;
use stor_port::types::v0::{
    store::{
        pool::{DrainConfig, DrainProgressOp},
        volume::ReplicaMoveRequester,
    },
    transport::ReplicaId,
};
use tracing::{error, info};

/// Volume HotSpare reconciler
#[derive(Debug)]
pub(super) struct HotSpareReconciler {}
impl HotSpareReconciler {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for HotSpareReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        let volumes = context.specs().volumes_rsc();
        for mut volume in volumes {
            results.push(hot_spare_reconcile(&mut volume, context).await);
        }
        Self::squash_results(results)
    }
}

#[tracing::instrument(level = "debug", skip(context, volume_spec), fields(volume.uuid = %volume_spec.uuid(), request.reconcile = true))]
async fn hot_spare_reconcile(
    volume_spec: &mut ResourceMutex<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let mut volume = match volume_spec.operation_guard() {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    let volume_state = context.registry().volume_state(volume.uuid()).await?;

    if !volume.as_ref().policy.self_heal {
        return PollResult::Ok(PollerState::Idle);
    }
    if !volume.as_ref().status.created() {
        return PollResult::Ok(PollerState::Idle);
    }

    match volume_state.status {
        VolumeStatus::Online if volume.as_ref().metadata.drain_config().is_none() => {
            volume_replica_count_reconciler(&mut volume, None, context).await
        }
        // An Online volume with a pool drain in progress also needs its nexus children
        // reconciled, as the spare replica has to be added to the nexus.
        VolumeStatus::Online | VolumeStatus::Unknown | VolumeStatus::Degraded => {
            hot_spare_nexus_reconcile(&mut volume, &volume_state, context).await
        }
        VolumeStatus::Faulted => PollResult::Ok(PollerState::Idle),
        VolumeStatus::ShuttingDown | VolumeStatus::Shutdown => PollResult::Ok(PollerState::Idle),
    }
}

async fn hot_spare_nexus_reconcile(
    volume: &mut OperationGuardArc<VolumeSpec>,
    volume_state: &VolumeState,
    context: &PollContext,
) -> PollResult {
    let mut results = vec![];

    if let Some(nexus) = &volume_state.target {
        let mut nexus = context.specs().nexus(&nexus.uuid).await?;

        // Since we don't want the reconcilers to work on shutdown state.
        if !nexus.as_ref().spec_status.created() || nexus.as_ref().is_shutdown() {
            return PollResult::Ok(PollerState::Idle);
        }

        // generic nexus reconciliation (does not matter that it belongs to a volume)
        results.push(generic_nexus_reconciler(volume, &mut nexus, context).await);

        // fixup the volume replica count: creates new replicas when we're behind
        // removes extra replicas but only if they're UNUSED (by a nexus)
        results.push(volume_replica_count_reconciler(volume, Some(&mut nexus), context).await);
        // fixup the nexus replica count to match the volume's replica count
        results.push(nexus_replica_count_reconciler(volume, &mut nexus, context).await);
    } else {
        results.push(volume_replica_count_reconciler(volume, None, context).await);
    }

    squash_results(results)
}

#[tracing::instrument(skip(context, volume, nexus), fields(volume = ?volume.as_ref(), nexus.uuid = %nexus.uuid(), nexus.node = %nexus.as_ref().node, volume.uuid = tracing::field::Empty, request.reconcile = true))]
async fn generic_nexus_reconciler(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    if let Some(ref volume_uuid) = nexus.as_ref().owner {
        tracing::Span::current().record("volume.uuid", volume_uuid.as_str());
    }
    let mut results = vec![];
    results.push(handle_faulted_children(nexus, &mut Some(volume), context).await);
    results.push(unknown_children_remover(nexus, context).await);
    results.push(missing_children_remover(nexus, &mut Some(volume), context).await);
    squash_results(results)
}

/// Checks if nexus is Degraded and any child is Faulted. If yes, Depending on rebuild policy for
/// child it performs rebuild operation. We exclude NoSpace Degrade.
async fn handle_faulted_children(
    nexus: &mut OperationGuardArc<NexusSpec>,
    volume: &mut Option<&mut OperationGuardArc<VolumeSpec>>,
    context: &PollContext,
) -> PollResult {
    nexus::handle_faulted_children(nexus, volume, context).await
}

/// Given a degraded volume
/// When a nexus state has children that are not present in the spec
/// Then the children should eventually be removed from the state
/// And the uri's should not be destroyed
async fn unknown_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    nexus::unknown_children_remover(nexus, context).await
}

/// Given a degraded volume
/// When a nexus spec has children that are not present in the state
/// Then the children should eventually be removed from the spec
/// And the replicas should eventually be destroyed
async fn missing_children_remover(
    nexus: &mut OperationGuardArc<NexusSpec>,
    volume: &mut Option<&mut OperationGuardArc<VolumeSpec>>,
    context: &PollContext,
) -> PollResult {
    nexus::missing_children_remover(nexus, volume, context).await
}

/// Given a degraded volume
/// When the nexus spec has a different number of children to the number of volume replicas
/// Then the nexus spec should eventually have as many children as the number of volume replicas
async fn nexus_replica_count_reconciler(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().nexus(nexus_uuid).await?;

    let nexus_spec_clone = nexus.as_ref();
    let nexus_replica_children =
        nexus_spec_clone
            .children
            .iter()
            .fold(0usize, |mut counter, child| {
                // only account for children which are lvol replicas
                if let Some(replica) = child.as_replica() {
                    if context.specs().replica_rsc(replica.uuid()).is_some() {
                        counter += 1;
                    }
                }
                counter
            });
    return nexus_replica_count_reconciler_traced(
        volume,
        nexus,
        nexus_state,
        nexus_replica_children,
        context,
    )
    .await;
}
#[tracing::instrument(skip(context, volume, nexus, nexus_state), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
async fn nexus_replica_count_reconciler_traced(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: &mut OperationGuardArc<NexusSpec>,
    nexus_state: Nexus,
    nexus_replica_children: usize,
    context: &PollContext,
) -> PollResult {
    let vol_spec_clone = volume.as_ref();
    let volume_replicas = vol_spec_clone.num_replicas as usize;
    let need_spare = volume.as_ref().metadata.need_spare();

    let effective_replica_count = volume_replicas + need_spare as usize;

    match nexus_replica_children.cmp(&effective_replica_count) {
        Ordering::Less => {
            nexus.warn_span(|| {
                tracing::warn!(
                    "The nexus only has '{}' replica(s) but the volume requires '{}' replica(s)",
                    nexus_replica_children,
                    effective_replica_count
                )
            });
            volume
                .attach_replicas_to_nexus(context.registry(), nexus)
                .await?;
        }
        Ordering::Greater => {
            nexus.warn_span(|| {
                tracing::warn!(
                    "The nexus has more replicas(s) ('{}') than the required replica count ('{}')",
                    nexus_replica_children,
                    effective_replica_count
                )
            });
            volume
                .remove_excess_replicas_from_nexus(context.registry(), nexus, &nexus_state)
                .await?;
        }
        Ordering::Equal => {}
    }

    PollResult::Ok(
        if nexus.as_ref().children.len() == effective_replica_count {
            PollerState::Idle
        } else {
            PollerState::Busy
        },
    )
}

/// When the number of created volume replicas is different to the required number of replicas
/// Then the number of created volume replicas should eventually match the required number of
/// replicas.
/// Also, handles the replica move configs. Currently, replica move is requested by Pool drain.
/// Creates a spare replica if requested, monitors the spare and destroys moving replica when
/// spare rebuild completes.
async fn volume_replica_count_reconciler(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
) -> PollResult {
    volume_replica_count_reconciler_traced(volume, nexus, context).await
}

#[tracing::instrument(skip(context, volume, nexus), fields(volume.uuid = %volume.uuid(), request.reconcile = true))]
async fn volume_replica_count_reconciler_traced(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
) -> PollResult {
    let required_replica_count = volume.as_ref().num_replicas as usize;

    let need_spare = volume.as_ref().metadata.need_spare();

    let effective_replica_count = required_replica_count + need_spare as usize;

    let current_replicas = context.specs().volume_replicas(volume.uuid());
    let mut current_replica_count = current_replicas.len();

    match current_replica_count.cmp(&effective_replica_count) {
        Ordering::Less => {
            tracing::info!("entered less arm");
            // Guard: do not materialise a fresh, empty replica when the volume has
            // already been published but currently has zero spec'd replicas.
            //
            // A volume that has never been published has no `health_info_id` and
            // no data to lose, so a fresh replica there is correct.
            if current_replica_count == 0 && volume.as_ref().health_info_id().is_some() {
                // The reconciler cannot recover this volume on its own.
                // Emit the warning exactly once per agent lifetime per volume to avoid flooding
                // logs at the reconcile cadence.
                if !volume.as_ref().data_loss_warned() {
                    // Mark first, then log. The lock is dropped before the
                    // tracing macro fires so the lock isn't held across logging.
                    volume.lock().set_data_loss_warned();
                    volume.warn_span(|| {
                        tracing::warn!(
                            "Volume has no remaining replicas; hot-spare will not create \
                            additional replicas since no rebuild is possible"
                        )
                    });
                }
                return PollResult::Ok(PollerState::Idle);
            }

            volume.warn_span(|| {
                tracing::warn!(
                    "The volume has '{}' replica(s) but it should have '{}'",
                    current_replica_count,
                    required_replica_count
                )
            });

            let base_diff = required_replica_count.saturating_sub(current_replica_count);
            let mut drain_cfg = volume.as_ref().metadata.drain_config();
            let creating_spare =
                base_diff == 0 && drain_cfg.as_ref().is_some_and(|cfg| cfg.spare_not_placed());
            // As volume replica count cannot be modified while replica move is applied on volume and
            // replica move cannot be applied on a non-online volume. We can get non-zero base_diff
            // while replica move is also there only if user tries to force trigger rebuild and replica
            // is removed. In that case doing a Full rebuild takes precedence.
            let diff = if creating_spare { 1 } else { base_diff };

            match volume
                .create_volume_replicas(context.registry(), diff)
                .await?
            {
                result if !result.is_empty() => {
                    current_replica_count += result.len();
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
                    if creating_spare {
                        if let Some(drain_cfg) = drain_cfg.as_mut() {
                            if let Some(spare) = result.first() {
                                drain_cfg.update_placement_started_at();
                                drain_cfg.update_spare(Some(spare.clone()));
                                update_drain_status(context, drain_cfg.clone(), volume).await;
                            }
                        }
                    }
                }
                _ => {
                    if creating_spare {
                        if let Some(drain_cfg) = drain_cfg {
                            if let Ok(pool) = context.specs().guarded_pool(&drain_cfg.pool).await {
                                if pool.as_ref().evict_now(drain_cfg.placement_started_at) {
                                    if let Some(moving_replica) = drain_cfg.moving_replica {
                                        tracing::info!(
                                            "destroying replica {} without spare",
                                            moving_replica
                                        );
                                        destroy_moving_replica(
                                            nexus,
                                            volume,
                                            context,
                                            &moving_replica,
                                        )
                                        .await
                                    }
                                }
                            }
                        }
                    }
                    volume.error("Failed to create replicas");
                }
            }
        }
        Ordering::Greater => {
            volume.warn_span(|| {
                tracing::warn!(
                    "The volume has '{}' replica(s) but it should only have '{}'. Removing...",
                    current_replica_count,
                    required_replica_count
                )
            });

            let diff = current_replica_count - required_replica_count;
            match volume
                .remove_unused_volume_replicas(context.registry(), diff)
                .await
            {
                Ok(_) => {
                    volume.info("Successfully removed unused replicas");
                }
                Err(error) => {
                    volume.warn_span(|| {
                        tracing::warn!(
                            "Failed to remove unused replicas from volume, error: '{}'",
                            error.full_string()
                        )
                    });
                }
            }
        }
        Ordering::Equal => {
            if let Some(cfg) = volume.as_ref().metadata.drain_config() {
                if let Ok(vol) = context.registry().volume(&volume.as_ref().uuid).await {
                    if let Some(moving_replica) = cfg.moving_replica.as_ref() {
                        if cfg.unsafe_evict() && vol.state().status == VolumeStatus::Online {
                            tracing::info!(
                                "remove replica {:?} from volume {:?}",
                                cfg.moving_replica,
                                volume.as_ref().uuid
                            );
                            destroy_moving_replica(nexus, volume, context, moving_replica).await
                        } else if vol.spare_online() {
                            destroy_moving_replica(nexus, volume, context, moving_replica).await
                        }
                    }
                }
            }
        }
    }

    PollResult::Ok(if current_replica_count == required_replica_count {
        PollerState::Idle
    } else {
        PollerState::Busy
    })
}

async fn destroy_moving_replica(
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    moving_replica: &ReplicaId,
) {
    let removed = match nexus {
        Some(nexus) => match context.registry().nexus(nexus.uuid()).await {
            Ok(nexus_state) => {
                volume
                    .remove_volume_replica_in_use(
                        context.registry(),
                        nexus,
                        &nexus_state,
                        moving_replica,
                    )
                    .await
            }
            Err(error) => Err(error),
        },
        None => {
            volume
                .remove_unused_volume_replica(context.registry(), moving_replica)
                .await
        }
    };
    if let Err(error) = &removed {
        volume.warn_span(|| {
            tracing::warn!(
                "Failed to remove moving replica '{}': {}",
                moving_replica,
                error.full_string()
            )
        });
    } else {
        tracing::info!("moving replica destroyed");
        let drain_cfg = volume.as_ref().metadata.drain_config();
        if let Some(drain_cfg) = drain_cfg {
            remove_drain_status(context, drain_cfg.clone(), volume).await;
        }
    }
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
            let move_requester = ReplicaMoveRequester::PoolDrain(drain_cfg.clone());
            volume.update_move_config(move_requester).await;
        }
    }
}

async fn remove_drain_status(
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
            volume.remove_move_config().await;
        }
    }
}
