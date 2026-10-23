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

use crate::controller::reconciler::volume::drain_utils::{
    create_missing_replica, create_spare_replica, forget_lost_spare, handle_drain_replica_move,
};
use std::cmp::Ordering;
use stor_port::types::v0::store::volume::OverReplicationRequester;

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

#[tracing::instrument(level = "debug", skip(context, volume_spec), fields(volume.uuid = %volume_spec.uuid(), request.reconcile = true, event = ?context.event()))]
async fn hot_spare_reconcile(
    volume_spec: &mut ResourceMutex<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let mut volume = match volume_spec.operation_guard() {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    let volume_state = context.registry().volume_state(volume.uuid()).await?;

    if !volume.as_ref().policy.self_heal && volume.as_ref().metadata.drain_config().is_none() {
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

#[tracing::instrument(skip(context, volume, nexus), fields(volume = ?volume.as_ref(), nexus.uuid = %nexus.uuid(), nexus.node = %nexus.as_ref().node, volume.uuid = tracing::field::Empty, request.reconcile = true, event = ?context.event()))]
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

    let nexus_replica_children =
        nexus
            .as_ref()
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
    let effective_replica_count = volume.as_ref().effective_replica_count() as usize;
    match nexus_replica_children.cmp(&effective_replica_count) {
        Ordering::Less | Ordering::Greater => {
            nexus_replica_count_reconciler_traced(
                volume,
                nexus,
                nexus_state,
                nexus_replica_children,
                effective_replica_count,
                context,
            )
            .await
        }
        Ordering::Equal => PollResult::Ok(PollerState::Idle),
    }
}
#[tracing::instrument(skip(context, volume, nexus, nexus_state), fields(nexus.uuid = %nexus.uuid(), request.reconcile = true, event = ?context.event()))]
async fn nexus_replica_count_reconciler_traced(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: &mut OperationGuardArc<NexusSpec>,
    nexus_state: Nexus,
    nexus_replica_children: usize,
    effective_replica_count: usize,
    context: &PollContext,
) -> PollResult {
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
/// Also, handles the over-replication requests. Currently, over-replication is requested by Pool drain.
/// Creates a spare replica if requested, monitors the spare and destroys moving replica when
/// spare rebuild completes.
async fn volume_replica_count_reconciler(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
) -> PollResult {
    forget_lost_spare(volume, context).await;
    let required_replica_count = volume.as_ref().num_replicas as usize;
    let effective_replica_count = volume.as_ref().effective_replica_count() as usize;
    let current_replica_count = context.specs().volume_replicas(volume.uuid()).len();
    match current_replica_count.cmp(&effective_replica_count) {
        Ordering::Less | Ordering::Greater => {
            volume_replica_count_reconciler_traced(
                volume,
                nexus,
                context,
                required_replica_count,
                effective_replica_count,
                current_replica_count,
            )
            .await
        }
        Ordering::Equal => {
            if volume.as_ref().has_over_replication() {
                volume_replica_count_reconciler_traced(
                    volume,
                    nexus,
                    context,
                    required_replica_count,
                    effective_replica_count,
                    current_replica_count,
                )
                .await
            } else {
                Ok(PollerState::Idle)
            }
        }
    }
}

#[tracing::instrument(skip(context, volume), fields(volume.uuid = %volume.uuid(), request.reconcile = true, event = ?context.event()))]
async fn volume_replica_count_reconciler_traced(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
    required_replica_count: usize,
    effective_replica_count: usize,
    mut current_replica_count: usize,
) -> PollResult {
    match current_replica_count.cmp(&effective_replica_count) {
        Ordering::Less => {
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
                    effective_replica_count
                )
            });

            let missing_replica_count =
                required_replica_count.saturating_sub(current_replica_count);
            let drain_cfg = volume.as_ref().metadata.drain_config();
            // As volume replica count cannot be modified while replica move is applied on volume and
            // replica move cannot be applied on a non-online volume. We can get non-zero base_diff
            // while replica move is also there only if user tries to force trigger rebuild and replica
            // is removed. In that case doing a Full rebuild takes precedence.
            let creating_spare = missing_replica_count == 0
                && drain_cfg.as_ref().is_some_and(|cfg| cfg.spare_not_placed());
            if creating_spare {
                return create_spare_replica(volume, context, drain_cfg, nexus).await;
            } else {
                current_replica_count +=
                    create_missing_replica(volume, context, missing_replica_count).await
            }
        }
        Ordering::Greater => {
            volume.warn_span(|| {
                tracing::warn!(
                    "The volume has '{}' replica(s) but it should only have '{}'. Removing...",
                    current_replica_count,
                    effective_replica_count
                )
            });

            let diff = current_replica_count - effective_replica_count;
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
            if let Some(requester) = volume.as_ref().metadata.over_replication() {
                return match requester {
                    OverReplicationRequester::PoolDrain(drain) => {
                        handle_drain_replica_move(volume, nexus, context, drain).await
                    }
                };
            }
        }
    }

    PollResult::Ok(if current_replica_count == effective_replica_count {
        PollerState::Idle
    } else {
        PollerState::Busy
    })
}
