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

use agents::errors::SvcError;
use std::cmp::Ordering;
use stor_port::types::v0::{
    store::{
        pool::{DrainConfig, DrainProgressOp, UnwindSpare},
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
/// Also, handles the replica move configs. Currently, replica move is requested by Pool drain.
/// Creates a spare replica if requested, monitors the spare and destroys moving replica when
/// spare rebuild completes.
async fn volume_replica_count_reconciler(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
) -> PollResult {
    forget_lost_spare(volume, context).await;
    let step = ReplicaCountStep::next(
        volume.as_ref().num_replicas as usize,
        context.specs().volume_replicas(volume.uuid()).len(),
        volume.as_ref().metadata.drain_config(),
    );
    match step {
        ReplicaCountStep::Idle => PollResult::Ok(PollerState::Idle),
        step => volume_replica_count_reconciler_traced(volume, nexus, context, step).await,
    }
}

/// The next step of the volume replica count reconciliation.
#[derive(Debug)]
enum ReplicaCountStep {
    /// The volume has fewer replicas than required.
    /// Restoring the required replica count takes precedence over any replica move.
    CreateMissing { current: usize, required: usize },
    /// The replica move needs its spare placed.
    CreateSpare(DrainConfig),
    /// The volume has more replicas than expected.
    RemoveExcess { current: usize, expected: usize },
    /// The replica count is as expected, drive the replica move.
    ProgressMove(DrainConfig),
    /// The replica count is as expected and there is no replica move.
    Idle,
}

impl ReplicaCountStep {
    /// Decide the next step, given the required and current replica counts and the replica move.
    fn next(required: usize, current: usize, drain_cfg: Option<DrainConfig>) -> Self {
        if current < required {
            return Self::CreateMissing { current, required };
        }
        let Some(cfg) = drain_cfg else {
            return if current > required {
                Self::RemoveExcess {
                    current,
                    expected: required,
                }
            } else {
                Self::Idle
            };
        };
        // A replica move which over-replicates is entitled to one replica above the required count.
        let expected = required + usize::from(!cfg.unsafe_evict());
        if current > expected {
            Self::RemoveExcess { current, expected }
        } else if current < expected && cfg.spare_not_placed() {
            Self::CreateSpare(cfg)
        } else {
            Self::ProgressMove(cfg)
        }
    }
}

#[tracing::instrument(skip(context, volume), fields(volume.uuid = %volume.uuid(), request.reconcile = true, event = ?context.event()))]
async fn volume_replica_count_reconciler_traced(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
    step: ReplicaCountStep,
) -> PollResult {
    match step {
        // Guard: do not materialise a fresh, empty replica when the volume has
        // already been published but currently has zero spec'd replicas.
        //
        // A volume that has never been published has no `health_info_id` and
        // no data to lose, so a fresh replica there is correct.
        ReplicaCountStep::CreateMissing { current: 0, .. }
            if volume.as_ref().health_info_id().is_some() =>
        {
            warn_data_loss_once(volume);
            PollResult::Ok(PollerState::Idle)
        }
        ReplicaCountStep::CreateMissing { current, required } => {
            add_missing_replicas(volume, context, current, required).await
        }
        ReplicaCountStep::CreateSpare(cfg) => {
            create_spare_replica(volume, context, cfg, nexus).await
        }
        ReplicaCountStep::RemoveExcess { current, expected } => {
            remove_excess_replicas(volume, context, expected, current).await
        }
        ReplicaCountStep::ProgressMove(cfg) => {
            progress_replica_move(volume, nexus, context, cfg).await
        }
        ReplicaCountStep::Idle => PollResult::Ok(PollerState::Idle),
    }
}

/// The volume has fewer replicas than required: create the missing ones.
async fn add_missing_replicas(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    current_replica_count: usize,
    required_replica_count: usize,
) -> PollResult {
    volume.warn_span(|| {
        tracing::warn!(
            "The volume has '{current_replica_count}' replica(s) but it should have '{required_replica_count}'",
        )
    });

    let missing_replica_count = required_replica_count - current_replica_count;
    let created = create_missing_replica(volume, context, missing_replica_count).await;
    PollResult::Ok(if created == missing_replica_count {
        PollerState::Idle
    } else {
        PollerState::Busy
    })
}

/// The reconciler cannot recover a published volume with no replicas on its own.
/// Emit the warning exactly once per agent lifetime per volume to avoid flooding
/// logs at the reconcile cadence.
fn warn_data_loss_once(volume: &mut OperationGuardArc<VolumeSpec>) {
    if volume.as_ref().data_loss_warned() {
        return;
    }
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

/// The volume has more replicas than its effective replica count: remove the unused ones.
async fn remove_excess_replicas(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    effective_replica_count: usize,
    current_replica_count: usize,
) -> PollResult {
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
    // The replica count is re-evaluated on the next poll.
    PollResult::Ok(PollerState::Busy)
}

/// The next step of a replica move, once the volume is at its effective replica count.
#[derive(Debug, PartialEq)]
enum ReplicaMoveStep {
    /// No spare is required, so the moving replica is evicted straight away.
    EvictMoving,
    /// The spare is online, so the moving replica can be retired.
    RetireMoving,
    /// The spare has to be unwound.
    DestroySpare(ReplicaId),
    /// The move was cancelled and there is no spare left: drop the move config.
    EndMove,
    /// Nothing to do yet (eg: the spare is still rebuilding).
    Wait,
}

impl ReplicaMoveStep {
    /// Decide the next step of the replica move.
    fn next(cfg: &DrainConfig, volume_online: bool, spare_online: bool) -> Self {
        if cfg.unsafe_evict() && volume_online {
            return Self::EvictMoving;
        }
        match (&cfg.unwind_spare, cfg.spare()) {
            (Some(UnwindSpare::Respare | UnwindSpare::Cancelled), Some(spare)) => {
                return Self::DestroySpare(spare.clone());
            }
            // Drain cancelled: end the move, keeping the moving replica.
            (Some(UnwindSpare::Cancelled), None) => return Self::EndMove,
            _ => {}
        }
        if spare_online {
            Self::RetireMoving
        } else {
            Self::Wait
        }
    }
}

/// The volume is at its expected replica count: drive the in-progress replica move.
async fn progress_replica_move(
    volume: &mut OperationGuardArc<VolumeSpec>,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    context: &PollContext,
    cfg: DrainConfig,
) -> PollResult {
    let Ok(vol) = context.registry().volume(&volume.as_ref().uuid).await else {
        return PollResult::Ok(PollerState::Idle);
    };
    let volume_online = vol.state().status == VolumeStatus::Online;

    match ReplicaMoveStep::next(&cfg, volume_online, vol.spare_online()) {
        ReplicaMoveStep::EvictMoving => {
            info!(
                "remove replica {:?} from volume {:?}",
                cfg.moving_replica,
                volume.as_ref().uuid
            );
            destroy_moving_replica(nexus, volume, context, &cfg.moving_replica).await
        }
        ReplicaMoveStep::RetireMoving => {
            destroy_moving_replica(nexus, volume, context, &cfg.moving_replica).await
        }
        ReplicaMoveStep::DestroySpare(spare) => {
            destroy_spare_replica(nexus, volume, context, &spare).await
        }
        ReplicaMoveStep::EndMove => {
            remove_drain_status(context, cfg, volume).await;
            PollResult::Ok(PollerState::Idle)
        }
        ReplicaMoveStep::Wait => PollResult::Ok(PollerState::Idle),
    }
}

/// If the drain config records a spare replica which is no longer a replica of this volume (its
/// child was removed and the replica destroyed), forget it so that a fresh
/// spare gets placed.
async fn forget_lost_spare(volume: &mut OperationGuardArc<VolumeSpec>, context: &PollContext) {
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
async fn create_spare_replica(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    mut drain_cfg: DrainConfig,
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
) -> PollResult {
    volume.info_span(|| {
        tracing::info!(
            replica.uuid = %drain_cfg.moving_replica,
            "Creating a spare replica for the moving replica"
        )
    });
    if drain_cfg.placement_started_at.is_none() {
        drain_cfg.update_placement_started_at();
        update_drain_status(context, drain_cfg.clone(), volume).await;
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

                drain_cfg.update_spare(Some(replica.clone()));
                update_drain_status(context, drain_cfg, volume).await;
                Ok(PollerState::Idle)
            } else {
                error!(volume.uuid = %volume.uuid(), "No spare replica was created for volume");
                evict_if_applicable(context, drain_cfg, nexus, volume).await
            }
        }
        Err(error) => {
            let reason = error.full_string();
            error!(volume.uuid = %volume.uuid(), "No spare replica was created for volume: {reason}");
            evict_if_applicable(context, drain_cfg, nexus, volume).await
        }
    }
}

/// Create replicas for the volume to match user requested replica count.
async fn create_missing_replica(
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

/// Removes the replica from the volume, detaching it from the nexus first if there is one.
async fn remove_volume_replica(
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

async fn destroy_spare_replica(
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    spare_replica: &ReplicaId,
) -> PollResult {
    let removed = remove_volume_replica(nexus, volume, context, spare_replica).await;
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

async fn destroy_moving_replica(
    nexus: Option<&mut OperationGuardArc<NexusSpec>>,
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
    moving_replica: &ReplicaId,
) -> PollResult {
    let removed = remove_volume_replica(nexus, volume, context, moving_replica).await;
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

pub(crate) async fn update_drain_status(
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
