use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    registry::NumRebuilds,
    resources::{
        operations::ResourcePublishing, operations_helper::OperationSequenceGuard,
        OperationGuardArc, ResourceMutex,
    },
    task_poller::{PollResult, PollerState},
};

use stor_port::types::v0::{
    store::volume::VolumeSpec,
    transport::{UnpublishVolume, VolumeState, VolumeStatus},
};

/// Offline Volume Rebuild.
///
/// Detects unpublished volumes whose replicas have become unhealthy and
/// creates a temporary non-shared nexus so the existing rebuild engine
/// can restore them. Once rebuild completes, tears the nexus down.
///
/// The per-volume grace-period state lives on `VolumeMetadata.runtime` so it
/// gets cleaned up automatically when the volume is deleted.
///
/// See `designs/replicated-pv/mayastor/offline-volume-rebuild.md` for the
/// full design.
#[derive(Debug)]
pub(super) struct OfflineRebuildReconciler {}

impl OfflineRebuildReconciler {
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for OfflineRebuildReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        if !context.registry().offline_rebuild_enabled() {
            return PollResult::Ok(PollerState::Idle);
        }

        // Nothing can start while the system-wide limit is already met, so don't walk
        // the volumes at all. Once is enough: the count behind this is refreshed when
        // node state is, on another thread, so re-reading it as we go would give the
        // same answer.
        if context.registry().rebuild_allowed().await.is_err() {
            tracing::debug!("Offline rebuilds deferred: max concurrent rebuilds reached");
            return PollResult::Ok(PollerState::Idle);
        }

        let mut results = vec![];
        let mut volumes = context.specs().volumes_rsc();

        // Requested volumes go first, oldest request first, so the queue is ordered by
        // when the operator asked rather than by where the volume happens to sit.
        // `volumes_rsc` returns insertion order, which does not change between polls,
        // so without this a requested volume sitting behind others would keep losing
        // the slot on every poll rather than just once. Stable, so volumes which were
        // not requested keep their relative order behind those which were.
        volumes.sort_by_key(|volume| {
            volume
                .lock()
                .metadata
                .offline_rebuild_requested_at()
                .map_or(RequestOrder::Unrequested, RequestOrder::RequestedAt)
        });

        // Offline rebuilds also consume `max_rebuilds` slots, so left alone a batch
        // of them can crowd out published volumes which are serving I/O. Budget is
        // whatever headroom is left under `max_offline_rebuilds` once the offline
        // rebuilds already running are accounted for, and it is spent as we go so a
        // single poll cannot overshoot.
        let mut budget = crate::volume::offline_rebuild_budget(&volumes, context.registry()).await;

        for mut volume in volumes {
            // Unlike the system-wide limit above, the budget is spent as we go, so it
            // is worth re-reading. Stop once it runs out rather than carrying on down
            // the queue. The check belongs here and not in the per-volume reconcile,
            // which returns Busy for unrelated reasons too, such as a volume still
            // inside its grace period; breaking on that would let one waiting volume
            // hold up the rest.
            if budget == Some(0) {
                tracing::debug!(
                    "Offline rebuilds deferred: max concurrent offline rebuilds reached"
                );
                results.push(PollResult::Ok(PollerState::Busy));
                break;
            }
            results.push(offline_rebuild_reconcile(&mut volume, context, &mut budget).await);
        }
        Self::squash_results(results)
    }
}

/// Queue order for one poll: requested volumes first, oldest request first, then
/// everything else in the order it already had.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum RequestOrder {
    RequestedAt(std::time::Instant),
    Unrequested,
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(volume.uuid = %volume_spec.uuid(), request.reconcile = true, event = ?context.event())
)]
async fn offline_rebuild_reconcile(
    volume_spec: &mut ResourceMutex<VolumeSpec>,
    context: &PollContext,
    budget: &mut Option<NumRebuilds>,
) -> PollResult {
    let mut volume = match volume_spec.operation_guard() {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };

    if (!volume.as_ref().policy.self_heal && volume.as_ref().metadata.drain_config().is_none())
        || !volume.as_ref().status.created()
    {
        return PollResult::Ok(PollerState::Idle);
    }

    // Early bails that don't need the volume state: a real (shared) publish is
    // not our concern, an unshared target we don't own is also not our concern
    // (e.g. user produced one via direct REST publish), and a never-published
    // volume has no data to rebuild.
    let has_offline_rebuild_target = match volume.as_ref().target() {
        Some(target) => {
            if target.protocol().is_some() || !volume.as_ref().is_offline_rebuild_target() {
                return PollResult::Ok(PollerState::Idle);
            }
            true
        }
        None => {
            if volume.as_ref().health_info_id().is_none() {
                return PollResult::Ok(PollerState::Idle);
            }
            false
        }
    };
    tracing::info!("has offline rebuild target {}", has_offline_rebuild_target);

    // Must use the health-aware state: for an unpublished volume the plain
    // volume_state() derives status from the replica spec count (unchanged when a
    // node dies), so it reports Online and we'd never detect the degradation.
    // volume_state_health() factors in actual online replica health.
    let volume_state = context
        .registry()
        .volume_state_health(volume.uuid())
        .await?;

    if has_offline_rebuild_target {
        return teardown_if_rebuilt(&mut volume, &volume_state, context).await;
    }

    // Volume is unpublished with prior health info. Wait for it to actually be
    // Degraded, then enforce the grace period before initiating rebuild.
    if volume_state.status != VolumeStatus::Degraded {
        // Drops any outstanding request too, so it cannot silently skip the grace
        // period on some later, unrelated degradation.
        volume.clear_offline_rebuild_marks();
        return PollResult::Ok(PollerState::Idle);
    }

    let spare_need_rebuilding = volume.as_ref().metadata.need_spare()
        && !context
            .registry()
            .volume(volume.uuid())
            .await
            .is_ok_and(|v| v.spare_online());

    // Scale-up added a replica that was never part of the last-published
    // nexus, so it is definitively out-of-sync — not a transient blip the
    // grace period is meant to absorb. If that replica is also currently
    // online it can serve as an in-place rebuild target, so no fresh pool is
    // needed either. Also, a spare which needs rebuilding is an in-place target.
    let has_in_place_target = volume
        .has_scale_up_rebuild_target(&volume_state.replica_topology, context.registry())
        .await?
        || spare_need_rebuilding;

    // Skips the wait only: the checks below still apply.
    let requested = volume.offline_rebuild_requested();

    if requested {
        tracing::info!(
            volume.uuid = %volume.uuid(),
            "Bypassing grace period: rebuild requested by operator"
        );
    } else if has_in_place_target {
        tracing::info!(
            volume.uuid = %volume.uuid(),
            "Bypassing grace period: replica added since last publish"
        );
    } else {
        let degraded_for = volume.lock().metadata.offline_rebuild_degraded();
        let grace_period = context.registry().offline_rebuild_grace_period();
        if degraded_for < grace_period {
            tracing::debug!(
                volume.uuid = %volume.uuid(),
                remaining = ?grace_period.saturating_sub(degraded_for),
                "Offline rebuild waiting for grace period"
            );
            return PollResult::Ok(PollerState::Busy);
        }
    }

    initiate_offline_rebuild(
        &mut volume,
        &volume_state,
        context,
        has_in_place_target,
        budget,
    )
    .await
}

/// Creates a non-shared nexus for the volume so the existing rebuild engine
/// (HotSpareReconciler) can restore faulted replicas.
///
/// `has_in_place_target` skips the candidate-pool check: when an extra
/// replica already sits in the topology (e.g. from scale-up while
/// unpublished), that replica is itself the rebuild destination and no fresh
/// pool is required.
async fn initiate_offline_rebuild(
    volume: &mut OperationGuardArc<VolumeSpec>,
    volume_state: &VolumeState,
    context: &PollContext,
    has_in_place_target: bool,
    budget: &mut Option<NumRebuilds>,
) -> PollResult {
    let registry = context.registry();

    // Respect the system-wide rebuild concurrency limit.
    if registry.rebuild_allowed().await.is_err() {
        tracing::debug!(
            volume.uuid = %volume.uuid(),
            "Offline rebuild deferred: max concurrent rebuilds reached"
        );
        return PollResult::Ok(PollerState::Busy);
    }

    // And the offline-specific one, which keeps slots free for published volumes.
    if *budget == Some(0) {
        tracing::debug!(
            volume.uuid = %volume.uuid(),
            "Offline rebuild deferred: max concurrent offline rebuilds reached"
        );
        return PollResult::Ok(PollerState::Busy);
    }

    match volume
        .start_offline_rebuild(volume_state, registry, has_in_place_target)
        .await
    {
        Ok(()) => {
            // Spend the slot so later volumes in this poll see the smaller budget;
            // the next poll recounts from the live targets. One here matches the
            // floor the count applies to a target whose rebuild hasn't started.
            if let Some(budget) = budget.as_mut() {
                *budget = budget.saturating_sub(1);
            }
            PollResult::Ok(PollerState::Idle)
        }
        // The reconciler keeps trying on later polls, so nothing to escalate here.
        Err(_) => PollResult::Ok(PollerState::Idle),
    }
}

/// Tears down the temporary nexus once the volume is fully rebuilt. For any
/// other state we wait — a stuck or transiently-unavailable nexus is left in
/// place so we don't churn (creating and destroying repeatedly) or accidentally
/// tear down a healthy in-progress rebuild during a brief node blip.
async fn teardown_if_rebuilt(
    volume: &mut OperationGuardArc<VolumeSpec>,
    volume_state: &VolumeState,
    context: &PollContext,
) -> PollResult {
    if volume_state.status == VolumeStatus::Online {
        tracing::info!(
            volume.uuid = %volume.uuid(),
            "Offline rebuild complete; tearing down temporary nexus"
        );

        let request = UnpublishVolume::new(volume.uuid(), false, vec![]);
        match volume.unpublish(context.registry(), &request).await {
            Ok(_) => {
                tracing::info!(
                    volume.uuid = %volume.uuid(),
                    "Temporary nexus destroyed; volume returned to unpublished state"
                );
            }
            Err(error) => {
                tracing::warn!(
                    volume.uuid = %volume.uuid(),
                    %error,
                    "Failed to tear down offline rebuild nexus"
                );
            }
        }
        return PollResult::Ok(PollerState::Idle);
    }

    // Safe teardown when the rebuild has no chance of progressing: nexus is up
    // but `Faulted` means no healthy source children remain (e.g. the source
    // node went offline mid-rebuild).
    if volume_state.status == VolumeStatus::Faulted {
        let request = UnpublishVolume::new(volume.uuid(), false, vec![]);
        if let Err(error) = volume.unpublish(context.registry(), &request).await {
            tracing::warn!(
                volume.uuid = %volume.uuid(),
                %error,
                "Failed to tear down stuck offline rebuild nexus"
            );
        } else {
            tracing::warn!(
                volume.uuid = %volume.uuid(),
                "Offline rebuild source no longer available; tore down temporary nexus"
            );
        }
        // Restart the grace timer from scratch when viability returns, so a
        // recovering node has to clear the same wait window a fresh degradation would.
        // An operator request is deliberately left standing: it was not honoured, and
        // nothing about losing viability says they stopped wanting the rebuild. It
        // still only skips the wait, so the checks above continue to apply.
        volume.lock().metadata.clear_offline_rebuild_degraded();
        return PollResult::Ok(PollerState::Idle);
    }

    // Only `Online` and `Faulted` are clean enough signals to act on. Anything
    // else (Degraded mid-rebuild, transient state during a node blip, etc.)
    // we wait on — tearing down would either churn against the grace timer
    // or kill a healthy in-progress rebuild during a brief hiccup.
    PollResult::Ok(PollerState::Idle)
}
