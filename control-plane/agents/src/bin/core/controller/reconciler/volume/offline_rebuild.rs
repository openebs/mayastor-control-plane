use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    resources::{
        operations::ResourcePublishing, operations_helper::OperationSequenceGuard,
        OperationGuardArc, ResourceMutex,
    },
    task_poller::{PollResult, PollerState},
};

use stor_port::types::v0::{
    store::volume::VolumeSpec,
    transport::{PublishVolume, UnpublishVolume, VolumeState, VolumeStatus},
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

        let mut results = vec![];
        let volumes = context.specs().volumes_rsc();
        for mut volume in volumes {
            results.push(offline_rebuild_reconcile(&mut volume, context).await);
        }
        Self::squash_results(results)
    }
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(volume.uuid = %volume_spec.uuid(), request.reconcile = true)
)]
async fn offline_rebuild_reconcile(
    volume_spec: &mut ResourceMutex<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let mut volume = match volume_spec.operation_guard() {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };

    if !volume.as_ref().policy.self_heal || !volume.as_ref().status.created() {
        return PollResult::Ok(PollerState::Idle);
    }

    // Early bails that don't need the volume state: a real (shared) publish is
    // not our concern, and a never-published volume has no data to rebuild.
    let has_offline_rebuild_target = match volume.as_ref().target() {
        Some(target) => {
            if target.protocol().is_some() {
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
        volume.lock().metadata.clear_offline_rebuild_degraded();
        return PollResult::Ok(PollerState::Idle);
    }

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

    initiate_offline_rebuild(&mut volume, context).await
}

/// Creates a non-shared nexus for the volume so the existing rebuild engine
/// (HotSpareReconciler) can restore faulted replicas.
async fn initiate_offline_rebuild(
    volume: &mut OperationGuardArc<VolumeSpec>,
    context: &PollContext,
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

    tracing::info!(
        volume.uuid = %volume.uuid(),
        "Initiating offline rebuild: creating non-shared nexus"
    );

    let request = PublishVolume::new(
        volume.uuid().clone(),
        None, // auto-select target node (prefers node with healthy replica)
        None, // share = None → nexus won't be shared
        Default::default(),
        vec![],
        Default::default(),
    );

    match volume.publish(registry, &request).await {
        Ok(_) => {
            tracing::info!(
                volume.uuid = %volume.uuid(),
                "Offline rebuild nexus created; HotSpareReconciler will handle the rebuild"
            );
            volume.lock().metadata.clear_offline_rebuild_degraded();
            PollResult::Ok(PollerState::Idle)
        }
        Err(error) => {
            tracing::warn!(
                volume.uuid = %volume.uuid(),
                %error,
                "Failed to create offline rebuild nexus"
            );
            PollResult::Ok(PollerState::Idle)
        }
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

    // Only `Online` is a clean enough signal to tear the nexus down. Anything
    // else (Degraded mid-rebuild, transient state during a node blip, etc.)
    // we wait on — tearing down would either churn against the grace timer
    // or kill a healthy in-progress rebuild during a brief hiccup.
    PollResult::Ok(PollerState::Idle)
}
