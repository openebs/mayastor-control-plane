use crate::controller::{
    reconciler::{PollContext, TaskPoller},
    resources::{operations_helper::OperationSequenceGuard, ResourceMutex},
    task_poller::{PollResult, PollerState},
};

use stor_port::types::v0::{store::volume::VolumeSpec, transport::VolumeStatus};

/// Offline Volume Rebuild reconciler.
///
/// Detects unpublished volumes whose replicas have become unhealthy and would
/// benefit from a rebuild. Iteration 1 only logs eligible candidates; the
/// actual rebuild action is added in a follow-up iteration.
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
    skip(context, volume_spec),
    fields(volume.uuid = %volume_spec.uuid(), request.reconcile = true)
)]
async fn offline_rebuild_reconcile(
    volume_spec: &mut ResourceMutex<VolumeSpec>,
    context: &PollContext,
) -> PollResult {
    let volume = match volume_spec.operation_guard() {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };

    if !volume.as_ref().policy.self_heal
        || !volume.as_ref().status.created()
        || volume.as_ref().target().is_some()
    {
        return PollResult::Ok(PollerState::Idle);
    }

    let volume_state = context.registry().volume_state(volume.uuid()).await?;
    if volume_state.status == VolumeStatus::Degraded {
        tracing::debug!(
            volume.uuid = %volume.uuid(),
            "Unpublished volume is Degraded; eligible for offline rebuild"
        );
    }

    PollResult::Ok(PollerState::Idle)
}
