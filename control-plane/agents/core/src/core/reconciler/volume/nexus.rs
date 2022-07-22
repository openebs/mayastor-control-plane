use crate::core::{
    reconciler::{
        nexus::{fixup_nexus_protocol, missing_nexus_recreate},
        PollContext, TaskPoller,
    },
    specs::OperationSequenceGuard,
    task_poller::{PollResult, PollerState},
};

use common_lib::types::v0::store::{volume::VolumeSpec, OperationMode};

use crate::core::reconciler::nexus::faulted_nexus_remover;
use common_lib::types::v0::transport::VolumeStatus;
use parking_lot::Mutex;
use std::sync::Arc;

/// Volume nexus reconciler
/// When io-engine instances restart they come up "empty" and so we need to recreate
/// any previously created nexuses
#[derive(Debug)]
pub(super) struct VolumeNexusReconciler {}
impl VolumeNexusReconciler {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for VolumeNexusReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        let volumes = context.specs().get_locked_volumes();
        for volume in volumes {
            results.push(volume_nexus_reconcile(&volume, context).await);
        }
        Self::squash_results(results)
    }
}

#[tracing::instrument(level = "trace", skip(context, volume_spec), fields(volume.uuid = %volume_spec.lock().uuid, request.reconcile = true))]
async fn volume_nexus_reconcile(
    volume_spec: &Arc<Mutex<VolumeSpec>>,
    context: &PollContext,
) -> PollResult {
    let _guard = match volume_spec.operation_guard(OperationMode::ReconcileStart) {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    let volume = {
        let volume = volume_spec.lock();
        if !volume.policy.self_heal || !volume.status.created() {
            return PollResult::Ok(PollerState::Idle);
        }
        volume.clone()
    };
    match context.specs().get_volume_target_nexus(&volume) {
        Some(nexus_spec) => {
            let _guard = match nexus_spec.operation_guard(OperationMode::ReconcileStart) {
                Ok(guard) => guard,
                Err(_) => return PollResult::Ok(PollerState::Busy),
            };
            let mode = OperationMode::ReconcileStep;

            if !nexus_spec.lock().spec_status.created() {
                return PollResult::Ok(PollerState::Idle);
            }

            let volume_state = context.registry().get_volume_state(&volume.uuid).await?;

            if volume_state.status != VolumeStatus::Online {
                faulted_nexus_remover(&nexus_spec, context, mode).await?;
                missing_nexus_recreate(&nexus_spec, context, mode).await?;
            }
            fixup_nexus_protocol(&nexus_spec, context, mode).await
        }
        None => PollResult::Ok(PollerState::Idle),
    }
}
