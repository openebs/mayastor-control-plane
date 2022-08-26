use crate::controller::{
    reconciler::{
        nexus::{fixup_nexus_protocol, missing_nexus_recreate},
        PollContext, TaskPoller,
    },
    resources::{operations_helper::OperationSequenceGuard, ResourceMutex},
    task_poller::{PollResult, PollerState},
};

use common_lib::types::v0::store::volume::VolumeSpec;

use crate::controller::reconciler::nexus::{enospc_children_faulter, faulted_nexus_remover};
use common_lib::types::v0::transport::VolumeStatus;

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
        for mut volume in volumes {
            results.push(volume_nexus_reconcile(&mut volume, context).await);
        }
        Self::squash_results(results)
    }
}

#[tracing::instrument(level = "trace", skip(context, volume_spec), fields(volume.uuid = %volume_spec.uuid(), request.reconcile = true))]
async fn volume_nexus_reconcile(
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

    match context
        .specs()
        .get_volume_target_nexus_guard(volume.as_ref())
        .await?
    {
        Some(mut nexus) => {
            if !nexus.as_ref().spec_status.created() {
                return PollResult::Ok(PollerState::Idle);
            }

            let volume_state = context.registry().get_volume_state(volume.uuid()).await?;

            if volume_state.status != VolumeStatus::Online {
                faulted_nexus_remover(&mut nexus, context).await?;
                missing_nexus_recreate(&mut nexus, context).await?;
                enospc_children_faulter(&mut nexus, context).await?;
            }
            fixup_nexus_protocol(&mut nexus, context).await
        }
        None => PollResult::Ok(PollerState::Idle),
    }
}
