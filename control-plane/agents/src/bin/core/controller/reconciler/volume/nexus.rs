use crate::controller::{
    reconciler::{
        nexus::{
            capacity::enospc_children_onliner, faulted_nexus_remover, fixup_nexus_protocol,
            fixup_nexus_size, missing_nexus_recreate,
        },
        PollContext, TaskPoller,
    },
    resources::{operations_helper::OperationSequenceGuard, ResourceMutex},
    task_poller::{PollResult, PollerState},
};

use stor_port::types::v0::{store::volume::VolumeSpec, transport::VolumeStatus};

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
        let volumes = context.specs().volumes_rsc();
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
    let volume = match volume_spec.operation_guard() {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    if !volume.as_ref().policy.self_heal || !volume.as_ref().status.created() {
        return PollResult::Ok(PollerState::Idle);
    }

    match context.specs().volume_target_nexus(volume.as_ref()).await? {
        Some(mut nexus) => {
            if !nexus.as_ref().spec_status.created() || nexus.as_ref().is_shutdown() {
                return PollResult::Ok(PollerState::Idle);
            }

            let volume_state = context.registry().volume_state(volume.uuid()).await?;

            if volume_state.status != VolumeStatus::Online {
                faulted_nexus_remover(&mut nexus, context).await?;
                missing_nexus_recreate(&mut nexus, context).await?;
                enospc_children_onliner(&mut nexus, context).await?;
            }

            fixup_nexus_size(&mut nexus, &volume, context).await?;
            fixup_nexus_protocol(&mut nexus, context).await
        }
        None => PollResult::Ok(PollerState::Idle),
    }
}
