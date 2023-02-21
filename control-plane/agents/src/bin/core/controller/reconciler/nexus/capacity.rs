use crate::controller::{
    resources::{OperationGuardArc, TraceSpan},
    task_poller::{PollContext, PollResult, PollerState},
};

use stor_port::types::v0::{store::nexus::NexusSpec, transport::NexusStatus};

/// Find thin-provisioned Nexus children which are degraded due to ENOSPC and try to online them
/// if sufficient pool space has been freed.
#[tracing::instrument(skip(nexus, context), level = "trace", fields(nexus.uuid = %nexus.uuid(), request.reconcile = true))]
pub(crate) async fn enospc_children_onliner(
    nexus: &mut OperationGuardArc<NexusSpec>,
    context: &PollContext,
) -> PollResult {
    let nexus_uuid = nexus.uuid();
    let nexus_state = context.registry().nexus(nexus_uuid).await?;
    let child_count = nexus_state.children.len();

    if nexus_state.status == NexusStatus::Degraded && child_count > 1 {
        for child in nexus_state.children.iter().filter(|c| c.enospc()) {
            nexus.warn_span(|| {
                tracing::info!(child.uri = child.uri.as_str(), "Found child with enospc")
            });

            // todo: online child
        }
    }

    PollResult::Ok(PollerState::Idle)
}
