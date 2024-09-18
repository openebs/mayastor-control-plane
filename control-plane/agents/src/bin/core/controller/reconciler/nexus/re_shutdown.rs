use crate::controller::{
    reconciler::PollTriggerEvent,
    resources::operations_helper::OperationSequenceGuard,
    task_poller::{PollContext, PollEvent, PollResult, PollerState, TaskPoller},
};

/// ReShutdown nexuses if node comes back online with the Nexus intact.
#[derive(Debug)]
pub(super) struct ReShutdown {}
impl ReShutdown {
    /// Return a new `Self`.
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for ReShutdown {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        // Fetch all nexuses that are not properly shutdown
        for nexus in context.registry().specs().failed_shutdown_nexuses().await {
            let Some(volume_id) = &nexus.immutable_ref().owner else {
                continue;
            };
            let Ok(_volume) = context.specs().volume(volume_id).await else {
                continue;
            };

            let Ok(mut nexus) = nexus.operation_guard() else {
                continue;
            };

            nexus.re_shutdown_nexus(context.registry()).await;
        }
        PollResult::Ok(PollerState::Idle)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        false
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        matches!(
            context.event(),
            PollEvent::Triggered(PollTriggerEvent::Start)
                | PollEvent::Triggered(PollTriggerEvent::NodeStateChangeOnline)
                | PollEvent::Triggered(PollTriggerEvent::NodeDrain)
        )
    }
}
