use crate::core::{
    reconciler::{PollContext, TaskPoller},
    specs::OperationSequenceGuard,
    task_poller::{PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState},
};
use common_lib::types::v0::store::{volume::VolumeSpec, OperationMode, TraceSpan};

use parking_lot::Mutex;
use std::sync::Arc;

/// Volume Garbage Collector reconciler
#[derive(Debug)]
pub(super) struct GarbageCollector {
    counter: PollTimer,
}
impl GarbageCollector {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {
            counter: PollTimer::from(5),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for GarbageCollector {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        for volume in context.specs().get_locked_volumes() {
            results.push(disown_unused_nexuses(&volume, context).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun => true,
            PollEvent::Triggered(PollTriggerEvent::VolumeDegraded) => true,
            PollEvent::Shutdown | PollEvent::Triggered(_) => false,
        }
    }
}

/// Given a volume
/// When any of its nexuses are no longer used
/// Then they should be disowned
/// And they should eventually be destroyed
#[tracing::instrument(level = "debug", skip(context, volume), fields(volume.uuid = %volume.lock().uuid, request.reconcile = true))]
async fn disown_unused_nexuses(
    volume: &Arc<Mutex<VolumeSpec>>,
    context: &PollContext,
) -> PollResult {
    let _guard = match volume.operation_guard(OperationMode::ReconcileStart) {
        Ok(guard) => guard,
        Err(_) => return PollResult::Ok(PollerState::Busy),
    };
    let mut results = vec![];
    let volume_clone = volume.lock().clone();

    for nexus in context.specs().get_volume_nexuses(&volume_clone.uuid) {
        match &volume_clone.target {
            Some(target) if target.nexus() == &nexus.lock().uuid => continue,
            _ => {}
        };
        let nexus_clone = nexus.lock().clone();

        nexus_clone.warn_span(|| tracing::warn!("Attempting to disown unused nexus"));
        // the nexus garbage collector will destroy the disowned nexuses
        match context
            .specs()
            .disown_nexus(context.registry(), &nexus)
            .await
        {
            Ok(_) => {
                nexus_clone.info_span(|| tracing::info!("Successfully disowned unused nexus"));
            }
            Err(error) => {
                nexus_clone.error_span(|| tracing::error!("Failed to disown unused nexus"));
                results.push(Err(error));
            }
        }
    }

    GarbageCollector::squash_results(results)
}
