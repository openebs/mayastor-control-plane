use crate::core::{
    reconciler::{PollContext, TaskPoller},
    specs::SpecOperations,
    task_poller::{PollEvent, PollResult, PollTimer, PollerState},
};

use common_lib::types::v0::store::volume::VolumeSpec;
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
        let volumes = context.specs().get_locked_volumes();
        for volume in volumes {
            let _ = garbage_collector_reconcile(volume, context).await;
        }
        PollResult::Ok(PollerState::Idle)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun => true,
            PollEvent::Shutdown | PollEvent::Triggered(_) => false,
        }
    }
}

async fn garbage_collector_reconcile(
    volume: Arc<Mutex<VolumeSpec>>,
    context: &PollContext,
) -> PollResult {
    let uuid = volume.lock().uuid.clone();
    let state = context.registry().get_volume_state(&uuid).await?;
    if volume.lock().state_synced(&state) {
        // todo: find all resources related to this volume Id and clean them up, if unused
        tracing::trace!("Collecting garbage for volume '{}'", uuid);
    }
    PollResult::Ok(PollerState::Idle)
}
