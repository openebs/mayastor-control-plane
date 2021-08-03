use crate::core::{
    reconciler::{PollContext, TaskPoller},
    specs::SpecOperations,
    task_poller::{PollResult, PollerState},
};

use common_lib::types::v0::store::volume::VolumeSpec;
use parking_lot::Mutex;
use std::sync::Arc;

/// Volume HotSpare reconciler
#[derive(Debug)]
pub(super) struct HotSpareReconciler {}
impl HotSpareReconciler {
    /// Return a new `Self`
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for HotSpareReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        let volumes = context.registry().specs.get_locked_volumes();
        for volume in volumes {
            results.push(hot_spare_reconcile(volume, context).await);
        }
        Self::squash_results(results)
    }
}

async fn hot_spare_reconcile(volume: Arc<Mutex<VolumeSpec>>, context: &PollContext) -> PollResult {
    let uuid = volume.lock().uuid.clone();
    let state = context.registry().get_volume_state(&uuid).await?;
    if !volume.lock().state_synced(&state) {
        // todo: reconcile the volume object
        tracing::warn!("Volume '{}' needs to be reconciled", uuid);
    }

    PollResult::Ok(PollerState::Idle)
}
