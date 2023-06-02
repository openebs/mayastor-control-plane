use crate::controller::{
    reconciler::snapshot::garbage_collector::GarbageCollector,
    task_poller::{PollContext, PollPeriods, PollResult, PollTimer, TaskPoller},
};

mod garbage_collector;

/// VolumeSnapshot Reconciler.
#[derive(Debug)]
pub(crate) struct VolumeSnapshotReconciler {
    counter: PollTimer,
    poll_targets: Vec<Box<dyn TaskPoller>>,
}
impl VolumeSnapshotReconciler {
    /// Return new `Self` with the provided period.
    pub(crate) fn from(period: PollPeriods) -> Self {
        VolumeSnapshotReconciler {
            counter: PollTimer::from(period),
            poll_targets: vec![Box::new(GarbageCollector::new())],
        }
    }
    /// Return new `Self` with the default period.
    pub(crate) fn new() -> Self {
        Self::from(1)
    }
}

#[async_trait::async_trait]
impl TaskPoller for VolumeSnapshotReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];
        for target in &mut self.poll_targets {
            results.push(target.try_poll(context).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}
