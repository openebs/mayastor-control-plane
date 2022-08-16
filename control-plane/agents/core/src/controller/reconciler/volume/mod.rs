mod garbage_collector;
mod hot_spare;
mod nexus;

use crate::controller::task_poller::{PollContext, PollPeriods, PollResult, PollTimer, TaskPoller};

use crate::controller::reconciler::volume::{
    garbage_collector::GarbageCollector, hot_spare::HotSpareReconciler,
    nexus::VolumeNexusReconciler,
};

/// Volume Reconciler loop which:
/// 1. does the replica replacement
/// 2. volume garbage collection
#[derive(Debug)]
pub struct VolumeReconciler {
    counter: PollTimer,
    poll_targets: Vec<Box<dyn TaskPoller>>,
}
impl VolumeReconciler {
    /// Return new `Self` with the provided period
    pub fn from(period: PollPeriods) -> Self {
        VolumeReconciler {
            counter: PollTimer::from(period),
            poll_targets: vec![
                Box::new(HotSpareReconciler::new()),
                Box::new(GarbageCollector::new()),
                Box::new(VolumeNexusReconciler::new()),
            ],
        }
    }
    /// Return new `Self` with the default period
    pub fn new() -> Self {
        Self::from(1)
    }
}

#[async_trait::async_trait]
impl TaskPoller for VolumeReconciler {
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
