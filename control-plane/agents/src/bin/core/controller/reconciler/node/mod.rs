mod nexus;

use crate::controller::{
    reconciler::node::nexus::NodeNexusReconciler,
    task_poller::{PollContext, PollPeriods, PollResult, PollTimer, TaskPoller},
};

/// Node reconciler loop which moves nexuses from draining nodes.
#[derive(Debug)]
pub(crate) struct NodeReconciler {
    counter: PollTimer,
    poll_targets: Vec<Box<dyn TaskPoller>>,
}
impl NodeReconciler {
    /// Return new `Self` with the provided period.
    pub(crate) fn from(period: PollPeriods) -> Self {
        NodeReconciler {
            counter: PollTimer::from(period),
            poll_targets: vec![Box::new(NodeNexusReconciler::new())],
        }
    }
    /// Return new `Self` with the default period.
    pub(crate) fn new() -> Self {
        Self::from(1)
    }
}

#[async_trait::async_trait]
impl TaskPoller for NodeReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = Vec::with_capacity(self.poll_targets.len());
        for target in &mut self.poll_targets {
            results.push(target.try_poll(context).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}
