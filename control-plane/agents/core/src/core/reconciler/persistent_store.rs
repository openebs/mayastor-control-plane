use crate::core::task_poller::{PollContext, PollResult, PollerState, TaskPoller};

/// Reconcile dirty specs in the persistent store.
/// This happens when we fail to update the persistent store and we have a "live" spec that
/// differs to what's written in the persistent store.
/// This reconciler basically attempts to write the dirty specs to the persistent store.
#[derive(Debug)]
pub(super) struct PersistentStoreReconciler {}
impl PersistentStoreReconciler {
    /// Return new `Self`
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskPoller for PersistentStoreReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let specs = context.specs();
        if context.registry().store_online().await {
            let dirty_pools = specs.reconcile_dirty_pools(context.registry()).await;
            let dirty_replicas = specs.reconcile_dirty_replicas(context.registry()).await;
            let dirty_nexuses = specs.reconcile_dirty_nexuses(context.registry()).await;
            let dirty_volumes = specs.reconcile_dirty_volumes(context.registry()).await;

            if dirty_nexuses || dirty_replicas || dirty_volumes || dirty_pools {
                return PollResult::Ok(PollerState::Busy);
            }
        }

        PollResult::Ok(PollerState::Idle)
    }
}
