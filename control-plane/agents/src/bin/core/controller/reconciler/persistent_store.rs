use crate::controller::{
    reconciler::PollTriggerEvent,
    task_poller::{PollContext, PollEvent, PollResult, PollerState, TaskPoller},
};

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
            let dirty_snapshots = specs
                .reconcile_dirty_volume_snapshots(context.registry())
                .await;
            let dirty_nodes = specs.reconcile_dirty_nodes(context.registry()).await;

            if dirty_nexuses
                || dirty_replicas
                || dirty_volumes
                || dirty_pools
                || dirty_snapshots
                || dirty_nodes
            {
                return PollResult::Ok(PollerState::Busy);
            }
        }

        PollResult::Ok(PollerState::Idle)
    }
    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun => true,
            PollEvent::Triggered(PollTriggerEvent::SimulStart) => true,
            PollEvent::Triggered(_event) => true,
            PollEvent::Shutdown => true,
        }
    }
}
