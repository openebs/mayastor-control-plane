use crate::core::{
    specs::{OperationSequenceGuard, SpecOperations},
    task_poller::{PollContext, PollEvent, PollResult, PollTimer, PollerState, TaskPoller},
};
use common_lib::types::v0::{message_bus::ReplicaOwners, store::OperationMode};
use std::ops::Deref;

/// Replica reconciler
#[derive(Debug)]
pub struct ReplicaReconciler {
    counter: PollTimer,
}

impl ReplicaReconciler {
    /// Return a new `Self`
    pub fn new() -> Self {
        Self {
            counter: PollTimer::from(1),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for ReplicaReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        destroy_orphaned_replicas(context).await
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

/// Destroy orphaned replicas.
/// Orphaned replicas are those that are managed but which don't have any owners.
async fn destroy_orphaned_replicas(context: &PollContext) -> PollResult {
    for replica in context.specs().get_replicas() {
        let _guard = match replica.operation_guard(OperationMode::ReconcileStart) {
            Ok(guard) => guard,
            Err(_) => return PollResult::Ok(PollerState::Busy),
        };

        let replica_spec = replica.lock().deref().clone();
        if replica_spec.managed && !replica_spec.owned() {
            match context
                .specs()
                .destroy_replica_spec(
                    context.registry(),
                    &replica_spec,
                    ReplicaOwners::default(),
                    false,
                    OperationMode::ReconcileStep,
                )
                .await
            {
                Ok(_) => {
                    tracing::info!(replica.uuid=%replica_spec.uuid, "Successfully destroyed orphaned replica");
                }
                Err(e) => {
                    tracing::trace!(replica.uuid=%replica_spec.uuid, error=%e, "Failed to destroy orphaned replica");
                }
            }
        }
    }
    PollResult::Ok(PollerState::Idle)
}
