#[cfg(test)]
mod tests;

use crate::core::{
    specs::{OperationSequenceGuard, SpecOperations},
    task_poller::{
        PollContext, PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState, TaskPoller,
    },
};
use common_lib::types::v0::{
    message_bus::ReplicaOwners,
    store::{replica::ReplicaSpec, OperationMode},
};
use parking_lot::Mutex;
use std::{ops::Deref, sync::Arc};

/// Replica reconciler
#[derive(Debug)]
pub struct ReplicaReconciler {
    counter: PollTimer,
}

impl ReplicaReconciler {
    /// Return a new `Self`
    pub fn new() -> Self {
        Self {
            counter: PollTimer::from(5),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for ReplicaReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let mut results = vec![];

        for replica in context.specs().get_replicas() {
            results.push(disown_missing_owners(context, &replica).await);
            results.push(destroy_orphaned_replicas(context, &replica).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }

    async fn poll_event(&mut self, context: &PollContext) -> bool {
        match context.event() {
            PollEvent::TimedRun | PollEvent::Triggered(PollTriggerEvent::Start) => true,
            PollEvent::Shutdown | PollEvent::Triggered(_) => false,
        }
    }
}

/// Remove replica owners who no longer exist.
/// In the event that the replicas become orphaned (have no owners) they will be destroyed by the
/// 'destroy_orphaned_replicas' reconcile loop.
async fn disown_missing_owners(
    context: &PollContext,
    replica: &Arc<Mutex<ReplicaSpec>>,
) -> PollResult {
    let specs = context.specs();

    // If we obtain the operation guard no one else can be modifying the replica spec.
    if let Ok(_guard) = replica.operation_guard(OperationMode::ReconcileStart) {
        let replica_spec = replica.lock().clone();

        if replica_spec.managed && replica_spec.owned() {
            let mut owner_removed = false;
            let owners = &replica_spec.owners;

            if let Some(volume) = owners.volume() {
                if specs.get_volume(volume).is_err() {
                    // The volume no longer exists. Remove it as an owner.
                    replica.lock().owners.disowned_by_volume();
                    owner_removed = true;
                    tracing::info!(replica.uuid=%replica_spec.uuid, volume.uuid=%volume, "Removed volume as replica owner");
                }
            };

            owners.nexuses().iter().for_each(|nexus| {
                if specs.get_nexus(nexus).is_none() {
                    // The nexus no longer exists. Remove it as an owner.
                    replica.lock().owners.disowned_by_nexus(nexus);
                    owner_removed = true;
                    tracing::info!(replica.uuid=%replica_spec.uuid, nexus.uuid=%nexus, "Removed nexus as replica owner");
                }
            });

            if owner_removed {
                let replica_clone = replica.lock().clone();
                if let Err(error) = context.registry().store_obj(&replica_clone).await {
                    // Log the fact that we couldn't persist the changes.
                    // If we reload the stale info from the persistent store (on a restart) we
                    // will run this reconcile loop again and tidy it up, so no need to retry
                    // here.
                    tracing::error!(replica.uuid=%replica_clone.uuid, error=%error, "Failed to persist disowned replica")
                }
            }
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Destroy orphaned replicas.
/// Orphaned replicas are those that are managed but which don't have any owners.
async fn destroy_orphaned_replicas(
    context: &PollContext,
    replica: &Arc<Mutex<ReplicaSpec>>,
) -> PollResult {
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
    PollResult::Ok(PollerState::Idle)
}
