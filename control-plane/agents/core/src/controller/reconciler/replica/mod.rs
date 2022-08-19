use crate::controller::{
    operations::ResourceLifecycle,
    reconciler::{GarbageCollect, ReCreate},
    specs::{OperationSequenceGuard, ResourceSpecsLocked, SpecOperationsHelper},
    task_poller::{
        PollContext, PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState, TaskPoller,
    },
};
use common_lib::types::v0::{
    store::{replica::ReplicaSpec, OperationGuardArc},
    transport::ReplicaOwners,
};

/// Replica reconciler
#[derive(Debug)]
pub(crate) struct ReplicaReconciler {
    counter: PollTimer,
}

impl ReplicaReconciler {
    /// Return a new `Self`
    pub(crate) fn new() -> Self {
        Self {
            counter: PollTimer::from(5),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for ReplicaReconciler {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let replicas = context.specs().get_replicas();
        let mut results = Vec::with_capacity(replicas.len());

        for replica in replicas {
            let mut replica = match replica.operation_guard() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            results.push(replica.garbage_collect(context).await);
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

#[async_trait::async_trait]
impl ReCreate for OperationGuardArc<ReplicaSpec> {
    async fn recreate_state(&mut self, _context: &PollContext) -> PollResult {
        // We get this automatically when recreating pools
        PollResult::Ok(PollerState::Idle)
    }
}

#[async_trait::async_trait]
impl GarbageCollect for OperationGuardArc<ReplicaSpec> {
    async fn garbage_collect(&mut self, context: &PollContext) -> PollResult {
        ReplicaReconciler::squash_results(vec![
            self.disown_orphaned(context).await,
            self.destroy_deleting(context).await,
            self.destroy_orphaned(context).await,
        ])
    }

    async fn destroy_deleting(&mut self, context: &PollContext) -> PollResult {
        destroy_deleting_replica(self, context).await
    }

    async fn destroy_orphaned(&mut self, context: &PollContext) -> PollResult {
        destroy_orphaned_replica(self, context).await
    }

    async fn disown_unused(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }

    async fn disown_orphaned(&mut self, context: &PollContext) -> PollResult {
        remove_missing_owners(self, context).await
    }
}

/// Remove replica owners who no longer exist.
/// In the event that the replicas become orphaned (have no owners) they will be destroyed by the
/// 'destroy_orphaned_replicas' reconcile loop.
async fn remove_missing_owners(
    replica: &mut OperationGuardArc<ReplicaSpec>,
    context: &PollContext,
) -> PollResult {
    let specs = context.specs();

    let owned = {
        let replica_spec = replica.lock();
        replica_spec.managed && replica_spec.owned()
    };

    if owned {
        let replica_spec = replica.lock().clone();
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
                // will run this reconcile loop again and tidy it up, so no need to retry here.
                tracing::error!(replica.uuid=%replica_clone.uuid, error=%error, "Failed to persist disowned replica")
            }
        }
    }

    PollResult::Ok(PollerState::Idle)
}

/// Destroy orphaned replicas.
/// Orphaned replicas are those that are managed but which don't have any owners.
async fn destroy_orphaned_replica(
    replica: &mut OperationGuardArc<ReplicaSpec>,
    context: &PollContext,
) -> PollResult {
    let destroy_owned = {
        let replica = replica.lock();
        replica.managed && !replica.owned()
    };

    if destroy_owned {
        destroy_replica(replica, context).await
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

/// Given a control plane replica
/// When its destruction fails
/// Then it should eventually be destroyed
async fn destroy_deleting_replica(
    replica: &mut OperationGuardArc<ReplicaSpec>,
    context: &PollContext,
) -> PollResult {
    let deleting = replica.lock().status().deleting();
    if deleting {
        destroy_replica(replica, context).await
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

#[tracing::instrument(level = "debug", skip(replica, context), fields(replica.uuid = %replica.lock().uuid, request.reconcile = true))]
async fn destroy_replica(
    replica: &mut OperationGuardArc<ReplicaSpec>,
    context: &PollContext,
) -> PollResult {
    let pool_id = replica.lock().pool.clone();
    if let Some(node) = ResourceSpecsLocked::get_pool_node(context.registry(), pool_id).await {
        let replica_spec = replica.lock().clone();
        match replica
            .destroy(
                context.registry(),
                &ResourceSpecsLocked::destroy_replica_request(
                    replica_spec.clone(),
                    ReplicaOwners::new_disown_all(),
                    &node,
                ),
            )
            .await
        {
            Ok(_) => {
                tracing::info!(replica.uuid=%replica_spec.uuid, "Successfully destroyed replica");
                PollResult::Ok(PollerState::Idle)
            }
            Err(e) => {
                tracing::trace!(replica.uuid=%replica_spec.uuid, error=%e, "Failed to destroy replica");
                PollResult::Err(e)
            }
        }
    } else {
        PollResult::Ok(PollerState::Busy)
    }
}
