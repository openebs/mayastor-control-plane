use crate::controller::{
    reconciler::{GarbageCollect, ReCreate},
    resources::{
        operations::{ResourceLifecycle, ResourceOwnerUpdate},
        operations_helper::{OperationSequenceGuard, ResourceSpecsLocked, SpecOperationsHelper},
        OperationGuardArc,
    },
    task_poller::{
        PollContext, PollEvent, PollResult, PollTimer, PollTriggerEvent, PollerState, TaskPoller,
    },
};
use common_lib::types::v0::{store::replica::ReplicaSpec, transport::ReplicaOwners};

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
        let replicas = context.specs().replicas();
        let mut results = Vec::with_capacity(replicas.len());

        for replica in replicas {
            if replica.lock().dirty() {
                continue;
            }
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

    async fn disown_invalid(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
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
    let owned = replica.as_ref().managed && replica.as_ref().owned();

    if owned {
        let replica_uuid = &replica.immutable_arc().uuid;
        let mut remove_owners = ReplicaOwners::default();
        if let Some(volume) = replica.as_ref().owners.volume() {
            if specs.volume_clone(volume).is_err() {
                // The volume no longer exists. Remove it as an owner.
                remove_owners.add_volume(volume.clone());
                tracing::info!(replica.uuid=%replica_uuid, volume.uuid=%volume, "Removing volume as replica owner");
            }
        }

        replica.as_ref().owners.nexuses().iter().for_each(|nexus| {
            if specs.nexus_rsc(nexus).is_none() {
                remove_owners.add_owner(nexus);
                tracing::info!(replica.uuid=%replica_uuid, nexus.uuid=%nexus, "Removing nexus as replica owner");
            }
        });

        if remove_owners.is_owned() {
            if let Err(error) = replica
                .remove_owners(context.registry(), &remove_owners, true)
                .await
            {
                tracing::error!(replica.uuid=%replica_uuid, error=%error, "Failed to persist disowned replica");
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
        let replica = replica.as_ref();
        replica.managed && !replica.owned() && !replica.status().deleted()
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
    let deleting = replica.as_ref().status().deleting();
    if deleting {
        destroy_replica(replica, context).await
    } else {
        PollResult::Ok(PollerState::Idle)
    }
}

#[tracing::instrument(level = "debug", skip(replica, context), fields(replica.uuid = %replica.uuid(), request.reconcile = true))]
async fn destroy_replica(
    replica: &mut OperationGuardArc<ReplicaSpec>,
    context: &PollContext,
) -> PollResult {
    let pool_ref = replica.as_ref().pool.pool_name();
    if let Some(node) = ResourceSpecsLocked::pool_node(context.registry(), pool_ref).await {
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
            Err(error) => {
                tracing::trace!(replica.uuid=%replica_spec.uuid, %error, "Failed to destroy replica");
                PollResult::Err(error)
            }
        }
    } else {
        PollResult::Ok(PollerState::Busy)
    }
}
