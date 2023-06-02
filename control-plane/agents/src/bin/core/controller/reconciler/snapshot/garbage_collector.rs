use crate::{
    controller::{
        reconciler::{poller::ReconcilerWorker, GarbageCollect},
        resources::{
            operations::ResourceLifecycleWithLifetime,
            operations_helper::{OperationSequenceGuard, SpecOperationsHelper},
            OperationGuardArc,
        },
        task_poller::{PollContext, PollResult, PollTimer, PollerState, TaskPoller},
    },
    volume::DestroyVolumeSnapshotRequest,
};
use stor_port::types::v0::store::snapshots::volume::VolumeSnapshot;

/// VolumeSnapshot Garbage Collector reconciler.
#[derive(Debug)]
pub(super) struct GarbageCollector {
    counter: PollTimer,
}
impl GarbageCollector {
    /// Return a new `Self`.
    pub(super) fn new() -> Self {
        Self {
            counter: ReconcilerWorker::garbage_collection_period(),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for GarbageCollector {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let snaps = context.specs().volume_snapshots_rsc();
        let mut results = Vec::with_capacity(snaps.len());
        for snap in snaps {
            if snap.lock().dirty() {
                continue;
            }
            let mut snap_guard = match snap.operation_guard() {
                Ok(guard) => guard,
                Err(_) => continue,
            };

            results.push(Self::squash_results(vec![
                snap_guard.garbage_collect(context).await,
            ]))
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}

#[async_trait::async_trait]
impl GarbageCollect for OperationGuardArc<VolumeSnapshot> {
    async fn garbage_collect(&mut self, context: &PollContext) -> PollResult {
        self.destroy_deleting(context).await
    }

    async fn destroy_deleting(&mut self, context: &PollContext) -> PollResult {
        deleting_volume_snapshot_reconciler(self, context).await
    }

    // Unimplemented garbage collectors
    async fn destroy_orphaned(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }
    async fn disown_unused(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }
    async fn disown_orphaned(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }
    async fn disown_invalid(&mut self, _context: &PollContext) -> PollResult {
        unimplemented!()
    }
}

#[tracing::instrument(skip(snapshot, context), level = "trace", fields(snapshot.id = %snapshot.uuid(), request.reconcile = true))]
async fn deleting_volume_snapshot_reconciler(
    snapshot: &mut OperationGuardArc<VolumeSnapshot>,
    context: &PollContext,
) -> PollResult {
    if !snapshot.as_ref().status().deleting() {
        return Ok(PollerState::Idle);
    }

    let Some(snap_rsc) = context.specs().volume_snapshot_rsc(snapshot.uuid()) else {
        return Ok(PollerState::Idle);
    };

    let snap_user_spec = snapshot.lock().spec().clone();
    match snapshot
        .destroy(
            context.registry(),
            &DestroyVolumeSnapshotRequest::new(snap_rsc, snap_user_spec),
        )
        .await
    {
        Ok(_) => {
            tracing::info!(
                snapshot.uuid = %snapshot.uuid(),
                "VolumeSnapshot deleted successfully"
            );
            Ok(PollerState::Idle)
        }
        Err(error) => {
            tracing::error!(
                snapshot.uuid = %snapshot.uuid(),
                "Failed to delete volumeSnapshot"
            );
            Err(error)
        }
    }
}
