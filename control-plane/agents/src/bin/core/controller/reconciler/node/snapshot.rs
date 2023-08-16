use crate::{
    controller::{
        io_engine::ReplicaSnapshotApi,
        reconciler::poller::ReconcilerWorker,
        resources::{OperationGuardArc, ResourceUid},
        task_poller::{PollContext, PollResult, PollTimer, PollerState, TaskPoller},
    },
    node::wrapper::GetterOps,
};
use agents::errors::SvcError;
use stor_port::types::v0::{
    store::node::NodeSpec,
    transport::{DestroyReplicaSnapshot, SnapshotId},
};

/// Node snapshot garbage collector.
#[derive(Debug)]
pub(super) struct NodeSnapshotGarbageCollector {
    counter: PollTimer,
}
impl NodeSnapshotGarbageCollector {
    /// Return a new `Self`.
    pub(super) fn new() -> Self {
        Self {
            counter: ReconcilerWorker::garbage_collection_period(),
        }
    }
}

#[async_trait::async_trait]
impl TaskPoller for NodeSnapshotGarbageCollector {
    async fn poll(&mut self, context: &PollContext) -> PollResult {
        let nodes = context.specs().nodes();
        let mut results = Vec::with_capacity(nodes.len());

        for node in nodes {
            let mut node_spec = context.registry().specs().guarded_node(node.id()).await?;
            results.push(delete_unknown_volume_snapshot_reconciler(context, &mut node_spec).await);
        }
        Self::squash_results(results)
    }

    async fn poll_timer(&mut self, _context: &PollContext) -> bool {
        self.counter.poll()
    }
}

/// Identify unknown snapshots and attempt to clean them up.
#[tracing::instrument(skip(node_spec, context), level = "trace", fields(request.reconcile = true))]
async fn delete_unknown_volume_snapshot_reconciler(
    context: &PollContext,
    node_spec: &mut OperationGuardArc<NodeSpec>,
) -> PollResult {
    let node_wrapper = context.registry().node_wrapper(node_spec.uid()).await?;

    let snapshots = node_wrapper.snapshots().await;
    for snap in snapshots.into_iter().filter(|s| !s.discarded()) {
        let Some((snap_uuid, txn_id)) = extract_uuid_txn_id(snap.snap_name()) else {
            continue;
        };
        // Keeps the guard to ensure nothing else is modifying the snapshot.
        let _guard = match context.registry().specs().volume_snapshot(&snap_uuid).await {
            Ok(snap_guard)
                if !snap_guard
                    .as_ref()
                    .metadata()
                    .transactions()
                    .contains_key(txn_id) =>
            {
                Some(snap_guard)
            }
            Err(SvcError::VolSnapshotNotFound { .. }) => None,
            Ok(_) | Err(_) => continue,
        };
        if node_wrapper
            .destroy_repl_snapshot(&DestroyReplicaSnapshot::new(
                snap.snap_uuid().clone(),
                snap.pool_uuid().clone(),
            ))
            .await
            .is_ok()
        {
            tracing::warn!(snapshot.uuid=%snap.snap_uuid(), snapshot.pool_uuid=%snap.pool_uuid(), "Deleted unknown replica snapshot from pool");
        }
    }

    Ok(PollerState::Idle)
}

/// Extracts the uuid and txn id from the snapshot name of format <snap_uuid>/<txn_id>.
fn extract_uuid_txn_id(input: &str) -> Option<(SnapshotId, &str)> {
    let parts: Vec<&str> = input.split('/').collect();
    if parts.len() != 2 {
        return None;
    }

    SnapshotId::try_from(parts[0])
        .ok()
        .map(|snap_uuid| (snap_uuid, parts[1]))
}
