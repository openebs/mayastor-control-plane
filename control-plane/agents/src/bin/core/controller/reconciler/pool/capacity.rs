use crate::{
    controller::{
        registry::Registry,
        resources::{operations_helper::OperationSequenceGuard, ResourceMutex},
        scheduling::pool::ENoSpcReplica,
        wrapper::{GetterOps, PoolWrapper},
    },
    pool::scheduling::unfiltered_enospc_pools,
};
use agents::errors::{PoolNotFound, SvcError};
use snafu::OptionExt;
use stor_port::types::v0::store::{pool::PoolSpec, replica::ReplicaSpec};

/// When a pool is exhausted multiple thin-provisioned replicas may fail with ENOSPC.
/// In this situation we'll currently adopt the simplistic strategy of faulting the largest replica
/// on the pool, or more specifically the replica with largest actual allocation.
/// A pre-condition for the replica faulting is that the volume to which the replica belongs to
/// should retain "enough" remaining healthy replicas! For example, we can't fault the last replica
/// of a volume as we can't rebuild from "thin" air.
pub(crate) async fn remove_larger_replicas(registry: &Registry) {
    let pools = unfiltered_enospc_pools(registry).await;

    if !pools.is_empty() {
        tracing::warn!("Found {} pools with ENOSPACE replicas", pools.len());
    }

    for pool in pools {
        let (pool, replicas) = pool.into_parts();
        if let Ok(pool_wrapper) = node_pool_wrapper(&pool, registry).await {
            let largest_replica = pool_wrapper
                .move_replicas()
                .into_iter()
                .filter_map(|r| {
                    replicas
                        .iter()
                        .find(|rs| rs.replica().uuid == r.uuid)
                        .map(|rs| (r, rs))
                })
                .max_by(|(a, _), (b, _)| match (&a.space, &b.space) {
                    (Some(space_a), Some(space_b)) => {
                        space_a.allocated_bytes.cmp(&space_b.allocated_bytes)
                    }
                    // If we're running a thin volume on io-engine v1, this should not happen.
                    _ => std::cmp::Ordering::Equal,
                })
                .and_then(|(r, rs)| registry.specs().replica_rsc(&r.uuid).map(|r| (r, rs)));

            // If the node is online/flaky we might not be able to get the current replica
            // information, and in this case there's not much we can do.
            if let Some(largest_replica) = largest_replica {
                let _ = remove_larger_replica(largest_replica, registry).await;
            }
        }
    }
}

async fn remove_larger_replica(
    (replica, eno_replica): (ResourceMutex<ReplicaSpec>, &ENoSpcReplica),
    registry: &Registry,
) -> Result<bool, SvcError> {
    let volume_owner = replica.lock().owners.volume().cloned();
    match volume_owner {
        Some(volume) => {
            let mut volume = registry.specs().volume(&volume).await?;
            volume.remove_replica(eno_replica, registry).await?;
            Ok(true)
        }
        None => {
            // The replica is not part of a volume, and not managed by us, NMP.
            if !replica.lock().managed {
                return Ok(false);
            }

            let nexus = eno_replica.nexus().operation_guard()?;
            let mut replica = replica.operation_guard()?;
            replica.fault(nexus, eno_replica, registry).await?;
            Ok(true)
        }
    }
}

async fn node_pool_wrapper(pool: &PoolSpec, registry: &Registry) -> Result<PoolWrapper, SvcError> {
    let node = registry.node_wrapper(&pool.node).await?;
    let pool_wrapper = node.pool_wrapper(&pool.id).await.context(PoolNotFound {
        pool_id: pool.id.clone(),
    })?;
    Ok(pool_wrapper)
}
