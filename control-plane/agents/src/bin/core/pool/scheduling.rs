use crate::controller::{
    registry::Registry,
    scheduling::{
        pool::{ENoSpcPool, NexusChildrenENoSpcPools},
        ResourceFilter,
    },
};

/// Get all the pools that have seen replicas hit with ENoSpcPool and their respective replicas
/// whose "NexusChild" have failed due to ENOSPC error.
/// They are currently "unfiltered" as we're still not sure what the best way to pick replicas
/// to move is.
pub(crate) async fn unfiltered_enospc_pools(registry: &Registry) -> Vec<ENoSpcPool> {
    let candidates: Vec<ENoSpcPool> = NexusChildrenENoSpcPools::builder_with_defaults(registry)
        .await
        .collect();
    candidates
}
