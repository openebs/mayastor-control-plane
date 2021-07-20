use crate::core::{
    registry::Registry,
    scheduling::{
        resources::ReplicaItem,
        volume,
        volume::{GetChildForRemoval, GetSuitablePools},
        ResourceFilter,
    },
    wrapper::PoolWrapper,
};

/// Return a list of pre sorted pools to be used by a volume
pub(crate) async fn get_volume_pool_candidates(
    request: impl Into<GetSuitablePools>,
    registry: &Registry,
) -> Vec<PoolWrapper> {
    volume::IncreaseVolumeReplica::builder_with_defaults(request, registry)
        .await
        .collect()
        .into_iter()
        .map(|e| e.collect())
        .collect()
}

/// Return a nexus child candidate to be removed from a nexus
pub(crate) async fn get_nexus_child_remove_candidate(
    request: &GetChildForRemoval,
    registry: &Registry,
) -> Vec<ReplicaItem> {
    volume::DecreaseVolumeReplica::builder_with_defaults(request, registry)
        .await
        .collect()
}
