use crate::core::{
    registry::Registry,
    scheduling::{
        nexus,
        nexus::GetPersistedNexusChildren,
        resources::{HealthyChildItems, ReplicaItem},
        volume,
        volume::{GetChildForRemoval, GetSuitablePools},
        ResourceFilter,
    },
    wrapper::PoolWrapper,
};
use common::errors::SvcError;

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

/// Return healthy replicas for volume nexus creation
pub(crate) async fn get_healthy_volume_replicas(
    request: &GetPersistedNexusChildren,
    registry: &Registry,
) -> Result<HealthyChildItems, SvcError> {
    let builder = nexus::CreateVolumeNexus::builder_with_defaults(request, registry).await?;

    if let Some(info) = &builder.context().nexus_info() {
        if !info.clean_shutdown {
            let items = builder.collect();
            return Ok(HealthyChildItems::One(items));
        }
    }
    let items = builder.collect();
    Ok(HealthyChildItems::All(items))
}
