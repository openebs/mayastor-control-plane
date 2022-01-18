use crate::core::{
    registry::Registry,
    scheduling::{
        nexus,
        nexus::GetPersistedNexusChildren,
        resources::HealthyChildItems,
        volume,
        volume::{GetChildForRemoval, GetSuitablePools},
        ResourceFilter,
    },
    wrapper::PoolWrapper,
};
use common::errors::SvcError;
use common_lib::types::v0::store::{nexus::NexusSpec, volume::VolumeSpec};

/// Return a list of pre sorted pools to be used by a volume
pub(crate) async fn get_volume_pool_candidates(
    request: impl Into<GetSuitablePools>,
    registry: &Registry,
) -> Vec<PoolWrapper> {
    volume::AddVolumeReplica::builder_with_defaults(request, registry)
        .await
        .collect()
        .into_iter()
        .map(|e| e.collect())
        .collect()
}

/// Return a volume child candidate to be removed from a volume
/// This list includes healthy and non_healthy candidates, so care must be taken to
/// make sure we don't remove "too many healthy" candidates and make the volume degraded
pub(crate) async fn get_volume_replica_remove_candidates(
    request: &GetChildForRemoval,
    registry: &Registry,
) -> Result<volume::DecreaseVolumeReplica, SvcError> {
    Ok(volume::DecreaseVolumeReplica::builder_with_defaults(request, registry).await?)
}

/// Return a nexus child candidate to be removed from a nexus
pub(crate) async fn get_nexus_child_remove_candidates(
    vol_spec: &VolumeSpec,
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<volume::DecreaseVolumeReplica, SvcError> {
    let volume_state = registry.get_volume_state(&vol_spec.uuid).await?;
    let request = GetChildForRemoval::new(vol_spec, &volume_state, false);
    Ok(
        volume::DecreaseVolumeReplica::builder_with_defaults(&request, registry)
            .await?
            // filter for children which belong to the nexus we're removing children from
            .filter(|_, i| match i.child_spec() {
                None => false,
                Some(child) => nexus_spec.children.contains(child),
            }),
    )
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
