use crate::core::{
    registry::Registry,
    scheduling::{
        nexus, nexus::GetPersistedNexusChildren, resources::HealthyChildItems, ResourceFilter,
    },
};
use common::errors::SvcError;
use common_lib::types::v0::store::{nexus::NexusSpec, TraceStrLog};

/// Return healthy replicas for volume/nexus
/// The persistent store has the latest information from mayastor, which tells us if any replica
/// has been faulted and therefore cannot be used by the nexus.
async fn get_healthy_children(
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

/// Get all usable healthy child replicas for nexus recreation
/// (only children which are ReplicaSpec's are returned).
/// The persistent store has the latest information from mayastor, which tells us if any replica
/// has been faulted and therefore cannot be used by the nexus.
pub(crate) async fn get_healthy_nexus_children(
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<HealthyChildItems, SvcError> {
    let children = get_healthy_children(
        &GetPersistedNexusChildren::new_recreate(nexus_spec),
        registry,
    )
    .await?;

    nexus_spec.trace(&format!("Healthy nexus replicas: {:?}", children));

    Ok(children)
}
