use crate::controller::{
    registry::Registry,
    resources::TraceStrLog,
    scheduling::{
        nexus,
        nexus::{GetPersistedNexusChildren, GetSuitableNodes},
        resources::HealthyChildItems,
        ResourceFilter,
    },
    wrapper::NodeWrapper,
};
use agents::errors::{NotEnough, SvcError};
use common_lib::types::v0::store::nexus::NexusSpec;

/// Return healthy replicas for volume/nexus
/// The persistent store has the latest information from io-engine, which tells us if any replica
/// has been faulted and therefore cannot be used by the nexus.
async fn get_healthy_children(
    request: &GetPersistedNexusChildren,
    registry: &Registry,
) -> Result<HealthyChildItems, SvcError> {
    let builder = nexus::CreateVolumeNexus::builder_with_defaults(request, registry).await?;
    let info = builder.context().nexus_info().clone();
    if let Some(info_inner) = &builder.context().nexus_info() {
        if !info_inner.clean_shutdown {
            return Ok(HealthyChildItems::One(info, builder.collect()));
        }
    }
    let items = builder.collect();
    Ok(HealthyChildItems::All(info, items))
}

/// Get all usable healthy child replicas for nexus recreation
/// (only children which are ReplicaSpec's are returned).
/// The persistent store has the latest information from io-engine, which tells us if any replica
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

/// Return the suitable node target to publish the volume for nexus placement on
/// volume publish or nexus failover.
pub(crate) async fn get_target_node_candidate(
    request: impl Into<GetSuitableNodes>,
    registry: &Registry,
) -> Result<NodeWrapper, SvcError> {
    let candidates: Vec<NodeWrapper> =
        nexus::NexusTargetNode::builder_with_defaults(request, registry)
            .await
            .collect()
            .into_iter()
            .map(|i| i.node_wrapper())
            .collect();
    match candidates.first() {
        None => Err(SvcError::NotEnoughResources {
            source: NotEnough::OfNodes { have: 0, need: 1 },
        }),
        Some(node) => Ok(node.clone()),
    }
}
