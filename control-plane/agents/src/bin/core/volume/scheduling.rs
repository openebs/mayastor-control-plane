use crate::{
    controller::{
        registry::Registry,
        scheduling::{
            nexus,
            nexus::{GetPersistedNexusChildren, GetSuitableNodes},
            resources::HealthyChildItems,
            volume,
            volume::{GetChildForRemoval, GetSuitablePools},
            ResourceFilter,
        },
        wrapper::PoolWrapper,
    },
    nexus::scheduling::target_node_candidates,
    node::wrapper::NodeWrapper,
};
use agents::errors::{NotEnough, SvcError};
use stor_port::types::v0::{
    store::{nexus::NexusSpec, volume::VolumeSpec},
    transport::{NodeId, Replica, VolumeState},
};

/// Return a list of pre sorted pools to be used by a volume.
pub(crate) async fn volume_pool_candidates(
    request: impl Into<GetSuitablePools>,
    registry: &Registry,
) -> Vec<PoolWrapper> {
    volume::AddVolumeReplica::builder_with_defaults(request.into(), registry)
        .await
        .collect()
        .into_iter()
        .map(|e| e.collect())
        .collect()
}

/// Return a volume child candidate to be removed from a volume.
/// This list includes healthy and non_healthy candidates, so care must be taken to
/// make sure we don't remove "too many healthy" candidates and make the volume degraded.
pub(crate) async fn volume_replica_remove_candidates(
    request: &GetChildForRemoval,
    registry: &Registry,
) -> Result<volume::DecreaseVolumeReplica, SvcError> {
    volume::DecreaseVolumeReplica::builder_with_defaults(request, registry).await
}

/// Return a nexus child candidate to be removed from a nexus.
pub(crate) async fn nexus_child_remove_candidates(
    vol_spec: &VolumeSpec,
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<volume::DecreaseVolumeReplica, SvcError> {
    let volume_state = registry.volume_state(&vol_spec.uuid).await?;
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

/// Return healthy replicas for volume nexus creation.
pub(crate) async fn healthy_volume_replicas(
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

/// Return healthy replicas for volume snapshotting.
/// todo: need to refactor to filter with dedicated scheduler.
pub(crate) async fn snapshoteable_replica(
    volume: &VolumeSpec,
    registry: &Registry,
) -> Result<HealthyChildItems, SvcError> {
    let published = volume.target().is_some();

    let request = GetPersistedNexusChildren::new_snapshot(volume);
    let builder = nexus::CreateVolumeNexus::builder_with_defaults(&request, registry).await?;
    let info = builder.context().nexus_info().clone();

    if let Some(info_inner) = &builder.context().nexus_info() {
        // if it's published then we of course we don't have a clean shutdown if the target
        // is still up..
        if !published && !info_inner.clean_shutdown {
            return Ok(HealthyChildItems::One(info, builder.collect()));
        }
    }
    let items = builder.collect();
    Ok(HealthyChildItems::All(info, items))
}

/// Return the suitable node target to publish the volume for nexus placement on volume publish.
pub(crate) async fn target_node_candidate(
    request: impl Into<GetSuitableNodes>,
    registry: &Registry,
    state: &VolumeState,
    preferred_node: &Option<NodeId>,
) -> Result<NodeWrapper, SvcError> {
    let request = request.into();
    let replicas = request.num_replicas;
    let candidates = target_node_candidates(request, registry, preferred_node).await;
    if replicas == 1 {
        // For 1replica volumes, pin the volume target to the replica node.
        if let Some(Some(node)) = state.replica_topology.values().last().map(|r| r.node()) {
            if let Some(node) = candidates.iter().find(|n| n.id() == node) {
                return Ok(node.clone());
            }
        }
    }
    match candidates.first() {
        None => Err(SvcError::NotEnoughResources {
            source: NotEnough::OfNodes { have: 0, need: 1 },
        }),
        Some(node) => Ok(node.clone()),
    }
}

/// List, filter and collect replicas for which are Online and the pools have
/// the required capacity for resizing the replica.
pub(crate) async fn resizeable_replicas(
    volume: &VolumeSpec,
    registry: &Registry,
    req_capacity: u64,
) -> Vec<Replica> {
    let builder =
        volume::ResizeVolumeReplicas::builder_with_defaults(registry, volume, req_capacity).await;

    builder
        .collect()
        .into_iter()
        .map(|item| item.state().clone())
        .collect()
}
