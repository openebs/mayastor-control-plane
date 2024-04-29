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
use std::collections::HashMap;
use stor_port::types::v0::{
    store::{nexus::NexusSpec, volume::VolumeSpec},
    transport::{NodeId, NodeTopology, PoolTopology, Replica, VolumeState},
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

/// Return a list of pre sorted pools in case of affinity.
pub(crate) fn second_pass_filter(
    pools: Vec<PoolWrapper>,
    suitable_pools: GetSuitablePools,
    registry: &Registry,
) -> Vec<PoolWrapper> {
    let mut map: HashMap<String, Vec<PoolWrapper>> = HashMap::new();
    for pool in pools.iter() {
        let id = pool.state().id.clone();
        let spec = registry.specs().pool(&id).unwrap_or_default();
        let pool_labels = spec.labels.clone().unwrap_or_default();
        let pool_affinity = match &suitable_pools.spec().topology.as_ref().unwrap().pool {
            Some(pool_topology) => match pool_topology {
                PoolTopology::Labelled(label) => label.affinity.clone(),
            },
            None => HashMap::new(),
        };

        // performance: gold and zone: us-west , then the map key is "performancegoldzoneus-west"
        // and the vallues are the vec of pool wrappers that have these labels
        for (pool_affinity_key, _) in pool_affinity.iter() {
            let mut result = String::new();
            if let Some(value) = pool_labels.get(pool_affinity_key) {
                result.push_str(pool_affinity_key);
                result.push_str(value);
            }
            if let Some(pools) = map.get_mut(&result) {
                pools.push(pool.clone());
            } else {
                map.insert(result, vec![pool.clone()]);
            }
        }
    }

    // Initialize variables to keep track of the key with the maximum size of values
    let mut max_key = String::new();
    let mut max_size = 0;

    for (key, list_of_pools) in map.iter() {
        if list_of_pools.len() >= suitable_pools.spec().desired_num_replicas() as usize
            && list_of_pools.len() > max_size
        {
            max_key = key.clone(); // Update the key with the maximum size
            max_size = list_of_pools.len(); // Update the maximum size
        }
    }

    let final_pools = match map.get(&max_key) {
        Some(pools) => pools.clone(),
        None => Vec::new(),
    };
    final_pools
}

pub(crate) fn is_affinity_requested(suitable_pools: GetSuitablePools) -> bool {
    match suitable_pools.spec().topology.as_ref() {
        Some(topology) => {
            let node_affinity = match &topology.node {
                Some(node_topology) => match node_topology {
                    NodeTopology::Labelled(label) => {
                        let mut matching_key = false;
                        for key in label.affinity.keys() {
                            if label.inclusion.contains_key(key) {
                                matching_key = true;
                            }
                        }
                        !matching_key && !label.affinity.is_empty()
                    }

                    NodeTopology::Explicit(_) => false,
                },
                None => false,
            };
            let pool_affinity = match &topology.pool {
                Some(pool_topology) => match pool_topology {
                    PoolTopology::Labelled(label) => {
                        let mut matching_key = false;
                        for key in label.affinity.keys() {
                            if label.inclusion.contains_key(key) {
                                matching_key = true;
                            }
                        }
                        !matching_key && !label.affinity.is_empty()
                    }
                },
                None => false,
            };
            node_affinity || pool_affinity
        }
        None => false,
    }
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
