use crate::controller::{registry::Registry, resources::ResourceUid};
use std::collections::HashMap;
use stor_port::types::v0::{
    store::volume::{AffinityGroupSpec, VolumeSpec},
    transport::{NodeId, PoolId},
};

/// Get the nodes where the Affinity Group volumes already have a replica.
/// This would be only applicable if the Affinity Group volumes are of single replica.
pub(crate) fn get_restricted_nodes(
    volume_spec: &VolumeSpec,
    affinity_group_spec: &AffinityGroupSpec,
    registry: &Registry,
) -> Vec<NodeId> {
    let mut restricted_nodes: Vec<NodeId> = Vec::new();
    if volume_spec.num_replicas == 1 {
        let specs = registry.specs();
        // Get the list of volumes being part of the Affinity Group.
        let affinity_group_volumes = affinity_group_spec.volumes();
        // Remove the current volume from the list, as it is under creation.
        let affinity_group_volumes: Vec<_> = affinity_group_volumes
            .iter()
            .filter(|volume_id| *volume_id != volume_spec.uid())
            .collect();
        // List of restricted nodes, which already has replicas from the volumes of the
        // Affinity Group.
        for volume in &affinity_group_volumes {
            // Fetch the list of nodes where the volume replicas are placed. If some
            // volume doesn't exist yet, this list will be empty
            // for that volume.
            let node_ids = specs.get_volume_replica_nodes(registry, volume);
            // Add a new node in the list if it doesn't already exist.
            let filtered_nodes: Vec<NodeId> = node_ids
                .into_iter()
                .filter(|id| !restricted_nodes.contains(id))
                .collect();
            restricted_nodes.extend(filtered_nodes);
        }
    }
    restricted_nodes
}

/// Get the map of pool to the number of the Affinity Group replica on the pool.
pub(crate) async fn get_pool_ag_replica_count(
    affinity_group_spec: &AffinityGroupSpec,
    registry: &Registry,
) -> HashMap<PoolId, u64> {
    let mut pool_ag_replica_count = HashMap::new();
    let specs = registry.specs();
    for volume in affinity_group_spec.volumes() {
        // Check if there exists a replica on the pool, that is a part of a Affinity Group
        // volume.
        for replica in specs.volume_replicas(volume) {
            *pool_ag_replica_count
                .entry(replica.lock().pool_name().clone())
                .or_insert(0) += 1;
        }
    }
    pool_ag_replica_count
}

/// Get the map of node to the number of the Affinity Group nexuses on the node.
pub(crate) async fn get_node_ag_nexus_count(
    affinity_group_spec: &AffinityGroupSpec,
    registry: &Registry,
) -> HashMap<NodeId, u64> {
    let ag_vols = affinity_group_spec.volumes();
    let node_ag_nexus_count = ag_vols
        .iter()
        .flat_map(|vol_id| registry.specs().volume_nexuses(vol_id))
        .map(|nexus| nexus.lock().node.clone())
        .fold(HashMap::<NodeId, u64>::new(), |mut map, node| {
            map.entry(node).and_modify(|count| *count += 1).or_insert(1);
            map
        });
    node_ag_nexus_count
}
