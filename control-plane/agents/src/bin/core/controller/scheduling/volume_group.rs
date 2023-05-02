use crate::controller::{registry::Registry, resources::ResourceUid};
use std::collections::HashMap;
use stor_port::types::v0::{
    store::volume::{VolumeGroupSpec, VolumeSpec},
    transport::{NodeId, PoolId},
};

/// Get the nodes where the volume group volumes already have a replica.
/// This would be only applicable if the volume group volumes are of single replica.
pub(crate) fn get_restricted_nodes(
    volume_spec: &VolumeSpec,
    volume_group_spec: &VolumeGroupSpec,
    registry: &Registry,
) -> Vec<NodeId> {
    let mut restricted_nodes: Vec<NodeId> = Vec::new();
    if volume_spec.num_replicas == 1 {
        let specs = registry.specs();
        // Get the list of volumes being part of the volume group.
        let volume_group_volumes = volume_group_spec.volumes();
        // Remove the current volume from the list, as it is under creation.
        let volume_group_volumes: Vec<_> = volume_group_volumes
            .iter()
            .filter(|volume_id| *volume_id != volume_spec.uid())
            .collect();
        // List of restricted nodes, which already has replicas from the volumes of the
        // volume group.
        for volume in &volume_group_volumes {
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

/// Get the map of pool to the number of the volume group replica on the pool.
pub(crate) async fn get_pool_vg_replica_count(
    volume_group_spec: &VolumeGroupSpec,
    registry: &Registry,
) -> HashMap<PoolId, u64> {
    let mut pool_vg_replica_count = HashMap::new();
    let specs = registry.specs();
    for volume in volume_group_spec.volumes() {
        // Check if there exists a replica on the pool, that is a part of a volume group
        // volume.
        for replica in specs.volume_replicas(volume) {
            *pool_vg_replica_count
                .entry(replica.lock().pool_name().clone())
                .or_insert(0) += 1;
        }
    }
    pool_vg_replica_count
}
