use crate::controller::{registry::Registry, resources::ResourceUid};
use std::collections::HashMap;
use stor_port::types::v0::{
    store::volume::{AffinityGroupSpec, VolumeOperation, VolumeSpec},
    transport::{NodeId, PoolId},
};

/// Get the nodes where the Affinity Group volumes already have a replica.
/// This would be only applicable if the Affinity Group volumes are of single replica.
pub(crate) fn get_restricted_nodes(
    volume_spec: &VolumeSpec,
    affinity_group_spec: &AffinityGroupSpec,
    registry: &Registry,
) -> Vec<NodeId> {
    // Since num_replicas in VolumeSpec is equal to 1 when scaling up from 1 replica to 2 replicas
    // for the volume of an affinity group, if the number of nodes in cluster is equal to number of
    // volumes in the affinity group we will exhaust nodes for scale up. This early exit relaxes the
    // restriction for the first scale up (only support 1 replica add at a time).
    if matches!(
        volume_spec
            .operation
            .as_ref()
            .map(|op| op.operation.clone()),
        Some(VolumeOperation::SetReplica(2))
    ) || volume_spec.num_replicas != 1
    {
        return Vec::new();
    }

    let specs = registry.specs();
    // List of restricted nodes, which already host replicas from the volumes of the affinity group
    affinity_group_spec
        .volumes()
        .iter()
        .filter(|&vid| vid != volume_spec.uid())
        .flat_map(|vid| specs.volume_replica_nodes(vid))
        .fold(Vec::new(), |mut acc, node_id| {
            if !acc.contains(&node_id) {
                acc.push(node_id);
            }
            acc
        })
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
