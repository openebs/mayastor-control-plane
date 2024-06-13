use super::{volume::ResizeVolumeReplicas, ReplicaFilters, ResourceFilter};
use crate::controller::scheduling::volume::{
    AddVolumeReplica, CloneVolumeSnapshot, SnapshotVolumeReplica,
};
use std::collections::HashMap;

mod affinity_group;
pub(crate) mod node;
pub(crate) mod pool;
mod simple;
mod thick;

pub(super) use simple::SimplePolicy;
pub(super) use thick::ThickPolicy;

struct DefaultBasePolicy {}
impl DefaultBasePolicy {
    fn filter(request: AddVolumeReplica) -> AddVolumeReplica {
        Self::filter_pools(Self::filter_nodes(request))
    }
    fn filter_nodes(request: AddVolumeReplica) -> AddVolumeReplica {
        request
            .filter(node::NodeFilters::cordoned_for_pool)
            .filter(node::NodeFilters::online_for_pool)
            .filter(node::NodeFilters::allowed)
            .filter(node::NodeFilters::unused)
            .filter(node::NodeFilters::topology)
    }
    fn filter_pools(request: AddVolumeReplica) -> AddVolumeReplica {
        request
            .filter(pool::PoolBaseFilters::usable)
            .filter(pool::PoolBaseFilters::capacity)
            .filter(pool::PoolBaseFilters::min_free_space)
            .filter(pool::PoolBaseFilters::topology)
    }
    fn filter_snapshot(request: SnapshotVolumeReplica) -> SnapshotVolumeReplica {
        Self::filter_snapshot_pools(Self::filter_snapshot_nodes(request))
    }
    fn filter_snapshot_nodes(request: SnapshotVolumeReplica) -> SnapshotVolumeReplica {
        request
            .filter(node::NodeFilters::cordoned_for_pool)
            .filter(node::NodeFilters::online_for_pool)
    }
    fn filter_snapshot_pools(request: SnapshotVolumeReplica) -> SnapshotVolumeReplica {
        request
            .filter(pool::PoolBaseFilters::usable)
            .filter(pool::PoolBaseFilters::capacity)
            .filter(pool::PoolBaseFilters::min_free_space)
    }
    fn filter_clone(request: CloneVolumeSnapshot) -> CloneVolumeSnapshot {
        Self::filter_clone_pools(Self::filter_clone_nodes(request))
    }
    fn filter_clone_nodes(request: CloneVolumeSnapshot) -> CloneVolumeSnapshot {
        request
            .filter(node::NodeFilters::cordoned_for_pool)
            .filter(node::NodeFilters::online_for_pool)
    }
    fn filter_clone_pools(request: CloneVolumeSnapshot) -> CloneVolumeSnapshot {
        request
            .filter(pool::PoolBaseFilters::usable)
            .filter(pool::PoolBaseFilters::capacity)
            .filter(pool::PoolBaseFilters::min_free_space)
    }
    fn filter_resize(request: ResizeVolumeReplicas) -> ResizeVolumeReplicas {
        request
            .filter(ReplicaFilters::online_for_resize)
            .filter(pool::PoolBaseFilters::min_free_space_repl_resize)
    }
}

/// Return true if all the keys present in the volume's pool/node
/// inclusion/affinity match the pool/node labels; otherwise, return false.
pub(crate) fn qualifies_label_criteria(
    vol_pool_inc_labels: HashMap<String, String>,
    vol_pool_affinity: HashMap<String, String>,
    pool_labels: &HashMap<String, String>,
) -> bool {
    let mut satisfy_inclusion = true;
    let mut satisfy_affinity = true;
    for (vol_inc_key, vol_inc_value) in vol_pool_inc_labels.iter() {
        match pool_labels.get(vol_inc_key) {
            Some(pool_val) => {
                if vol_inc_value.is_empty() {
                    continue;
                }
                if pool_val != vol_inc_value {
                    satisfy_inclusion = false;
                    break;
                }
            }
            None => {
                satisfy_inclusion = false;
                break;
            }
        }
    }

    for (vol_affinity_key, _) in vol_pool_affinity.iter() {
        match pool_labels.get(vol_affinity_key) {
            Some(_) => {
                continue;
            }
            None => {
                satisfy_affinity = false;
                break;
            }
        }
    }
    satisfy_inclusion && satisfy_affinity
}
