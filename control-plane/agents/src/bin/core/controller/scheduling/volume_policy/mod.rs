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

/// Return true if all the keys present in volume's pool/node inclusion matches with the pool/node
/// labels otherwise returns false.
pub(crate) fn does_pool_qualify_inclusion_labels(
    vol_inc_labels: HashMap<String, String>,
    resource_labels: HashMap<String, String>,
) -> bool {
    for (vol_inc_key, vol_inc_value) in vol_inc_labels.iter() {
        match resource_labels.get(vol_inc_key) {
            Some(pool_val) => {
                if vol_inc_value.is_empty() {
                    continue;
                }
                if pool_val != vol_inc_value {
                    return false;
                }
            }
            None => {
                return false;
            }
        }
    }
    true
}

/// Return true if all the key present in volume's pool/node exclusion matches with the pool/node
/// labels and their corresponding value are different otherwise returns false
pub(crate) fn does_pool_qualify_exclusion_labels(
    vol_exc_labels: HashMap<String, String>,
    resource_labels: HashMap<String, String>,
) -> bool {
    // If volume's pool exclusion labels is empty, then it means that the volume has no exclusion
    if vol_exc_labels.is_empty() {
        return true;
    }

    // exclusion_key_only are set of keys from vol_exc_labels has only keys with empty values
    // e.g. vol_exc_labels = {"key1": "", "key2": "value2"}
    let exclusion_key_only: Vec<_> = vol_exc_labels
        .iter()
        .filter(|(_, value)| value.is_empty())
        .map(|(key, _)| key)
        .collect();

    if exclusion_key_only.is_empty() {
        // If volume's pool exclusion labels has any key with non-empty value, then it means that
        // the pool should not have that key with that value
        let common_keys: Vec<_> = vol_exc_labels
            .keys()
            .filter(|&key| resource_labels.contains_key(key))
            .collect();

        if common_keys.is_empty() {
            return false;
        }

        return common_keys
            .iter()
            .any(|&key| vol_exc_labels.get(key) != resource_labels.get(key));
    } else {
        // If volume's pool exclusion labels has any key with empty value, then it means that the
        // pool should not have that key
        !exclusion_key_only
            .iter()
            .any(|key| resource_labels.contains_key(&key.to_string()))
    }
}
