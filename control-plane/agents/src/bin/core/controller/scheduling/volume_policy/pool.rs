use crate::controller::scheduling::{
    resources::{ChildItem, PoolItem},
    volume::{GetSuitablePoolsContext, ReplicaResizePoolsContext},
};
use std::collections::HashMap;
use stor_port::types::v0::transport::{PoolStatus, PoolTopology};

/// Filter pools used for replica creation.
pub(crate) struct PoolBaseFilters {}
impl PoolBaseFilters {
    /// The minimum free space in a pool for it to be eligible for thin provisioned replicas.
    fn free_space_watermark() -> u64 {
        16 * 1024 * 1024
    }
    /// Should only attempt to use pools with capacity bigger than the requested replica size.
    pub(crate) fn capacity(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        item.pool.capacity > request.size
    }
    /// Should only attempt to use pools with capacity bigger than the requested replica size.
    pub(crate) fn overcommit(
        request: &GetSuitablePoolsContext,
        item: &PoolItem,
        allowed_commit_percent: u64,
    ) -> bool {
        match request.as_thin() {
            true => request.overcommit(allowed_commit_percent, item.pool()),
            false => true,
        }
    }
    /// Should only attempt to use pools with capacity bigger than the requested size
    /// for replica expand.
    pub(crate) fn overcommit_repl_resize(
        request: &ReplicaResizePoolsContext,
        item: &ChildItem,
        allowed_commit_percent: u64,
    ) -> bool {
        match request.spec().as_thin() {
            true => request.overcommit(allowed_commit_percent, item.pool()),
            false => true,
        }
    }
    /// Should only attempt to use pools with sufficient free space.
    pub(crate) fn min_free_space(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        match request.as_thin() {
            true => item.pool.free_space() > Self::free_space_watermark(),
            false => item.pool.free_space() > request.size,
        }
    }
    /// Return true if the pool has enough capacity to resize the replica by the requested
    /// value.
    pub(crate) fn min_free_space_repl_resize(
        request: &ReplicaResizePoolsContext,
        item: &ChildItem,
    ) -> bool {
        match request.spec().as_thin() {
            true => item.pool().free_space() > Self::free_space_watermark(),
            false => item.pool().free_space() > request.required_capacity(),
        }
    }
    /// Should only attempt to use pools with sufficient free space for a full rebuild.
    /// Currently the data-plane fully rebuilds a volume, meaning a thin provisioned volume
    /// becomes fully allocated.
    pub(crate) fn min_free_space_full_rebuild(
        request: &GetSuitablePoolsContext,
        item: &PoolItem,
    ) -> bool {
        match request.as_thin() && request.config().is_none() {
            true => item.pool.free_space() > Self::free_space_watermark(),
            false => item.pool.free_space() > request.size,
        }
    }
    /// Should only attempt to use usable (not faulted) pools.
    pub(crate) fn usable(_: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        item.pool.status != PoolStatus::Faulted && item.pool.status != PoolStatus::Unknown
    }

    /// Should only attempt to use pools having specific creation label if topology has it.
    pub(crate) fn topology(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        let volume_pool_topology_inclusion_labels: HashMap<String, String>;
        let volume_pool_topology_exclusion_labels: HashMap<String, String>;
        match request.topology.clone() {
            None => return true,
            Some(topology) => match topology.pool {
                None => return true,
                Some(pool_topology) => match pool_topology {
                    PoolTopology::Labelled(labelled_topology) => {
                        // Return false if the exclusion and incluson labels has any common key.
                        if labelled_topology
                            .inclusion
                            .keys()
                            .any(|key| labelled_topology.exclusion.contains_key(key))
                        {
                            return false;
                        }

                        if !labelled_topology.inclusion.is_empty()
                            || !labelled_topology.exclusion.is_empty()
                        {
                            volume_pool_topology_inclusion_labels = labelled_topology.inclusion;
                            volume_pool_topology_exclusion_labels = labelled_topology.exclusion;
                        } else {
                            return true;
                        }
                    }
                },
            },
        };

        // We will reach this part of code only if the volume has inclusion/exclusion labels.
        match request.registry().specs().pool(&item.pool.id) {
            Ok(spec) => match spec.labels {
                None => false,
                Some(pool_labels) => {
                    let inc_match = does_pool_qualify_inclusion_labels(
                        volume_pool_topology_inclusion_labels,
                        pool_labels.clone(),
                    );
                    let exc_match = does_pool_qualify_exclusion_labels(
                        volume_pool_topology_exclusion_labels,
                        pool_labels,
                    );
                    inc_match && exc_match
                }
            },
            Err(_) => false,
        }
    }
}

/// Return true if all the keys present in volume's pool inclusion matches with the pool labels
/// otherwise returns false.
pub(crate) fn does_pool_qualify_inclusion_labels(
    vol_pool_inc_labels: HashMap<String, String>,
    pool_labels: HashMap<String, String>,
) -> bool {
    for (vol_inc_key, vol_inc_value) in vol_pool_inc_labels.iter() {
        match pool_labels.get(vol_inc_key) {
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

/// Return true if all the key present in volume's pool exclusion matches with the pool labels and
/// their corresponding value are different otherwise returns false
pub(crate) fn does_pool_qualify_exclusion_labels(
    vol_pool_exc_labels: HashMap<String, String>,
    pool_labels: HashMap<String, String>,
) -> bool {
    // If volume's pool exclusion labels is empty, then it means that the volume has no exclusion
    if vol_pool_exc_labels.is_empty() {
        return true;
    }

    // exclusion_key_only are set of keys from vol_pool_exc_labels has only keys with empty values
    // e.g. vol_pool_exc_labels = {"key1": "", "key2": "value2"}
    let exclusion_key_only: Vec<_> = vol_pool_exc_labels
        .iter()
        .filter(|(_, value)| value.is_empty())
        .map(|(key, _)| key)
        .collect();

    if exclusion_key_only.is_empty() {
        // If volume's pool exclusion labels has any key with non-empty value, then it means that
        // the pool should not have that key with that value
        let common_keys: Vec<_> = vol_pool_exc_labels
            .keys()
            .filter(|&key| pool_labels.contains_key(key))
            .collect();

        if common_keys.is_empty() {
            return false;
        }

        return common_keys
            .iter()
            .any(|&key| vol_pool_exc_labels.get(key) != pool_labels.get(key));
    } else {
        // If volume's pool exclusion labels has any key with empty value, then it means that the
        // pool should not have that key
        !exclusion_key_only
            .iter()
            .any(|key| pool_labels.contains_key(&key.to_string()))
    }
}
