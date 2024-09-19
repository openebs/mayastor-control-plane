use crate::controller::scheduling::{
    resources::{ChildItem, PoolItem},
    volume::{GetSuitablePoolsContext, ReplicaResizePoolsContext},
    volume_policy::qualifies_label_criteria,
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
        let volume_pool_afffinty: HashMap<String, String>;
        match request.topology.clone() {
            None => return true,
            Some(topology) => match topology.pool {
                None => return true,
                Some(pool_topology) => match pool_topology {
                    PoolTopology::Labelled(labelled_topology) => {
                        // The labels in Volume Pool Topology should match the pool labels if
                        // present, otherwise selection of any pool is allowed.
                        if !labelled_topology.inclusion.is_empty()
                            || !labelled_topology.affinity.is_empty()
                        {
                            volume_pool_topology_inclusion_labels = labelled_topology.inclusion;
                            volume_pool_afffinty = labelled_topology.affinity;
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
                Some(pool_labels) => qualifies_label_criteria(
                    volume_pool_topology_inclusion_labels,
                    volume_pool_afffinty,
                    &pool_labels,
                ),
            },
            Err(_) => false,
        }
    }
}
