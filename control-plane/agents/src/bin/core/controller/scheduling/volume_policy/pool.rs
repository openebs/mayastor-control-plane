use crate::controller::{
    registry::Registry,
    scheduling::{
        resources::{ChildItem, PoolItem},
        volume::{GetSuitablePoolsContext, ReplicaResizePoolsContext},
        volume_policy::qualifies_label_criteria,
    },
};
use stor_port::types::v0::{
    store::volume::VolumeSpec,
    transport::{PoolId, PoolStatus, PoolTopology},
};

use std::collections::HashMap;

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
    /// Should only attempt to use encrypted pools.
    pub(crate) fn encrypted(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        let use_this = request.encrypted() == item.pool.encrypted;
        if !use_this && request.encrypted() && request.registry().encryption_preference_soft() {
            return !use_this;
        }
        use_this
    }
    /// Should only use the pools that match blobstore cluster size as specified via storage class,
    /// or if nothing specified match against a default blobstore cluster size.
    pub(crate) fn cluster_size(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        item.pool.cluster_size() == request.cluster_size
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

    /// Should only attempt to use uncordoned pools.
    pub(crate) fn uncordoned_repl(_: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        !item.cordoned().replicas
    }
    /// Should only attempt to use uncordoned pools.
    pub(crate) fn uncordoned_snaps(_: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        !item.cordoned().snapshots
    }
    /// Should only attempt to use uncordoned pools.
    pub(crate) fn uncordoned_rest(_: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        !item.cordoned().restores
    }

    /// Should only attempt to use pools having specific creation label if topology has it.
    pub(crate) fn topology(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        Self::topology_(request, request.registry(), &item.pool.id)
    }

    /// Should only attempt to use pools having specific creation label if topology has it.
    pub(crate) fn topology_(volume: &VolumeSpec, registry: &Registry, pool_id: &PoolId) -> bool {
        let volume_pool_topology_inclusion_labels: HashMap<String, String>;
        match &volume.topology {
            None => return true,
            Some(topology) => match &topology.pool {
                None => return true,
                Some(pool_topology) => match &pool_topology {
                    PoolTopology::Labelled(labelled_topology) => {
                        // The labels in Volume Pool Topology should match the pool labels if
                        // present, otherwise selection of any pool is allowed.
                        if labelled_topology.inclusion.is_empty() {
                            // todo: missing exclusion check?
                            return true;
                        }
                        volume_pool_topology_inclusion_labels = labelled_topology.inclusion.clone()
                    }
                },
            },
        };

        // We will reach this part of code only if the volume has inclusion/exclusion labels.
        match registry.specs().pool(pool_id) {
            Ok(spec) => match spec.labels {
                None => false,
                Some(pool_labels) => {
                    qualifies_label_criteria(volume_pool_topology_inclusion_labels, &pool_labels)
                }
            },
            Err(_) => false,
        }
    }
}
