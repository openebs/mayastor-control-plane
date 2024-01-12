use super::{volume::ResizeVolumeReplicas, ReplicaFilters, ResourceFilter};
use crate::controller::scheduling::{
    volume::{AddVolumeReplica, CloneVolumeSnapshot, SnapshotVolumeReplica},
    NodeFilters,
};

mod affinity_group;
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
            .filter(NodeFilters::cordoned_for_pool)
            .filter(NodeFilters::online_for_pool)
            .filter(NodeFilters::allowed)
            .filter(NodeFilters::unused)
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
            .filter(NodeFilters::cordoned_for_pool)
            .filter(NodeFilters::online_for_pool)
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
            .filter(NodeFilters::cordoned_for_pool)
            .filter(NodeFilters::online_for_pool)
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
