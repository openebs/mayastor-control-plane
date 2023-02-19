use crate::controller::scheduling::{
    volume::AddVolumeReplica,
    volume_policy::{
        pool::{PoolBaseFilters, PoolBaseSorters},
        DefaultBasePolicy,
    },
    ResourceFilter, ResourcePolicy,
};

pub(crate) struct ThickPolicy {}
impl ThickPolicy {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait(?Send)]
impl ResourcePolicy<AddVolumeReplica> for ThickPolicy {
    fn apply(self, to: AddVolumeReplica) -> AddVolumeReplica {
        DefaultBasePolicy::filter(to)
            .filter(PoolBaseFilters::min_free_space_full_rebuild)
            // sort pools in order of preference (from least to most number of replicas)
            .sort(PoolBaseSorters::sort_by_replica_count)
    }
}
