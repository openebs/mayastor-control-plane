use crate::controller::scheduling::{
    resources::PoolItem,
    volume::{AddVolumeReplica, GetSuitablePoolsContext},
    volume_policy::{affinity_group, pool::PoolBaseFilters, DefaultBasePolicy},
    ResourceFilter, ResourcePolicy, SortBuilder, SortCriteria,
};
use std::cmp::Ordering;
use weighted_scoring::{Criteria, Ranged, ValueGrading};

const TOTAL_REPLICA_COUNT_WEIGHT: Ranged = Ranged::new_const(40);
const FREE_SPACE_WEIGHT: Ranged = Ranged::new_const(60);
// Only for Affinity Group multiple replica case.
const AG_TOTAL_REPLICA_COUNT_WEIGHT: Ranged = Ranged::new_const(25);
const AG_REPL_COUNT_WEIGHT: Ranged = Ranged::new_const(15);

/// Policy for thick provisioned volumes.
pub(crate) struct ThickPolicy {}

#[async_trait::async_trait(?Send)]
impl ResourcePolicy<AddVolumeReplica> for ThickPolicy {
    fn apply(self, to: AddVolumeReplica) -> AddVolumeReplica {
        DefaultBasePolicy::filter(to)
            .filter(PoolBaseFilters::min_free_space_full_rebuild)
            .filter(affinity_group::SingleReplicaPolicy::replica_anti_affinity)
            // sort pools in order of preference (from least to most number of replicas)
            .sort_ctx(ThickPolicy::sort_by_weights)
    }
}

impl ThickPolicy {
    /// Create a new thick policy.
    pub(crate) fn new() -> Self {
        Self {}
    }
    /// SortCriteria for free space on pool.
    fn free_space() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("free_space", FREE_SPACE_WEIGHT),
            ValueGrading::Higher,
            |pool_item| pool_item.pool().free_space().into(),
        )
    }
    /// SortCriteria for number of total replicas on pool for Affinity Group.
    fn ag_replica_count() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("ag_replica_count", AG_REPL_COUNT_WEIGHT),
            ValueGrading::Lower,
            |pool_item| pool_item.ag_replica_count().into(),
        )
    }
    /// SortCriteria for number of total replicas on pool for Affinity Group.
    fn ag_total_replica_count() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("ag_total_replica_count", AG_TOTAL_REPLICA_COUNT_WEIGHT),
            ValueGrading::Lower,
            |pool_item| pool_item.len().into(),
        )
    }
    /// SortCriteria for number of total replicas on pool.
    fn non_ag_total_replica_count() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("non_ag_total_replica_count", TOTAL_REPLICA_COUNT_WEIGHT),
            ValueGrading::Lower,
            |item| item.len().into(),
        )
    }
    /// Sort pools by state and then by using weights between:
    /// 1. number of replicas or number of replicas of a ag (N_REPL_WEIGHT %)
    /// 2. free space         (FREE_SPACE_WEIGHT %)
    pub(crate) fn sort_by_weights(
        request: &GetSuitablePoolsContext,
        a: &PoolItem,
        b: &PoolItem,
    ) -> std::cmp::Ordering {
        match a.pool.state().status.partial_cmp(&b.pool().state().status) {
            Some(Ordering::Greater) => Ordering::Greater,
            Some(Ordering::Less) => Ordering::Less,
            None | Some(Ordering::Equal) => {
                let builder = SortBuilder::new();
                if request.affinity_group.is_some() && request.num_replicas > 1 {
                    if a.ag_replica_count.is_none() && b.ag_replica_count.is_none() {
                        builder.with_criteria(ThickPolicy::non_ag_total_replica_count)
                    } else {
                        builder
                            .with_criteria(ThickPolicy::ag_replica_count)
                            .with_criteria(ThickPolicy::ag_total_replica_count)
                    }
                } else {
                    builder.with_criteria(ThickPolicy::non_ag_total_replica_count)
                }
                .with_criteria(ThickPolicy::free_space)
                .compare(a, b)
            }
        }
    }
}
