use crate::controller::scheduling::{
    resources::PoolItem,
    volume::{AddVolumeReplica, GetSuitablePoolsContext},
    volume_policy::{pool::PoolBaseFilters, DefaultBasePolicy},
    ResourceFilter, ResourcePolicy,
};
use weighted_scoring::{Criteria, Ranged, ValueGrading, WeightedScore};

/// A very simple policy for pool replica placement that takes into account thin provisioning
/// and currently allocated bytes.
/// todo: make parameters configurable: eg they could be provided during volume creation?
pub(crate) struct SimplePolicy {
    /// Current volume replicas are not online.
    /// Configure a minimum size that is a percentage of the volume size.
    no_state_min_free_space_percent: u64,
    /// When creating a volume replica, the chosen pool should have free space larger than
    /// the current volume allocation plus some slack.
    min_free_space_slack_percent: u64,
}
impl Default for SimplePolicy {
    fn default() -> Self {
        Self {
            no_state_min_free_space_percent: 100,
            min_free_space_slack_percent: 40,
        }
    }
}
impl SimplePolicy {
    pub(crate) fn builder() -> Self {
        Self::default()
    }
}
#[async_trait::async_trait(?Send)]
impl ResourcePolicy<AddVolumeReplica> for SimplePolicy {
    fn apply(self, to: AddVolumeReplica) -> AddVolumeReplica {
        DefaultBasePolicy::filter(to)
            .filter(PoolBaseFilters::min_free_space)
            .filter_param(&self, SimplePolicy::min_free_space)
            // sort pools in order of preference (from least to most number of replicas)
            .sort(SimplePolicy::sort_by_weights)
    }
}

impl SimplePolicy {
    /// Sort pools using weights between:
    /// 1. number of replicas (N_REPL_WEIGHT %)
    /// 2. free space         (FREE_SPACE_WEIGHT %)
    /// 3. overcommitment     (OVER_COMMIT_WEIGHT %)
    pub(crate) fn sort_by_weights(a: &PoolItem, b: &PoolItem) -> std::cmp::Ordering {
        const N_REPL_WEIGHT: Ranged = Ranged::new_const(25);
        const FREE_SPACE_WEIGHT: Ranged = Ranged::new_const(40);
        const OVER_COMMIT_WEIGHT: Ranged = Ranged::new_const(35);

        let n_replicas = Criteria::new("n_replicas", N_REPL_WEIGHT);
        let free_space = Criteria::new("free_space", FREE_SPACE_WEIGHT);
        let over_commit = Criteria::new("over_commit", OVER_COMMIT_WEIGHT);

        let (score_a, score_b) = WeightedScore::dual_values()
            .weigh(n_replicas, ValueGrading::Lower, a.len(), b.len())
            .weigh(
                free_space,
                ValueGrading::Higher,
                a.pool().free_space(),
                b.pool().free_space(),
            )
            .weigh(
                over_commit,
                ValueGrading::Lower,
                a.pool().over_commitment(),
                b.pool().over_commitment(),
            )
            .score()
            .unwrap();

        score_a.cmp(&score_b)
    }
    /// Minimum free space is the currently allocated usage plus some percentage of volume size
    /// slack.
    fn min_free_space(&self, request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        if !request.thin {
            return item.pool.free_space() > request.size;
        }
        match request.allocated_bytes() {
            Some(bytes) => {
                let size = (self.min_free_space_slack_percent * request.size) / 100;
                let required_cap = (bytes + size).min(request.size);
                item.pool.free_space() > required_cap
            }
            None => {
                let min_size = (self.no_state_min_free_space_percent * request.size) / 100;
                item.pool.free_space() > min_size
            }
        }
    }
}
