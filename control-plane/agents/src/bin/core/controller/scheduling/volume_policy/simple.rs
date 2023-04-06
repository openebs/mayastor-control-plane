use crate::{
    controller::{
        registry::Registry,
        scheduling::{
            resources::PoolItem,
            volume::{AddVolumeReplica, GetSuitablePoolsContext},
            volume_policy::{pool::PoolBaseFilters, DefaultBasePolicy},
            ResourceFilter, ResourcePolicy,
        },
    },
    ThinArgs,
};
use weighted_scoring::{Criteria, Ranged, ValueGrading, WeightedScore};

/// A very simple policy for pool replica placement that takes into account thin provisioning
/// and currently allocated bytes.
/// todo: make parameters configurable: eg they could be provided during volume creation?
pub(crate) struct SimplePolicy {
    /// Current volume replicas are not online.
    /// Configure a minimum size that is a percentage of the volume size.
    no_state_min_free_space_percent: u64,
    cli_args: ThinArgs,
}

impl SimplePolicy {
    pub(crate) fn new(registry: &Registry) -> Self {
        Self {
            no_state_min_free_space_percent: 100,
            cli_args: registry.thin_args().clone(),
        }
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

        score_b.cmp(&score_a)
    }
    /// Minimum free space is the currently allocated usage plus some percentage of volume size
    /// slack.
    fn min_free_space(&self, request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        if !request.thin {
            return item.pool.free_space() > request.size;
        }
        match request.allocated_bytes() {
            Some(bytes) => {
                let size = if bytes == &0 {
                    self.cli_args.volume_commitment_initial * request.size
                } else {
                    self.cli_args.volume_commitment * request.size
                } / 100;
                let required_cap = (bytes + size).min(request.size);
                item.pool.free_space() > required_cap
            }
            None => {
                // We really have no clue for some reason.. should not happen but just in case
                // let's be conservative?
                let min_size = (self.no_state_min_free_space_percent * request.size) / 100;
                item.pool.free_space() > min_size
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        controller::scheduling::{resources::PoolItem, volume_policy::SimplePolicy},
        node::wrapper::NodeWrapper,
        pool::wrapper::PoolWrapper,
    };
    use stor_port::types::v0::transport::{NodeState, NodeStatus, PoolState, PoolStatus, Replica};

    use std::net::{IpAddr, Ipv4Addr};

    fn make_node(name: &str) -> NodeWrapper {
        let state = NodeState::new(
            name.into(),
            std::net::SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            NodeStatus::Online,
            None,
            None,
        );
        NodeWrapper::new_stub(&state)
    }
    fn make_pool(node: &str, pool: &str, free_space: u64, replicas: usize) -> PoolItem {
        let node_state = make_node(node);
        let used = 0; //10 * 1024 * 1024 * 1024;
        let capacity = free_space + used;
        let state = PoolState {
            node: node.into(),
            id: pool.into(),
            disks: vec![],
            status: PoolStatus::Online,
            capacity,
            used,
        };
        let replica = Replica::default();
        let pool = PoolWrapper::new(
            state,
            vec![replica]
                .into_iter()
                .cycle()
                .take(replicas)
                .collect::<Vec<_>>(),
        );
        PoolItem::new(node_state, pool)
    }
    #[test]
    fn sort_by_weights() {
        let gig = 1024 * 1024 * 1024;
        let mut pools = vec![
            make_pool("1", "1", gig, 0),
            make_pool("1", "2", gig * 2, 0),
            make_pool("2", "3", gig, 1),
        ];
        pools.sort_by(SimplePolicy::sort_by_weights);

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["2", "1", "3"]);

        let mut pools = vec![
            make_pool("1", "1", gig * 3, 0),
            make_pool("1", "2", gig * 2, 0),
            make_pool("2", "3", gig, 1),
        ];
        pools.sort_by(SimplePolicy::sort_by_weights);

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["1", "2", "3"]);

        let mut pools = vec![
            make_pool("1", "1", gig * 3, 400),
            make_pool("1", "2", gig * 2, 0),
            make_pool("2", "3", gig, 1),
        ];
        pools.sort_by(SimplePolicy::sort_by_weights);

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["2", "3", "1"]);

        let mut pools = vec![
            make_pool("1", "1", gig * 50, 100),
            make_pool("1", "2", gig * 2, 0),
            make_pool("2", "3", gig, 1),
        ];
        pools.sort_by(SimplePolicy::sort_by_weights);

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["1", "2", "3"]);

        // todo: add more tests before merge
    }
}
