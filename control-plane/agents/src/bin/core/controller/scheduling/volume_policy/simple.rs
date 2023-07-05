use crate::{
    controller::{
        registry::Registry,
        scheduling::{
            resources::PoolItem,
            volume::{AddVolumeReplica, GetSuitablePoolsContext, SnapshotVolumeReplica},
            volume_policy::{affinity_group, pool::PoolBaseFilters, DefaultBasePolicy},
            ResourceFilter, ResourcePolicy, SortBuilder, SortCriteria,
        },
    },
    ThinArgs,
};
use std::cmp::Ordering;
use weighted_scoring::{Criteria, Ranged, ValueGrading};

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
    /// Create a new simple policy.
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
            .filter(affinity_group::SingleReplicaPolicy::replica_anti_affinity)
            .filter_param(&self, SimplePolicy::min_free_space)
            .filter_param(&self, SimplePolicy::pool_overcommit)
            // sort pools in order of total weight of certain field values.
            .sort_ctx(SimplePolicy::sort_by_weights)
    }
}

#[async_trait::async_trait(?Send)]
impl ResourcePolicy<SnapshotVolumeReplica> for SimplePolicy {
    fn apply(self, to: SnapshotVolumeReplica) -> SnapshotVolumeReplica {
        DefaultBasePolicy::filter_snapshot(to)
            .filter(PoolBaseFilters::min_free_space)
            .filter_param(&self, SimplePolicy::min_free_space)
            .filter_param(&self, SimplePolicy::pool_overcommit)
    }
}

const TOTAL_REPLICA_COUNT_WEIGHT: Ranged = Ranged::new_const(25);
const FREE_SPACE_WEIGHT: Ranged = Ranged::new_const(40);
const OVER_COMMIT_WEIGHT: Ranged = Ranged::new_const(35);
// Only for Affinity Group multiple replica case.
const AG_TOTAL_REPLICA_COUNT_WEIGHT: Ranged = Ranged::new_const(15);
const AG_REPL_COUNT_WEIGHT: Ranged = Ranged::new_const(10);

impl SimplePolicy {
    /// SortCriteria for free space on pool.
    fn free_space() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("free_space", FREE_SPACE_WEIGHT),
            ValueGrading::Higher,
            |pool_item| pool_item.pool().free_space().into(),
        )
    }
    /// SortCriteria for number of Affinity Group replicas on pool only for Affinity Group.
    fn ag_replica_count() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("ag_replica_count", AG_REPL_COUNT_WEIGHT),
            ValueGrading::Lower,
            |pool_item| pool_item.ag_replica_count().into(),
        )
    }
    /// SortCriteria for number of total replicas on pool only for Affinity Group.
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
    /// SortCriteria for over commitment on pool.
    fn over_commitment() -> SortCriteria {
        SortCriteria::new(
            Criteria::new("over_commitment", OVER_COMMIT_WEIGHT),
            ValueGrading::Lower,
            |item| item.pool().over_commitment().into(),
        )
    }
    /// Sort pools using weights between:
    /// 1. number of replicas or number of replicas of a ag (N_REPL_WEIGHT %)
    /// 2. free space         (FREE_SPACE_WEIGHT %)
    /// 3. overcommitment     (OVER_COMMIT_WEIGHT %)
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
                        builder.with_criteria(SimplePolicy::non_ag_total_replica_count)
                    } else {
                        builder
                            .with_criteria(SimplePolicy::ag_replica_count)
                            .with_criteria(SimplePolicy::ag_total_replica_count)
                    }
                } else {
                    builder.with_criteria(SimplePolicy::non_ag_total_replica_count)
                }
                .with_criteria(SimplePolicy::free_space)
                .with_criteria(SimplePolicy::over_commitment)
                .compare(a, b)
            }
        }
    }

    /// Minimum free space is the currently allocated usage plus some percentage of volume size
    /// slack.
    fn min_free_space(&self, request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        if !request.as_thin() {
            return item.pool.free_space() > request.size;
        }
        if request.snap_repl() {
            return item.pool.free_space() > {
                (self.cli_args.snapshot_commitment * request.size) / 100
            };
        }
        item.pool.free_space()
            > match request.allocated_bytes() {
                Some(bytes) => {
                    let size = if bytes == &0 {
                        self.cli_args.volume_commitment_initial * request.size
                    } else {
                        self.cli_args.volume_commitment * request.size
                    } / 100;
                    (bytes + size).min(request.size)
                }
                None => {
                    // We really have no clue for some reason.. should not happen but just in case
                    // let's be conservative?
                    (self.no_state_min_free_space_percent * request.size) / 100
                }
            }
    }
    fn pool_overcommit(&self, request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        PoolBaseFilters::overcommit(request, item, self.cli_args.pool_commitment)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        controller::scheduling::{resources::PoolItem, volume_policy::SimplePolicy, SortBuilder},
        node::wrapper::NodeWrapper,
        pool::wrapper::PoolWrapper,
    };
    use std::{
        cmp::Ordering,
        net::{IpAddr, Ipv4Addr},
    };
    use stor_port::types::v0::transport::{NodeState, NodeStatus, PoolState, PoolStatus, Replica};

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
    fn make_pool(
        node: &str,
        pool: &str,
        free_space: u64,
        replicas: usize,
        ag_replica_count: Option<u64>,
    ) -> PoolItem {
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
            committed: None,
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
        PoolItem::new(node_state, pool, ag_replica_count)
    }
    fn ag_sort_comparator(a: &PoolItem, b: &PoolItem) -> Ordering {
        let builder = SortBuilder::new();
        if a.ag_replica_count.is_none() && b.ag_replica_count.is_none() {
            builder.with_criteria(SimplePolicy::non_ag_total_replica_count)
        } else {
            builder
                .with_criteria(SimplePolicy::ag_replica_count)
                .with_criteria(SimplePolicy::ag_total_replica_count)
        }
        .with_criteria(SimplePolicy::free_space)
        .with_criteria(SimplePolicy::over_commitment)
        .compare(a, b)
    }
    #[test]
    fn sort_by_weights() {
        let gig = 1024 * 1024 * 1024;

        let sort_builder = SortBuilder::new()
            .with_criteria(SimplePolicy::non_ag_total_replica_count)
            .with_criteria(SimplePolicy::free_space)
            .with_criteria(SimplePolicy::over_commitment);

        let mut pools = vec![
            make_pool("1", "1", gig, 0, None),
            make_pool("1", "2", gig * 2, 0, None),
            make_pool("2", "3", gig, 1, None),
        ];
        pools.sort_by(|a, b| sort_builder.compare(a, b));

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["2", "1", "3"]);

        let mut pools = vec![
            make_pool("1", "1", gig * 3, 0, None),
            make_pool("1", "2", gig * 2, 0, None),
            make_pool("2", "3", gig, 1, None),
        ];
        pools.sort_by(|a, b| sort_builder.compare(a, b));

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["1", "2", "3"]);

        let mut pools = vec![
            make_pool("1", "1", gig * 3, 400, None),
            make_pool("1", "2", gig * 2, 0, None),
            make_pool("2", "3", gig, 1, None),
        ];
        pools.sort_by(|a, b| sort_builder.compare(a, b));

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["2", "3", "1"]);

        let mut pools = vec![
            make_pool("1", "1", gig * 50, 100, None),
            make_pool("1", "2", gig * 2, 0, None),
            make_pool("2", "3", gig, 1, None),
        ];
        pools.sort_by(|a, b| sort_builder.compare(a, b));

        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(pool_names, vec!["1", "2", "3"]);

        let mut pools = vec![
            make_pool("1", "pool-1", gig, 500, None),
            make_pool("2", "pool-2", gig, 20, None),
            make_pool("3", "pool-3", gig, 500, Some(2)),
            make_pool("3", "pool-4", gig, 549, Some(4)),
            make_pool("3", "pool-5", gig, 550, Some(2)),
            make_pool("3", "pool-6", gig, 549, Some(2)),
        ];

        pools.sort_by(ag_sort_comparator);
        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(
            pool_names,
            vec!["pool-2", "pool-1", "pool-3", "pool-6", "pool-5", "pool-4"]
        );

        let mut pools = vec![
            make_pool("1", "pool-1", gig, 500, None),
            make_pool("2", "pool-2", gig * 2, 20, None),
            make_pool("3", "pool-3", gig, 500, Some(2)),
            make_pool("3", "pool-4", gig * 10, 549, Some(4)),
            make_pool("3", "pool-5", gig * 2, 550, Some(2)),
            make_pool("3", "pool-6", gig, 549, Some(2)),
        ];

        pools.sort_by(ag_sort_comparator);
        let pool_names = pools.iter().map(|p| p.pool.id.as_str()).collect::<Vec<_>>();
        assert_eq!(
            pool_names,
            vec!["pool-4", "pool-2", "pool-5", "pool-1", "pool-3", "pool-6"]
        );

        // todo: add more tests before merge
    }
}
