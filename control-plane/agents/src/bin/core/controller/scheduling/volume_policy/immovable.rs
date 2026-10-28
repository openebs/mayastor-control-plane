//! Read-only evaluation of why a replica cannot be moved to any other pool.
//!
//! This mirrors the filters the provisioning path applies to `AddVolumeReplica`, see
//! `DefaultBasePolicy::filter` and `SimplePolicy`/`ThickPolicy`, but runs every filter
//! independently over the full pool list so that all failing reasons are reported, rather than
//! only the first filter which excluded a pool.
//! It is purely informational: the decision on whether a replica is movable stays with the
//! scheduler itself, and none of the scheduling filters or policies depend on this module.
use crate::controller::{
    registry::Registry,
    scheduling::{
        volume::{AddVolumeReplica, GetSuitablePools},
        volume_policy::{
            affinity_group::SingleReplicaPolicy, node::NodeFilters, pool::PoolBaseFilters,
            SimplePolicy,
        },
    },
};
use stor_port::types::v0::{store::volume::VolumeSpec, transport::PoolId};

use std::collections::BTreeSet;

/// The category of constraint which excluded a pool as a replica move target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ImmovableReason {
    /// Node constraints: cordoned, offline, not allowed or already used by the volume.
    Nodes,
    /// Capacity constraints: pool too small, not enough free space or overcommitted.
    Size,
    /// Node or pool topology constraints.
    Topology,
    /// The pool blobstore cluster size does not match the volume's.
    ClusterSize,
    /// A single replica volume whose only replica is never evicted when unsafe eviction was
    /// requested.
    SingleReplicaEviction,
    /// The pool does not satisfy the volume's encryption requirement.
    Encryption,
    /// Affinity group anti-affinity: the node already holds a replica of another volume of the
    /// same affinity group.
    AffinityGroup,
    /// No pool other than the one the replica is on is available as a target.
    NoCandidatePool,
    /// Any other constraint: pool state or cordon.
    Other,
}

/// Returns every category of constraint which excludes at least one pool from receiving the
/// replica currently on `source_pool`, excluding `source_pool` itself.
/// The drain places a spare replica before removing the moving one, so, unlike a plain replica
/// move, a node which already holds a replica of the volume is not a valid target.
/// The result is empty if no pool is excluded by any constraint.
pub(crate) async fn replica_move_blockers(
    registry: &Registry,
    volume: &VolumeSpec,
    source_pool: &PoolId,
) -> BTreeSet<ImmovableReason> {
    use ImmovableReason::*;

    let request = GetSuitablePools::new(volume, None);
    let (ctx, pools) = AddVolumeReplica::context_and_pools(request, registry).await;

    let thin = ctx.as_thin();
    let simple = SimplePolicy::new(registry);
    let mut blockers = BTreeSet::new();

    let targets = pools
        .iter()
        .filter(|p| &p.pool.id != source_pool)
        .collect::<Vec<_>>();
    if targets.is_empty() {
        blockers.insert(NoCandidatePool);
        return blockers;
    }

    for pool in targets {
        // Record `reason` if the filter rejected this pool.
        let mut check = |passed: bool, reason| {
            if !passed {
                blockers.insert(reason);
            }
        };

        check(NodeFilters::cordoned_for_pool(&ctx, pool), Nodes);
        check(NodeFilters::online_for_pool(&ctx, pool), Nodes);
        check(NodeFilters::allowed(&ctx, pool), Nodes);
        check(NodeFilters::unused(&ctx, pool), Nodes);
        check(NodeFilters::topology(&ctx, pool), Topology);

        check(PoolBaseFilters::usable(&ctx, pool), Other);
        check(PoolBaseFilters::uncordoned_repl(&ctx, pool), Other);
        check(PoolBaseFilters::capacity(&ctx, pool), Size);
        check(PoolBaseFilters::min_free_space(&ctx, pool), Size);
        check(PoolBaseFilters::topology(&ctx, pool), Topology);

        if thin {
            check(simple.min_free_space(&ctx, pool), Size);
            check(simple.pool_overcommit(&ctx, pool), Size);
        } else {
            check(
                PoolBaseFilters::min_free_space_full_rebuild(&ctx, pool),
                Size,
            );
        }

        check(PoolBaseFilters::encrypted(&ctx, pool), Encryption);
        check(PoolBaseFilters::cluster_size(&ctx, pool), ClusterSize);
        check(
            SingleReplicaPolicy::replica_anti_affinity(&ctx, pool),
            AffinityGroup,
        );
    }
    blockers
}
