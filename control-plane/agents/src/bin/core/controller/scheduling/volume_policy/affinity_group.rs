use crate::controller::scheduling::{resources::PoolItem, volume::GetSuitablePoolsContext};
use itertools::Itertools;

/// Policy for single replica volumes of a AffinityGroup.
/// This ensure, each replica of the volumes of the Affinity Group
/// are placed on a different node, preventing single point of failure.
/// Currently this policy is strict i.e creation will fail if policy is not met.
pub(crate) struct SingleReplicaPolicy {}
impl SingleReplicaPolicy {
    /// Should only use nodes which don't have replica of other volume of the same Affinity Group.
    /// This filter ensures replica anti-affinity among volumes of single replica of a Affinity
    /// Group.
    pub(crate) fn replica_anti_affinity(
        request: &GetSuitablePoolsContext,
        item: &PoolItem,
    ) -> bool {
        if let Some(restricted_nodes) = request.ag_restricted_nodes() {
            return !restricted_nodes.iter().contains(item.node.id());
        }
        true
    }
}
