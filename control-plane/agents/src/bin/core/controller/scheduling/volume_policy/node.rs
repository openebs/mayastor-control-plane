use crate::controller::scheduling::{
    nexus::GetSuitableNodesContext,
    resources::{NodeItem, PoolItem},
    volume::GetSuitablePoolsContext,
    volume_policy::qualifies_inclusion_labels,
};
use std::collections::HashMap;
use stor_port::types::v0::transport::NodeTopology;

/// Filter nodes used for replica creation.
pub(crate) struct NodeFilters {}
impl NodeFilters {
    /// Should only attempt to use online nodes for pools.
    pub(crate) fn online_for_pool(_request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        item.node.is_online()
    }
    /// Should only attempt to use allowed nodes (by the topology).
    pub(crate) fn allowed(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        request.allowed_nodes().is_empty() || request.allowed_nodes().contains(&item.pool.node)
    }
    /// Should only attempt to use nodes not currently used by the volume.
    /// When moving a replica the current replica node is allowed to be reused for a different pool.
    pub(crate) fn unused(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        if let Some(moving) = request.move_repl() {
            if moving.node() == &item.pool.node && moving.pool() != &item.pool.id {
                return true;
            }
        }
        let registry = request.registry();
        let used_nodes = registry.specs().volume_data_nodes(&request.uuid);
        !used_nodes.contains(&item.pool.node)
    }
    /// Should only attempt to use nodes which are not cordoned.
    pub(crate) fn cordoned_for_pool(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        let registry = request.registry();
        !registry
            .specs()
            .cordoned_nodes()
            .into_iter()
            .any(|node_spec| node_spec.id() == &item.pool.node)
    }

    /// Should only attempt to use online nodes.
    pub(crate) fn online(_request: &GetSuitableNodesContext, item: &NodeItem) -> bool {
        item.node_wrapper().is_online()
    }

    /// Should only attempt to use nodes which are not cordoned.
    pub(crate) fn cordoned(request: &GetSuitableNodesContext, item: &NodeItem) -> bool {
        let registry = request.registry();
        !registry
            .specs()
            .cordoned_nodes()
            .into_iter()
            .any(|node_spec| node_spec.id() == item.node_wrapper().id())
    }

    /// Should only attempt to use node where current target is not present.
    pub(crate) fn current_target(request: &GetSuitableNodesContext, item: &NodeItem) -> bool {
        if let Some(target) = request.target() {
            target.node() != item.node_wrapper().id()
        } else {
            true
        }
    }
    /// Should only attempt to use node where there are no targets for the current volume.
    pub(crate) fn no_targets(request: &GetSuitableNodesContext, item: &NodeItem) -> bool {
        let volume_targets = request.registry().specs().volume_nexuses(&request.uuid);
        !volume_targets
            .into_iter()
            .any(|n| &n.lock().node == item.node_wrapper().id())
    }
    /// Should only attempt to use nodes having specific creation label if topology has it.
    pub(crate) fn topology(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        let volume_node_topology_inclusion_labels: HashMap<String, String>;
        match &request.topology {
            None => return true,
            Some(topology) => match &topology.node {
                None => return true,
                Some(node_topology) => match node_topology {
                    NodeTopology::Labelled(labelled_topology) => {
                        // The labels in Volume Node Topology should match the node labels if
                        // present, otherwise selection of any pool is allowed.
                        if !labelled_topology.inclusion.is_empty() {
                            volume_node_topology_inclusion_labels =
                                labelled_topology.inclusion.clone()
                        } else {
                            return true;
                        }
                    }
                    NodeTopology::Explicit(_) => return true,
                },
            },
        };

        // We will reach this part of code only if the volume has inclusion/exclusion labels.
        match request.registry().specs().node(&item.pool.node) {
            Ok(spec) => {
                let inc_match = qualifies_inclusion_labels(
                    volume_node_topology_inclusion_labels,
                    spec.labels(),
                );
                inc_match
            }
            Err(_) => false,
        }
    }
}

/// Sort nodes to pick the best choice for nexus target.
pub(crate) struct NodeSorters {}
impl NodeSorters {
    /// Sort nodes by the number of active nexus present per node.
    /// The lesser the number of active nexus on a node, the more would be its selection priority.
    /// In case this is a Affinity Group, then it would be spread on basis of number of ag targets
    /// and then on basis of total targets on equal.
    pub(crate) fn number_targets(a: &NodeItem, b: &NodeItem) -> std::cmp::Ordering {
        a.ag_nexus_count()
            .cmp(&b.ag_nexus_count())
            .then_with(|| a.ag_preferred().cmp(&b.ag_preferred()).reverse())
            .then_with(|| {
                a.node_wrapper()
                    .nexus_count()
                    .cmp(&b.node_wrapper().nexus_count())
            })
    }
}
