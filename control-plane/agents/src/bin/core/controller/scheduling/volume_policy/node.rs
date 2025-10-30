use crate::controller::{
    registry::Registry,
    resources::ResourceMutex,
    scheduling::{
        nexus::GetSuitableNodesContext,
        resources::{NodeItem, PoolItem, TopologyRmInfo},
        volume::GetSuitablePoolsContext,
        volume_policy::qualifies_label_criteria,
    },
};
use stor_port::types::v0::{
    store::{node::NodeSpec, replica::ReplicaSpec, volume::VolumeSpec},
    transport::{LabelledTopology, NodeId, NodeTopology},
};

use std::collections::HashMap;

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
        Self::topology_(
            request,
            request.registry(),
            &item.pool.node,
            request.spread_labels(),
        )
    }
    /// Get topology remove information used to help determine which replica should be removed first.
    pub(crate) fn topology_replica_removal(
        volume: &VolumeSpec,
        node: &NodeSpec,
        used_node_spread: &HashMap<String, Vec<String>>,
    ) -> Option<TopologyRmInfo> {
        let Some(topology) = volume_labels_any(volume) else {
            return TopologyRmInfo::new_no_topology();
        };

        if !qualifies_label_criteria(&topology.inclusion, node.labels())
            || !qualifies_label_criteria(&topology.exclusion, node.labels())
        {
            return TopologyRmInfo::new_invalid();
        }

        let mut conflicts = 0;
        for (used_key, used_values) in used_node_spread {
            let Some(node_value) = node.labels().get(used_key) else {
                // we'd have been excluded anyway
                continue;
            };
            if used_values.contains(node_value) {
                // we conflict with another
                conflicts += 1;
            }
        }

        TopologyRmInfo::new_clashes(conflicts)
    }
    /// Should only attempt to use nodes having specific creation label if topology has it.
    pub(crate) fn topology_(
        volume: &VolumeSpec,
        registry: &Registry,
        node_id: &NodeId,
        used_node_spread: &HashMap<String, Vec<String>>,
    ) -> bool {
        let Some(topology) = volume_labels_any(volume) else {
            return true;
        };

        // We will reach this part of code only if the volume has inclusion/exclusion labels.
        match registry.specs().node_rsc(node_id) {
            Ok(spec) => {
                let spec = spec.lock();
                qualifies_label_criteria(&topology.inclusion, spec.labels())
                    && qualifies_label_criteria(&topology.exclusion, spec.labels())
                    && qualifies_spread_criteria(used_node_spread, spec.labels())
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

/// Get volume labelled topology, if any is set.
pub(crate) fn volume_labels_any(volume: &VolumeSpec) -> Option<&LabelledTopology> {
    match volume_labels(volume) {
        Some(labelled_topology)
            if !labelled_topology.exclusion.is_empty()
                || !labelled_topology.inclusion.is_empty() =>
        {
            Some(labelled_topology)
        }
        _ => None,
    }
}
/// Get node exclusion labels, if any is present.
pub(crate) fn node_exclusion(volume: &VolumeSpec) -> Option<&HashMap<String, String>> {
    match volume_labels(volume) {
        Some(labelled_topology) if !labelled_topology.exclusion.is_empty() => {
            Some(&labelled_topology.exclusion)
        }
        _ => None,
    }
}

/// Get the currently used volume spread labels.
/// # NOTE
/// Only nodes which are respecting the current exclusion policy are used to populate these labels.
/// This ensures we start working towards the correct topology at the price of limiting the list
/// to the would-be correct nodes only!
pub(crate) fn volume_node_spread_labels(
    volume_spec: &VolumeSpec,
    registry: &Registry,
    replicas: &[ResourceMutex<ReplicaSpec>],
) -> HashMap<String, Vec<String>> {
    volume_node_spread_label_impl(
        volume_spec,
        replicas.iter().flat_map(|replica| {
            let replica = replica.lock();
            registry
                .specs()
                .pool_with(replica.pool_name(), |p| p.node.clone())
                .and_then(|n| registry.specs().node_rsc(&n))
        }),
    )
}
/// Similar to [`volume_node_spread_labels`] but accepting an iteration over [`ReplicaSpec`].
pub(crate) fn volume_node_spread_labels_x<'a>(
    volume_spec: &VolumeSpec,
    registry: &Registry,
    replicas: impl Iterator<Item = &'a ReplicaSpec>,
) -> HashMap<String, Vec<String>> {
    volume_node_spread_label_impl(
        volume_spec,
        replicas.into_iter().flat_map(|replica| {
            registry
                .specs()
                .pool_with(replica.pool_name(), |p| p.node.clone())
                .and_then(|n| registry.specs().node_rsc(&n))
        }),
    )
}

fn volume_node_spread_label_impl(
    volume_spec: &VolumeSpec,
    replica_nodes: impl Iterator<Item = ResourceMutex<NodeSpec>>,
) -> HashMap<String, Vec<String>> {
    let Some(volume_exc_labels) = node_exclusion(volume_spec) else {
        return Default::default();
    };
    if volume_exc_labels.is_empty() {
        return Default::default();
    }

    // the spread keys and the currently used values which cannot be reused by new replicas
    let mut exclude_labels = HashMap::<String, Vec<String>>::new();
    for node in replica_nodes {
        let node = node.lock();
        let labels = node.labels();

        node_spread_labels(volume_exc_labels, labels, &mut exclude_labels);
    }
    exclude_labels
}
fn node_spread_labels(
    volume_labels: &HashMap<String, String>,
    node_labels: &HashMap<String, String>,
    accrued_exclusions: &mut HashMap<String, Vec<String>>,
) {
    // first we need to ensure the node is respecting the volume's current spread policy
    // if not, then there's no point including this node in the log, since we'll have to remove it
    // sometime later anyway!
    let mut node_exclusions = HashMap::<&String, &String>::new();

    for vol_key in volume_labels.keys() {
        let Some(value) = node_labels.get(vol_key) else {
            // we're missing a key, so don't accrue this node!
            return;
        };
        node_exclusions.insert(vol_key, value);
    }

    // node is compliant with spread labels, so we can accrue with its labels
    for (key, value) in node_exclusions {
        accrued_exclusions
            .entry(key.clone())
            .or_default()
            .push(value.clone());
    }
}

fn qualifies_spread_criteria(
    used_labels: &HashMap<String, Vec<String>>,
    node_labels: &HashMap<String, String>,
) -> bool {
    for (used_key, used_values) in used_labels {
        let Some(node_values) = node_labels.get(used_key) else {
            // exclusion key must exist in order for spread to be valid
            return false;
        };
        if used_values.contains(node_values) {
            // we want to spread across different values
            return false;
        }
    }
    true
}

fn volume_labels(volume: &VolumeSpec) -> Option<&LabelledTopology> {
    let topology = volume.topology.as_ref();
    let node_topology = topology.and_then(|topology| topology.node.as_ref());

    match node_topology {
        Some(NodeTopology::Labelled(labelled_topology)) => Some(labelled_topology),
        _ => None,
    }
}
