use crate::core::{
    registry::Registry,
    wrapper::{NodeWrapper, PoolWrapper},
};
use common_lib::types::v0::{
    message_bus::{Child, ChildUri, Replica, Volume},
    store::{nexus_persistence::ChildInfo, replica::ReplicaSpec, volume::VolumeSpec},
};

#[derive(Debug, Clone)]
pub(crate) struct PoolItem {
    pub(crate) node: NodeWrapper,
    pub(crate) pool: PoolWrapper,
}

impl PoolItem {
    fn new(node: NodeWrapper, pool: PoolWrapper) -> Self {
        Self { node, pool }
    }
    pub(crate) fn collect(self) -> PoolWrapper {
        self.pool
    }
}

pub(crate) struct PoolItemLister {}
impl PoolItemLister {
    async fn nodes(registry: &Registry) -> Vec<NodeWrapper> {
        let nodes = registry.get_nodes_wrapper().await;
        let mut raw_nodes = vec![];
        for node in nodes {
            let node = node.lock().await;
            raw_nodes.push(node.clone());
        }
        raw_nodes
    }
    pub(crate) async fn list(registry: &Registry) -> Vec<PoolItem> {
        let pools = Self::nodes(registry)
            .await
            .iter()
            .map(|n| {
                n.pool_wrappers()
                    .iter()
                    .map(|p| PoolItem::new(n.clone(), p.clone()))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect();
        pools
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicaItem {
    replica: ReplicaSpec,
    child_uri: Option<ChildUri>,
    child_status: Option<Child>,
}

impl ReplicaItem {
    pub(crate) fn new(replica: ReplicaSpec, child_uri: Vec<ChildUri>, child: Vec<Child>) -> Self {
        Self {
            replica,
            child_uri: child_uri.first().cloned(),
            // ANA not currently supported
            child_status: child.first().cloned(),
        }
    }
    pub(crate) fn spec(&self) -> &ReplicaSpec {
        &self.replica
    }
    pub(crate) fn uri(&self) -> &Option<ChildUri> {
        &self.child_uri
    }
    pub(crate) fn status(&self) -> &Option<Child> {
        &self.child_status
    }
}

pub(crate) struct ReplicaItemLister {}
impl ReplicaItemLister {
    pub(crate) async fn list(
        registry: &Registry,
        spec: &VolumeSpec,
        state: &Volume,
    ) -> Vec<ReplicaItem> {
        let replicas = registry.specs.get_volume_replicas(&spec.uuid);
        let nexuses = registry.specs.get_volume_nexuses(&spec.uuid);
        let replicas = replicas.iter().map(|r| r.lock().clone());

        let replica_states = registry.get_replicas().await;
        replicas
            .map(|r| {
                ReplicaItem::new(
                    r.clone(),
                    replica_states
                        .iter()
                        .find(|rs| rs.uuid == r.uuid)
                        .map(|rs| {
                            nexuses
                                .iter()
                                .filter_map(|n| {
                                    n.lock()
                                        .children
                                        .iter()
                                        .find(|c| c.uri() == rs.uri)
                                        .map(|n| n.uri())
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                    replica_states
                        .iter()
                        .find(|rs| rs.uuid == r.uuid)
                        .map(|rs| {
                            state
                                .children
                                .iter()
                                .filter_map(|n| {
                                    n.children.iter().find(|c| c.uri.as_str() == rs.uri)
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
    }
}

/// Individual nexus child (replicas) which can be used for nexus creation
#[derive(Debug, Clone)]
pub(crate) struct ChildItem {
    replica_spec: ReplicaSpec,
    replica_state: Replica,
    child_info: Option<ChildInfo>,
}

/// If the nexus is shutdown uncleanly, only one child/replica may be used and it must be healthy
/// This is to avoid inconsistent data between the healthy replicas
#[derive(Debug, Clone)]
pub(crate) enum HealthyChildItems {
    /// One with multiple healthy candidates
    One(Vec<ChildItem>),
    /// All the healthy replicas can be used
    All(Vec<ChildItem>),
}
impl HealthyChildItems {
    /// Check if there are no healthy children
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            HealthyChildItems::One(items) => items.is_empty(),
            HealthyChildItems::All(items) => items.is_empty(),
        }
    }
}

impl ChildItem {
    /// Create a new `Self` from the replica and the persistent child information
    pub(crate) fn new(
        replica_spec: ReplicaSpec,
        replica_state: Replica,
        child_info: Option<ChildInfo>,
    ) -> Self {
        Self {
            replica_spec,
            replica_state,
            child_info,
        }
    }
    /// Get the replica spec
    pub(crate) fn spec(&self) -> &ReplicaSpec {
        &self.replica_spec
    }
    /// Get the replica state
    pub(crate) fn state(&self) -> &Replica {
        &self.replica_state
    }
    /// Get the persisted nexus child information
    pub(crate) fn info(&self) -> &Option<ChildInfo> {
        &self.child_info
    }
}
