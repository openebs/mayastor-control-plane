use crate::core::{
    registry::Registry,
    wrapper::{NodeWrapper, PoolWrapper},
};
use common_lib::types::v0::{
    store::{
        nexus_child::NexusChild,
        nexus_persistence::{ChildInfo, NexusInfo},
        replica::ReplicaSpec,
    },
    transport::{Child, ChildUri, Replica},
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
        let nodes = registry.get_node_wrappers().await;
        let mut raw_nodes = vec![];
        for node in nodes {
            let node = node.read().await;
            raw_nodes.push(node.clone());
        }
        raw_nodes
    }
    pub(crate) async fn list(registry: &Registry) -> Vec<PoolItem> {
        let pools = Self::nodes(registry)
            .await
            .iter()
            .flat_map(|n| {
                n.pool_wrappers()
                    .iter()
                    .filter(|p| registry.specs().get_pool(&p.id).is_ok())
                    .map(|p| PoolItem::new(n.clone(), p.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        pools
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicaItem {
    replica_spec: ReplicaSpec,
    replica_state: Option<Replica>,
    child_uri: Option<ChildUri>,
    child_state: Option<Child>,
    child_spec: Option<NexusChild>,
    child_info: Option<ChildInfo>,
}

impl ReplicaItem {
    /// Create new `Self` from the provided arguments
    pub(crate) fn new(
        replica: ReplicaSpec,
        replica_state: Option<&Replica>,
        child_uri: Option<ChildUri>,
        child_state: Option<Child>,
        child_spec: Option<NexusChild>,
        child_info: Option<ChildInfo>,
    ) -> Self {
        Self {
            replica_spec: replica,
            replica_state: replica_state.cloned(),
            child_uri,
            child_state,
            child_spec,
            child_info,
        }
    }
    /// Get a reference to the replica spec
    pub(crate) fn spec(&self) -> &ReplicaSpec {
        &self.replica_spec
    }
    /// Get a reference to the replica state
    pub(crate) fn state(&self) -> Option<&Replica> {
        self.replica_state.as_ref()
    }
    /// Get a reference to the child spec
    pub(crate) fn uri(&self) -> &Option<ChildUri> {
        &self.child_uri
    }
    /// Get a reference to the child state
    pub(crate) fn child_state(&self) -> &Option<Child> {
        &self.child_state
    }
    /// Get a reference to the child spec
    pub(crate) fn child_spec(&self) -> Option<&NexusChild> {
        self.child_spec.as_ref()
    }
    /// Get a reference to the child info
    pub(crate) fn child_info(&self) -> Option<&ChildInfo> {
        self.child_info.as_ref()
    }
}

/// Individual nexus child (replicas) which can be used for nexus creation
#[derive(Clone)]
pub(crate) struct ChildItem {
    replica_spec: ReplicaSpec,
    replica_state: Replica,
    pool_state: PoolWrapper,
    child_info: Option<ChildInfo>,
}

// todo: keep original Debug and use valuable trait from tracing instead
impl std::fmt::Debug for ChildItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildItem")
            .field("replica_spec", &self.replica_spec)
            .field("replica_state", &self.replica_state)
            .field("pool_state", self.pool_state.state())
            .field("child_info", &self.child_info.as_ref())
            .finish()
    }
}

/// If the nexus is shutdown uncleanly, only one child/replica may be used and it must be healthy
/// This is to avoid inconsistent data between the healthy replicas
#[derive(Debug, Clone)]
pub(crate) enum HealthyChildItems {
    /// One with multiple healthy candidates
    One(Option<NexusInfo>, Vec<ChildItem>),
    /// All the healthy replicas can be used
    All(Option<NexusInfo>, Vec<ChildItem>),
}
impl HealthyChildItems {
    /// Check if there are no healthy children
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            HealthyChildItems::One(_, items) => items.is_empty(),
            HealthyChildItems::All(_, items) => items.is_empty(),
        }
    }
    /// Get a reference to the list of candidates
    pub(crate) fn candidates(&self) -> &Vec<ChildItem> {
        match self {
            HealthyChildItems::One(_, items) => items,
            HealthyChildItems::All(_, items) => items,
        }
    }
    /// Get a reference to the list of candidates
    pub(crate) fn nexus_info(&self) -> &Option<NexusInfo> {
        match self {
            HealthyChildItems::One(info, _) => info,
            HealthyChildItems::All(info, _) => info,
        }
    }
}

impl ChildItem {
    /// Create a new `Self` from the replica and the persistent child information
    pub(crate) fn new(
        replica_spec: &ReplicaSpec,
        replica_state: &Replica,
        child_info: Option<&ChildInfo>,
        pool_state: &PoolWrapper,
    ) -> Self {
        Self {
            replica_spec: replica_spec.clone(),
            replica_state: replica_state.clone(),
            child_info: child_info.cloned(),
            pool_state: pool_state.clone(),
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
    /// Get the pool wrapper
    pub(crate) fn pool(&self) -> &PoolWrapper {
        &self.pool_state
    }
}
