use crate::core::{
    registry::Registry,
    wrapper::{NodeWrapper, PoolWrapper},
};
use common_lib::types::v0::{
    message_bus::{Child, ChildUri, Volume},
    store::{replica::ReplicaSpec, volume::VolumeSpec},
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

        // no error is actually ever returned, fixup:
        let replica_states = registry.get_replicas().await.unwrap();
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
