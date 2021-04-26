use crate::core::registry::Registry;
use std::{collections::HashMap, ops::Deref, sync::Arc};

use tokio::sync::{Mutex, RwLock};

use mbus_api::v0::{NexusId, NodeId, PoolId, ReplicaId, VolumeId};
use store::types::v0::{
    nexus::NexusSpec,
    node::NodeSpec,
    pool::PoolSpec,
    replica::ReplicaSpec,
    volume::VolumeSpec,
};

/// Locked Resource Specs
#[derive(Default, Clone, Debug)]
pub(crate) struct ResourceSpecsLocked(Arc<RwLock<ResourceSpecs>>);

impl Deref for ResourceSpecsLocked {
    type Target = Arc<RwLock<ResourceSpecs>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Resource Specs
#[derive(Default, Debug)]
pub(crate) struct ResourceSpecs {
    pub(crate) volumes: HashMap<VolumeId, Arc<Mutex<VolumeSpec>>>,
    pub(crate) nodes: HashMap<NodeId, Arc<Mutex<NodeSpec>>>,
    pub(crate) nexuses: HashMap<NexusId, Arc<Mutex<NexusSpec>>>,
    pub(crate) pools: HashMap<PoolId, Arc<Mutex<PoolSpec>>>,
    pub(crate) replicas: HashMap<ReplicaId, Arc<Mutex<ReplicaSpec>>>,
}

impl ResourceSpecsLocked {
    pub(crate) fn new() -> Self {
        ResourceSpecsLocked::default()
    }
    /// Start worker threads
    /// 1. test store connections and commit dirty specs to the store
    pub(crate) fn start(&self, registry: Registry) {
        let this = self.clone();
        tokio::spawn(async move {
            this.reconcile_dirty_replicas(registry).await;
        });
    }
}
