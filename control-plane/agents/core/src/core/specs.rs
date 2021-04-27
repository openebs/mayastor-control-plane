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
        tokio::spawn(async move { this.reconcile_dirty_specs(registry).await });
    }

    /// Reconcile dirty specs to the persistent store
    async fn reconcile_dirty_specs(&self, registry: Registry) {
        loop {
            let dirty_replicas = self.reconcile_dirty_replicas(&registry).await;
            let dirty_nexuses = self.reconcile_dirty_nexuses(&registry).await;

            let period = if dirty_nexuses || dirty_replicas {
                registry.reconcile_period
            } else {
                registry.reconcile_idle_period
            };

            tokio::time::delay_for(period).await;
        }
    }
}
