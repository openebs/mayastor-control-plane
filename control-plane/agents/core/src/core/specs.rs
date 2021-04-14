use mbus_api::v0::{NexusId, NodeId, PoolId, ReplicaId, VolumeId};
use std::{collections::HashMap, ops::Deref, sync::Arc};
use store::types::v0 as sv0;
use sv0::{NexusSpec, NodeSpec, PoolSpec, ReplicaSpec, VolumeSpec};
use tokio::sync::{Mutex, RwLock};

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
#[derive(Default, Clone, Debug)]
pub(crate) struct ResourceSpecs {
    pub(crate) volumes: HashMap<VolumeId, Arc<Mutex<VolumeSpec>>>,
    pub(crate) nodes: HashMap<NodeId, Arc<Mutex<NodeSpec>>>,
    pub(crate) nexuses: HashMap<NexusId, Arc<Mutex<NexusSpec>>>,
    pub(crate) pools: HashMap<PoolId, Arc<Mutex<PoolSpec>>>,
    pub(crate) replicas: HashMap<ReplicaId, Arc<Mutex<ReplicaSpec>>>,
}
