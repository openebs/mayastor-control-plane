use common_lib::types::v0::{
    message_bus::{Nexus, NexusId, Pool, PoolId, Replica, ReplicaId},
    store::{nexus::NexusState, pool::PoolState, replica::ReplicaState},
};
use std::{ops::Deref, sync::Arc};

use super::resource_map::ResourceMap;
use parking_lot::{Mutex, RwLock};

/// Locked Resource States
#[derive(Default, Clone, Debug)]
pub(crate) struct ResourceStatesLocked(Arc<RwLock<ResourceStates>>);

impl ResourceStatesLocked {
    pub(crate) fn new() -> Self {
        ResourceStatesLocked::default()
    }
}

impl Deref for ResourceStatesLocked {
    type Target = Arc<RwLock<ResourceStates>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Resource States
#[derive(Default, Debug)]
pub(crate) struct ResourceStates {
    /// Todo: Add runtime state information for nodes.
    nexuses: ResourceMap<NexusId, NexusState>,
    pools: ResourceMap<PoolId, PoolState>,
    replicas: ResourceMap<ReplicaId, ReplicaState>,
}

impl ResourceStates {
    /// Update the various resource states.
    /// This purges any previous updates.
    pub(crate) fn update(&mut self, pools: Vec<Pool>, replicas: Vec<Replica>, nexuses: Vec<Nexus>) {
        self.replicas.clear();
        self.replicas.populate(replicas);

        self.pools.clear();
        self.pools.populate(pools);

        self.nexuses.clear();
        self.nexuses.populate(nexuses);
    }

    /// Returns a vector of nexus states.
    pub(crate) fn get_nexus_states(&self) -> Vec<NexusState> {
        Self::cloned_inner_states(self.nexuses.to_vec())
    }

    /// Returns a vector of pool states.
    pub(crate) fn get_pool_states(&self) -> Vec<PoolState> {
        Self::cloned_inner_states(self.pools.to_vec())
    }

    /// Returns a vector of replica states.
    pub(crate) fn get_replica_states(&self) -> Vec<ReplicaState> {
        Self::cloned_inner_states(self.replicas.to_vec())
    }

    /// Takes a vector of resources protected by an 'Arc' and 'Mutex' and returns a vector of
    /// unprotected resources.
    fn cloned_inner_states<S>(locked_states: Vec<Arc<Mutex<S>>>) -> Vec<S>
    where
        S: Clone,
    {
        locked_states.iter().map(|s| s.lock().clone()).collect()
    }
}
