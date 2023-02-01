use crate::controller::resources::{resource_map::ResourceMap, ResourceMutex};
use common_lib::types::v0::{
    store::{nexus::NexusState, pool::PoolState, replica::ReplicaState},
    transport::{self, Nexus, NexusId, PoolId, Replica, ReplicaId},
};

use indexmap::map::Values;
use parking_lot::RwLock;
use std::{ops::Deref, sync::Arc};

/// Locked Resource States.
#[derive(Clone, Default, Debug)]
pub(crate) struct ResourceStatesLocked(Arc<RwLock<ResourceStates>>);

impl ResourceStatesLocked {
    /// Return a new empty `Self`.
    pub(crate) fn new() -> Self {
        Default::default()
    }
}

impl Deref for ResourceStatesLocked {
    type Target = Arc<RwLock<ResourceStates>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Resource States.
#[derive(Default, Debug)]
pub(crate) struct ResourceStates {
    nexuses: ResourceMap<NexusId, NexusState>,
    pools: ResourceMap<PoolId, PoolState>,
    replicas: ResourceMap<ReplicaId, ReplicaState>,
}

impl ResourceStates {
    /// Update the various resource states.
    pub(crate) fn update(
        &mut self,
        pools: Vec<transport::PoolState>,
        replicas: Vec<Replica>,
        nexuses: Vec<Nexus>,
    ) {
        self.update_replicas(replicas);
        self.update_pools(pools);
        self.update_nexuses(nexuses);
    }

    /// Update nexus states.
    pub(crate) fn update_nexuses(&mut self, nexuses: Vec<Nexus>) {
        self.nexuses.clear();
        self.nexuses.populate(nexuses);
    }

    /// Update nexus state.
    pub(crate) fn update_nexus(&mut self, nexus: Nexus) {
        self.nexuses.insert(nexus.into());
    }

    /// Returns a vector of cloned nexus states.
    pub(crate) fn nexus_states_cloned(&self) -> Vec<NexusState> {
        Self::cloned_inner_states(self.nexuses.values())
    }

    /// Returns an iterator of nexus states.
    pub(crate) fn nexus_states(&self) -> Values<NexusId, ResourceMutex<NexusState>> {
        self.nexuses.values()
    }

    /// Returns the nexus state for the nexus with the given ID.
    pub(crate) fn nexus_state(&self, id: &NexusId) -> Option<NexusState> {
        self.nexuses.get(id).map(|state| state.lock().clone())
    }

    /// Update pool states.
    pub(crate) fn update_pools(&mut self, pools: Vec<transport::PoolState>) {
        self.pools.clear();
        self.pools.populate(pools);
    }

    /// Returns a vector of cloned pool states.
    pub(crate) fn pool_states_cloned(&self) -> Vec<PoolState> {
        Self::cloned_inner_states(self.pools.values())
    }

    /// Returns an iterator of pool states.
    pub(crate) fn pool_states(&self) -> Values<PoolId, ResourceMutex<PoolState>> {
        self.pools.values()
    }

    /// Get a pool with the given ID.
    pub(crate) fn pool_state(&self, id: &PoolId) -> Option<PoolState> {
        let pool_state = self.pools.get(id)?;
        Some(pool_state.lock().clone())
    }

    /// Update replica states.
    pub(crate) fn update_replicas(&mut self, replicas: Vec<Replica>) {
        self.replicas.clear();
        self.replicas.populate(replicas);
    }

    /// Returns a vector of cloned replica states.
    pub(crate) fn replica_states_cloned(&self) -> Vec<ReplicaState> {
        Self::cloned_inner_states(self.replicas.values())
    }

    /// Returns an iterator of replica states.
    pub(crate) fn replica_states(&self) -> Values<ReplicaId, ResourceMutex<ReplicaState>> {
        self.replicas.values()
    }

    /// Get a replica with the given ID.
    pub(crate) fn replica_state(&self, id: &ReplicaId) -> Option<&ResourceMutex<ReplicaState>> {
        self.replicas.get(id)
    }

    /// Clear all state information.
    pub(crate) fn clear_all(&mut self) {
        self.nexuses.clear();
        self.pools.clear();
        self.replicas.clear();
    }

    /// Takes an iterator of resources resourced by an 'Arc' and 'Mutex' and returns a vector of
    /// unprotected resources.
    fn cloned_inner_states<I, S>(locked_states: Values<I, ResourceMutex<S>>) -> Vec<S>
    where
        S: Clone,
    {
        locked_states
            .into_iter()
            .map(|s| s.lock().clone())
            .collect()
    }
}
