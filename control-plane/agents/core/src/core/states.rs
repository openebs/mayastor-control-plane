use common_lib::types::v0::{
    message_bus::{Nexus, NexusId, Pool, PoolId, Replica, ReplicaId, UuidString},
    store::{nexus::NexusState, pool::PoolState, replica::ReplicaState},
};
use std::{collections::HashMap, hash::Hash, ops::Deref, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

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
    nexuses: HashMap<NexusId, Arc<Mutex<NexusState>>>,
    pools: HashMap<PoolId, Arc<Mutex<PoolState>>>,
    replicas: HashMap<ReplicaId, Arc<Mutex<ReplicaState>>>,
}

impl ResourceStates {
    /// Update the states of the resources.
    pub(crate) async fn update(
        &mut self,
        pools: Vec<Pool>,
        replicas: Vec<Replica>,
        nexuses: Vec<Nexus>,
    ) {
        Self::update_resource(&mut self.pools, pools).await;
        Self::update_resource(&mut self.replicas, replicas).await;
        Self::update_resource(&mut self.nexuses, nexuses).await;
    }

    /// Returns a vector of nexus states.
    pub(crate) async fn get_nexus_states(&self) -> Vec<NexusState> {
        Self::states_vector(&self.nexuses).await
    }

    /// Returns a vector of pool states.
    pub(crate) async fn get_pool_states(&self) -> Vec<PoolState> {
        Self::states_vector(&self.pools).await
    }

    /// Returns a vector of replica states.
    pub(crate) async fn get_replica_states(&self) -> Vec<ReplicaState> {
        Self::states_vector(&self.replicas).await
    }

    /// Update the state of the resources with the latest runtime state.
    /// If a runtime state of a resource is not provided, the resource is removed from the list.
    async fn update_resource<I, S, R>(
        resource_states: &mut HashMap<I, Arc<Mutex<S>>>,
        runtime_state: Vec<R>,
    ) where
        I: From<String> + Eq + Hash,
        R: UuidString,
        S: From<R>,
    {
        resource_states.clear();
        for state in runtime_state {
            let uuid = state.uuid_as_string().into();
            let resource_state = state.into();
            match resource_states.get(&uuid) {
                Some(locked_state) => {
                    debug!("Updating {}", std::any::type_name::<S>());
                    let mut state = locked_state.lock().await;
                    *state = resource_state;
                }
                None => {
                    resource_states.insert(uuid, Arc::new(Mutex::new(resource_state)));
                }
            }
        }
    }

    /// Returns a vector of states.
    async fn states_vector<I, S>(resource_states: &HashMap<I, Arc<Mutex<S>>>) -> Vec<S>
    where
        S: Clone,
    {
        let mut states = vec![];
        for nexus_state in resource_states.values() {
            let object = nexus_state.lock().await;
            states.push(object.clone());
        }
        states
    }
}
