use crate::types::v0::store::{nexus, pool, replica};
use serde::{Deserialize, Serialize};

/// Retrieve all states from core agent
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetStates {}

/// Runtime state of the resources.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct States {
    /// nexus states
    pub nexuses: Vec<nexus::NexusState>,
    /// pool states
    pub pools: Vec<pool::PoolState>,
    /// replica states
    pub replicas: Vec<replica::ReplicaState>,
}
