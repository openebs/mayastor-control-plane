use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::*;
use crate::types::v0::store::{nexus, pool, replica, volume};

/// Retrieve all specs from core agent
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetSpecs {}

/// Specs detailing the requested configuration of the objects.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Specs {
    /// volume specs
    pub volumes: Vec<volume::VolumeSpec>,
    /// nexus specs
    pub nexuses: Vec<nexus::NexusSpec>,
    /// pool specs
    pub pools: Vec<pool::PoolSpec>,
    /// replica specs
    pub replicas: Vec<replica::ReplicaSpec>,
}

impl From<Specs> for models::Specs {
    fn from(src: Specs) -> Self {
        Self::new(src.nexuses, src.pools, src.replicas, src.volumes)
    }
}
