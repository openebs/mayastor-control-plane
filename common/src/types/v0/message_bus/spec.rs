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
        fn vec_to_vec<F: Clone, T: From<F>>(from: Vec<F>) -> Vec<T> {
            from.iter().cloned().map(From::from).collect()
        }
        Self::new(
            vec_to_vec(src.nexuses),
            vec_to_vec(src.pools),
            vec_to_vec(src.replicas),
            vec_to_vec(src.volumes),
        )
    }
}
// impl From<models::Specs> for Specs {
//     fn from(src: models::Specs) -> Self {
//         fn vec_to_vec<F: Clone, T: From<F>>(from: Vec<F>) -> Vec<T> {
//             from.iter().cloned().map(From::from).collect()
//         }
//         Self {
//             nexuses: vec_to_vec(src.nexuses),
//             pools: vec_to_vec(src.pools),
//             replicas: vec_to_vec(src.replicas),
//             volumes: vec_to_vec(src.volumes),
//         }
//     }
// }
