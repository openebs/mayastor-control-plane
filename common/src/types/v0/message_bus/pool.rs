use super::*;

use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug};

use std::ops::Deref;
use strum_macros::{EnumString, ToString};

/// Pool Service
/// Get all the pools from specific node or None for all nodes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetPools {
    /// Filter request
    pub filter: Filter,
}

/// State of the Pool
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq)]
pub enum PoolState {
    /// unknown state
    Unknown = 0,
    /// the pool is in normal working order
    Online = 1,
    /// the pool has experienced a failure but can still function
    Degraded = 2,
    /// the pool is completely inaccessible
    Faulted = 3,
}

impl Default for PoolState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for PoolState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}

/// Pool information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
    /// current state of the pool
    pub state: PoolState,
    /// size of the pool in bytes
    pub capacity: u64,
    /// used bytes from the pool
    pub used: u64,
}

bus_impl_string_id!(PoolId, "ID of a mayastor pool");

// online > degraded > unknown/faulted
impl PartialOrd for PoolState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            PoolState::Unknown => match other {
                PoolState::Unknown => None,
                PoolState::Online => Some(Ordering::Less),
                PoolState::Degraded => Some(Ordering::Less),
                PoolState::Faulted => None,
            },
            PoolState::Online => match other {
                PoolState::Unknown => Some(Ordering::Greater),
                PoolState::Online => Some(Ordering::Equal),
                PoolState::Degraded => Some(Ordering::Greater),
                PoolState::Faulted => Some(Ordering::Greater),
            },
            PoolState::Degraded => match other {
                PoolState::Unknown => Some(Ordering::Greater),
                PoolState::Online => Some(Ordering::Less),
                PoolState::Degraded => Some(Ordering::Equal),
                PoolState::Faulted => Some(Ordering::Greater),
            },
            PoolState::Faulted => match other {
                PoolState::Unknown => None,
                PoolState::Online => Some(Ordering::Less),
                PoolState::Degraded => Some(Ordering::Less),
                PoolState::Faulted => Some(Ordering::Equal),
            },
        }
    }
}

/// Pool device URI
/// Can be specified in the form of a file path or a URI
/// eg: /dev/sda, aio:///dev/sda, malloc:///disk?size_mb=100
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PoolDeviceUri(String);
impl Deref for PoolDeviceUri {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Default for PoolDeviceUri {
    fn default() -> Self {
        Self("malloc:///disk?size_mb=100".into())
    }
}
impl From<&str> for PoolDeviceUri {
    fn from(device: &str) -> Self {
        Self(device.to_string())
    }
}
impl From<&String> for PoolDeviceUri {
    fn from(device: &String) -> Self {
        Self(device.clone())
    }
}
impl From<String> for PoolDeviceUri {
    fn from(device: String) -> Self {
        Self(device)
    }
}

/// Create Pool Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreatePool {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
}

/// Destroy Pool Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyPool {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
}
