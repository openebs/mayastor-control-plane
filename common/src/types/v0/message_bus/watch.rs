use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

///
/// Watcher Agent

/// Create new Resource Watch
/// Uniquely identifiable by resource_id and callback
pub type CreateWatch = Watch;

/// Watch Resource in the store
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Watch {
    /// id of the resource to watch on
    pub id: WatchResourceId,
    /// callback used to notify the watcher of a change
    pub callback: WatchCallback,
    /// type of watch
    pub watch_type: WatchType,
}

/// Get Resource Watches
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetWatchers {
    /// id of the resource to get
    pub resource: WatchResourceId,
}

/// Uniquely Identify a Resource
pub type Resource = WatchResourceId;

/// The different resource types that can be watched
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub enum WatchResourceId {
    /// nodes
    Node(NodeId),
    /// pools
    Pool(PoolId),
    /// replicas
    Replica(ReplicaId),
    /// replica state
    ReplicaState(ReplicaId),
    /// replica spec
    ReplicaSpec(ReplicaId),
    /// nexuses
    Nexus(NexusId),
    /// volumes
    Volume(VolumeId),
}
impl Default for WatchResourceId {
    fn default() -> Self {
        Self::Node(Default::default())
    }
}
impl ToString for WatchResourceId {
    fn to_string(&self) -> String {
        match self {
            WatchResourceId::Node(id) => format!("nodes/{}", id.to_string()),
            WatchResourceId::Pool(id) => format!("pools/{}", id.to_string()),
            WatchResourceId::Replica(id) => {
                format!("replicas/{}", id.to_string())
            }
            WatchResourceId::ReplicaState(id) => {
                format!("replicas_state/{}", id.to_string())
            }
            WatchResourceId::ReplicaSpec(id) => {
                format!("replicas_spec/{}", id.to_string())
            }
            WatchResourceId::Nexus(id) => format!("nexuses/{}", id.to_string()),
            WatchResourceId::Volume(id) => format!("volumes/{}", id.to_string()),
        }
    }
}

/// The difference types of watches
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum WatchType {
    /// Watch for changes on the desired state
    Desired,
    /// Watch for changes on the actual state
    Actual,
    /// Watch for both `Desired` and `Actual` changes
    All,
}
impl Default for WatchType {
    fn default() -> Self {
        Self::All
    }
}

/// Delete Watch which was previously created by CreateWatcher
/// Fields should match the ones used for the creation
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeleteWatch {
    /// id of the resource to delete the watch from
    pub id: WatchResourceId,
    /// callback to be deleted
    pub callback: WatchCallback,
    /// type of watch to be deleted
    pub watch_type: WatchType,
}

/// Watcher Callback types
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum WatchCallback {
    /// HTTP URI callback
    Uri(String),
}
impl Default for WatchCallback {
    fn default() -> Self {
        Self::Uri(Default::default())
    }
}
