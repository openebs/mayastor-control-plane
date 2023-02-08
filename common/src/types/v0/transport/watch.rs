use super::*;

use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt::Debug};

///
/// Watch Agent

/// Create new Resource Watch
/// Uniquely identifiable by resource_id and callback
pub type CreateWatch = Watch;

/// watch Resource in the store
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Watch {
    /// id of the resource to watch on
    pub id: WatchResourceId,
    /// callback used to notify the watch of a change
    pub callback: WatchCallback,
    /// type of watch
    pub watch_type: WatchType,
}

impl TryFrom<&Watch> for models::RestWatch {
    type Error = ();
    fn try_from(value: &Watch) -> Result<Self, Self::Error> {
        match &value.callback {
            WatchCallback::Uri(uri) => Ok(Self {
                resource: value.id.to_string(),
                callback: uri.to_string(),
            }),
            /* other types are not implemented yet and should map to an error
             * _ => Err(()), */
        }
    }
}

/// Get Resource Watches
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetWatches {
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
            WatchResourceId::Node(id) => format!("nodes/{id}"),
            WatchResourceId::Pool(id) => format!("pools/{id}"),
            WatchResourceId::Replica(id) => {
                format!("replicas/{id}")
            }
            WatchResourceId::ReplicaState(id) => {
                format!("replicas_state/{id}")
            }
            WatchResourceId::ReplicaSpec(id) => {
                format!("replicas_spec/{id}")
            }
            WatchResourceId::Nexus(id) => format!("nexuses/{id}"),
            WatchResourceId::Volume(id) => format!("volumes/{id}"),
        }
    }
}

/// The difference types of watches
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum WatchType {
    /// watch for changes on the desired state
    Desired,
    /// watch for changes on the actual state
    Actual,
    /// watch for both `Desired` and `Actual` changes
    All,
}
impl Default for WatchType {
    fn default() -> Self {
        Self::All
    }
}

/// Delete watch which was previously created by CreateWatch
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

/// Watch Callback types
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
