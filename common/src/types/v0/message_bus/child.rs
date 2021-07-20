use super::*;

use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug};

/// Child information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Child {
    /// uri of the child device
    pub uri: ChildUri,
    /// state of the child
    pub state: ChildState,
    /// current rebuild progress (%)
    pub rebuild_progress: Option<i32>,
}

impl From<Child> for models::Child {
    fn from(src: Child) -> Self {
        Self {
            rebuild_progress: src.rebuild_progress,
            state: src.state.into(),
            uri: src.uri.into(),
        }
    }
}
impl From<models::Child> for Child {
    fn from(src: models::Child) -> Self {
        Self {
            uri: src.uri.into(),
            state: src.state.into(),
            rebuild_progress: src.rebuild_progress,
        }
    }
}

bus_impl_string_id_percent_decoding!(ChildUri, "URI of a mayastor nexus child");

impl PartialEq<Child> for ChildUri {
    fn eq(&self, other: &Child) -> bool {
        self == &other.uri
    }
}
impl PartialEq<String> for ChildUri {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

/// Child State information
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum ChildState {
    /// Default Unknown state
    Unknown = 0,
    /// healthy and contains the latest bits
    Online = 1,
    /// rebuild is in progress (or other recoverable error)
    Degraded = 2,
    /// unrecoverable error (control plane must act)
    Faulted = 3,
}
impl PartialOrd for ChildState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match &self {
            ChildState::Unknown => match &other {
                ChildState::Unknown => None,
                ChildState::Online => Some(Ordering::Less),
                ChildState::Degraded => None,
                ChildState::Faulted => None,
            },
            ChildState::Online => match &other {
                ChildState::Unknown => Some(Ordering::Greater),
                ChildState::Online => Some(Ordering::Equal),
                ChildState::Degraded => Some(Ordering::Greater),
                ChildState::Faulted => Some(Ordering::Greater),
            },
            ChildState::Degraded => match &other {
                ChildState::Unknown => None,
                ChildState::Online => Some(Ordering::Less),
                ChildState::Degraded => Some(Ordering::Equal),
                ChildState::Faulted => Some(Ordering::Greater),
            },
            ChildState::Faulted => match &other {
                ChildState::Unknown => None,
                ChildState::Online => Some(Ordering::Less),
                ChildState::Degraded => Some(Ordering::Less),
                ChildState::Faulted => Some(Ordering::Equal),
            },
        }
    }
}

impl Default for ChildState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for ChildState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}
impl From<ChildState> for models::ChildState {
    fn from(src: ChildState) -> Self {
        match src {
            ChildState::Unknown => Self::Unknown,
            ChildState::Online => Self::Online,
            ChildState::Degraded => Self::Degraded,
            ChildState::Faulted => Self::Faulted,
        }
    }
}
impl From<models::ChildState> for ChildState {
    fn from(src: models::ChildState) -> Self {
        match src {
            models::ChildState::Unknown => Self::Unknown,
            models::ChildState::Online => Self::Online,
            models::ChildState::Degraded => Self::Degraded,
            models::ChildState::Faulted => Self::Faulted,
        }
    }
}

/// Remove Child from Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveNexusChild {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// URI of the child device to be removed
    pub uri: ChildUri,
}

impl From<AddNexusChild> for RemoveNexusChild {
    fn from(add: AddNexusChild) -> Self {
        Self {
            node: add.node,
            nexus: add.nexus,
            uri: add.uri,
        }
    }
}

/// Add child to Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AddNexusChild {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// URI of the child device to be added
    pub uri: ChildUri,
    /// auto start rebuilding
    pub auto_rebuild: bool,
}
