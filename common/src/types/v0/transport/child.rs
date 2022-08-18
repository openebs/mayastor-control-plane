use super::*;

use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug, str::FromStr};

/// Child information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Child {
    /// Uri of the child device.
    pub uri: ChildUri,
    /// State of the child.
    pub state: ChildState,
    /// Current rebuild progress (%).
    pub rebuild_progress: Option<u8>,
    /// Reason for the child state.
    pub state_reason: ChildStateReason,
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

bus_impl_string_id_percent_decoding!(ChildUri, "URI of a nexus child");

impl ChildUri {
    /// Get the io-engine bdev uuid from the ChildUri
    pub fn uuid_str(&self) -> Option<String> {
        match url::Url::from_str(self.as_str()) {
            Ok(url) => {
                let uuid = url.query_pairs().find(|(name, _)| name == "uuid");
                uuid.map(|(_, uuid)| uuid.to_string())
            }
            Err(_) => None,
        }
    }
}
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
    Unknown,
    /// healthy and contains the latest bits
    Online,
    /// rebuild is in progress (or other recoverable error)
    Degraded,
    /// unrecoverable error (control plane must act)
    Faulted,
}
impl ChildState {
    /// Check if the child is `Faulted`
    pub fn faulted(&self) -> bool {
        self == &Self::Faulted
    }
}
impl PartialOrd for ChildState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match &self {
            ChildState::Unknown => match &other {
                ChildState::Unknown => Some(Ordering::Equal),
                ChildState::Online => Some(Ordering::Less),
                ChildState::Degraded => Some(Ordering::Less),
                ChildState::Faulted => Some(Ordering::Greater),
            },
            ChildState::Online => match &other {
                ChildState::Unknown => Some(Ordering::Greater),
                ChildState::Online => Some(Ordering::Equal),
                ChildState::Degraded => Some(Ordering::Greater),
                ChildState::Faulted => Some(Ordering::Greater),
            },
            ChildState::Degraded => match &other {
                ChildState::Unknown => Some(Ordering::Greater),
                ChildState::Online => Some(Ordering::Less),
                ChildState::Degraded => Some(Ordering::Equal),
                ChildState::Faulted => Some(Ordering::Greater),
            },
            ChildState::Faulted => match &other {
                ChildState::Unknown => Some(Ordering::Less),
                ChildState::Online => Some(Ordering::Less),
                ChildState::Degraded => Some(Ordering::Less),
                ChildState::Faulted => Some(Ordering::Equal),
            },
        }
    }
}

/// Child State Reason information.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub enum ChildStateReason {
    /// No particular reason for the child to be in this state.
    /// This is typically the init state.
    #[default]
    Unknown,
    /// Child is being initialized.
    Init,
    /// Child is being destroyed or has been closed.
    Closed,
    /// Invalid child device configuration (e.g. mismatching size).
    ConfigInvalid,
    /// Out of sync: child device is ok, but needs to be rebuilt.
    OutOfSync,
    /// Thin-provisioned child failed a write operate because
    /// the underlying logical volume failed to allocate space.
    /// This a recoverable state in case when addtional space
    /// can be freed from the logical volume store.
    NoSpace,
    /// The underlying device timed out.
    /// This a recoverable state in case the device can be expected
    /// to come back online.
    TimedOut,
    /// Cannot open device.
    CantOpen,
    /// The child failed to rebuild successfully.
    RebuildFailed,
    /// The child has been faulted due to I/O error(s).
    IoError,
    /// The child has been explicitly faulted due to an RPC call.
    ByClient,
    /// Admin command failure.
    AdminCommandFailed,
}

impl Default for ChildState {
    fn default() -> Self {
        Self::Unknown
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

/// Remove Child from Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveNexusChild {
    /// id of the io-engine instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// URI of the child device to be removed
    pub uri: ChildUri,
}
impl RemoveNexusChild {
    /// Return new `Self`
    pub fn new(node: &NodeId, nexus: &NexusId, uri: &ChildUri) -> Self {
        Self {
            node: node.clone(),
            nexus: nexus.clone(),
            uri: uri.clone(),
        }
    }
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
    /// id of the io-engine instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// URI of the child device to be added
    pub uri: ChildUri,
    /// auto start rebuilding
    pub auto_rebuild: bool,
}
