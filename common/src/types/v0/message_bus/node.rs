use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use strum_macros::{EnumString, ToString};

/// Registration
///
/// Register message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Register {
    /// id of the mayastor instance
    pub id: NodeId,
    /// grpc_endpoint of the mayastor instance
    pub grpc_endpoint: String,
}

/// Deregister message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Deregister {
    /// id of the mayastor instance
    pub id: NodeId,
}

/// Node Service
///
/// Get all the nodes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetNodes {}

/// State of the Node
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq)]
pub enum NodeState {
    /// Node has unexpectedly disappeared
    Unknown,
    /// Node is deemed online if it has not missed the
    /// registration keep alive deadline
    Online,
    /// Node is deemed offline if has missed the
    /// registration keep alive deadline
    Offline,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Node information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    /// id of the mayastor instance
    pub id: NodeId,
    /// grpc_endpoint of the mayastor instance
    pub grpc_endpoint: String,
    /// deemed state of the node
    pub state: NodeState,
}

impl From<models::Node> for Node {
    fn from(src: models::Node) -> Self {
        Self {
            id: src.id.into(),
            grpc_endpoint: src.grpc_endpoint,
            state: src.state.into(),
        }
    }
}

bus_impl_string_id!(NodeId, "ID of a mayastor node");

impl From<Node> for models::Node {
    fn from(src: Node) -> Self {
        Self::new(src.grpc_endpoint, src.id, src.state)
    }
}
impl From<&Node> for models::Node {
    fn from(src: &Node) -> Self {
        let src = src.clone();
        Self::new(src.grpc_endpoint, src.id, src.state)
    }
}

impl From<NodeState> for models::NodeState {
    fn from(src: NodeState) -> Self {
        match src {
            NodeState::Unknown => Self::Unknown,
            NodeState::Online => Self::Online,
            NodeState::Offline => Self::Offline,
        }
    }
}
impl From<models::NodeState> for NodeState {
    fn from(src: models::NodeState) -> Self {
        match src {
            models::NodeState::Unknown => Self::Unknown,
            models::NodeState::Online => Self::Online,
            models::NodeState::Offline => Self::Offline,
        }
    }
}
