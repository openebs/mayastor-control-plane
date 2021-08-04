use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::types::v0::store::node::NodeSpec;
use strum_macros::{EnumString, ToString};

/// Registration
///
/// Register message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Register {
    /// id of the mayastor instance
    pub id: NodeId,
    /// grpc endpoint of the mayastor instance
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
/// Get storage nodes by filter
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetNodes {
    filter: Filter,
}
impl GetNodes {
    /// Return `Self` to request all nodes (`None`) or a specific node (`NodeId`)
    pub fn from(node_id: impl Into<Option<NodeId>>) -> Self {
        let node_id = node_id.into();
        Self {
            filter: node_id.map_or(Filter::None, Filter::Node),
        }
    }
    /// Get the inner `Filter`
    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}

/// Node information
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    /// Node identification
    id: NodeId,
    /// Specification of the node.
    spec: Option<NodeSpec>,
    /// Runtime state of the node.
    state: Option<NodeState>,
}
impl Node {
    /// Get new `Self` from the given parameters
    pub fn new(id: NodeId, spec: Option<NodeSpec>, state: Option<NodeState>) -> Self {
        Self { id, spec, state }
    }
    /// Get the node specification
    pub fn spec(&self) -> Option<&NodeSpec> {
        self.spec.as_ref()
    }
    /// Get the node runtime state
    pub fn state(&self) -> Option<&NodeState> {
        self.state.as_ref()
    }
}

impl From<models::Node> for Node {
    fn from(src: models::Node) -> Self {
        Self {
            id: src.id.into(),
            spec: src.spec.map(Into::into),
            state: src.state.map(Into::into),
        }
    }
}
impl From<Node> for models::Node {
    fn from(src: Node) -> Self {
        Self::new_all(src.id, src.spec.map(Into::into), src.state.map(Into::into))
    }
}

/// Status of the Node
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq)]
pub enum NodeStatus {
    /// Node has unexpectedly disappeared
    Unknown,
    /// Node is deemed online if it has not missed the
    /// registration keep alive deadline
    Online,
    /// Node is deemed offline if has missed the
    /// registration keep alive deadline
    Offline,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Node State information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeState {
    /// id of the mayastor instance
    pub id: NodeId,
    /// grpc_endpoint of the mayastor instance
    pub grpc_endpoint: String,
    /// deemed status of the node
    pub status: NodeStatus,
}
impl NodeState {
    /// Return a new `Self`
    pub fn new(id: NodeId, grpc_endpoint: String, status: NodeStatus) -> Self {
        Self {
            id,
            grpc_endpoint,
            status,
        }
    }
}

impl From<models::NodeState> for NodeState {
    fn from(src: models::NodeState) -> Self {
        Self {
            id: src.id.into(),
            grpc_endpoint: src.grpc_endpoint,
            status: src.status.into(),
        }
    }
}

bus_impl_string_id!(NodeId, "ID of a mayastor node");

impl From<NodeState> for models::NodeState {
    fn from(src: NodeState) -> Self {
        Self::new(src.grpc_endpoint, src.id, src.status)
    }
}
impl From<&NodeState> for models::NodeState {
    fn from(src: &NodeState) -> Self {
        let src = src.clone();
        Self::new(src.grpc_endpoint, src.id, src.status)
    }
}

impl From<NodeStatus> for models::NodeStatus {
    fn from(src: NodeStatus) -> Self {
        match src {
            NodeStatus::Unknown => Self::Unknown,
            NodeStatus::Online => Self::Online,
            NodeStatus::Offline => Self::Offline,
        }
    }
}
impl From<models::NodeStatus> for NodeStatus {
    fn from(src: models::NodeStatus) -> Self {
        match src {
            models::NodeStatus::Unknown => Self::Unknown,
            models::NodeStatus::Online => Self::Online,
            models::NodeStatus::Offline => Self::Offline,
        }
    }
}
