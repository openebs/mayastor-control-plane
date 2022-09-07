use super::*;

use serde::{Deserialize, Serialize};
use std::{fmt::Debug, str::FromStr};

use crate::types::v0::store::node::NodeSpec;
use strum_macros::{EnumString, ToString};

/// Registration
///
/// Register message payload
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Register {
    /// Node Id of the io-engine instance.
    pub id: NodeId,
    /// Grpc endpoint of the io-engine instance.
    pub grpc_endpoint: std::net::SocketAddr,
    /// Api versions registered by the dataplane.
    pub api_versions: Option<Vec<ApiVersion>>,
}

/// Deregister message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Deregister {
    /// Node Id of the io-engine instance.
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
    /// New get nodes request
    pub fn new(filter: Filter) -> Self {
        Self { filter }
    }
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
    /// Get the node id
    pub fn id(&self) -> &NodeId {
        &self.id
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
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeState {
    /// Node Id of the io-engine instance.
    pub id: NodeId,
    /// Grpc endpoint of the io-engine instance
    pub grpc_endpoint: std::net::SocketAddr,
    /// Deemed status of the node
    pub status: NodeStatus,
    /// Api versions supported by the dataplane.
    pub api_versions: Option<Vec<ApiVersion>>,
}

impl NodeState {
    /// Return a new `Self`
    pub fn new(
        id: NodeId,
        grpc_endpoint: std::net::SocketAddr,
        status: NodeStatus,
        api_versions: Option<Vec<ApiVersion>>,
    ) -> Self {
        Self {
            id,
            grpc_endpoint,
            status,
            api_versions,
        }
    }
    /// Get the node identification
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    /// Get the node status
    pub fn status(&self) -> &NodeStatus {
        &self.status
    }
}
impl From<&Register> for NodeState {
    fn from(src: &Register) -> Self {
        Self::from(src.clone())
    }
}
impl From<Register> for NodeState {
    fn from(src: Register) -> Self {
        Self {
            id: src.id,
            grpc_endpoint: src.grpc_endpoint,
            status: NodeStatus::Online,
            api_versions: src.api_versions,
        }
    }
}

rpc_impl_string_id!(NodeId, "ID of a node");

impl From<NodeState> for models::NodeState {
    fn from(src: NodeState) -> Self {
        Self::new(src.grpc_endpoint.to_string(), src.id, src.status)
    }
}
impl From<&NodeState> for models::NodeState {
    fn from(src: &NodeState) -> Self {
        Self::from(src.clone())
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

/// api versions known by control plane
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum ApiVersion {
    V0,
    V1,
}

impl FromStr for ApiVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v0" => Ok(Self::V0),
            "v1" => Ok(Self::V1),
            _ => Err(format!("The api version: {} is not supported", s)),
        }
    }
}

impl Default for ApiVersion {
    fn default() -> Self {
        Self::V0
    }
}
