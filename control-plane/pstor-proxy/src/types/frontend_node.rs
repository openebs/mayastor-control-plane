use crate::v1;
use pstor::{ApiVersion, ObjectKey, StorableObject, StorableObjectType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Helper to convert from Vec<F> into Vec<T>.
pub trait IntoVec<T>: Sized {
    /// Performs the conversion.
    fn into_vec(self) -> Vec<T>;
}

/// Frontend node definition.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrontendNode {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the csi-node instance (gRPC).
    pub spec: FrontendNodeSpec,
    /// Node labels.
    pub state: Option<FrontendNodeState>,
}

impl FrontendNode {
    pub fn new(spec: FrontendNodeSpec, state: Option<FrontendNodeState>) -> Self {
        Self {
            id: spec.id.clone(),
            spec,
            state,
        }
    }
}

/// Frontend node spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrontendNodeSpec {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the frontend instance.
    pub endpoint: std::net::SocketAddr,
    /// Node labels.
    pub labels: FrontendNodeLabels,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrontendNodeState {}

impl FrontendNodeSpec {
    pub fn new(
        id: FrontendNodeId,
        endpoint: std::net::SocketAddr,
        labels: FrontendNodeLabels,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
        }
    }
}

/// Multiple Frontend nodes.
#[derive(Default, Debug, Clone)]
pub struct FrontendNodes {
    /// Vector of entries
    pub entries: Vec<FrontendNode>,
    /// The token to use in subsequent requests.
    pub next_token: Option<u64>,
}

impl IntoVec<FrontendNodeSpec> for Vec<FrontendNodeSpec> {
    fn into_vec(self) -> Vec<FrontendNodeSpec> {
        self
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct FrontendNodeId(String);

/// Frontend node labels
pub type FrontendNodeLabels = HashMap<String, String>;

/// Key used by the store to uniquely identify a VolumeSnapshot.
pub struct FrontendNodeKey(FrontendNodeId);

impl std::fmt::Display for FrontendNodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(::core::format_args!("{}", self.0))
    }
}
impl From<&FrontendNodeId> for FrontendNodeKey {
    fn from(id: &FrontendNodeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for FrontendNodeKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::FrontendNodeSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for FrontendNodeSpec {
    type Key = FrontendNodeKey;

    fn key(&self) -> Self::Key {
        FrontendNodeKey(self.id.clone())
    }
}

impl std::fmt::Display for FrontendNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(::core::format_args!("{}", self.0))
    }
}
impl FrontendNodeId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
impl From<&str> for FrontendNodeId {
    fn from(id: &str) -> Self {
        FrontendNodeId::from(id)
    }
}
impl From<String> for FrontendNodeId {
    fn from(id: String) -> Self {
        FrontendNodeId::from(id.as_str())
    }
}
impl From<&FrontendNodeId> for FrontendNodeId {
    fn from(id: &FrontendNodeId) -> FrontendNodeId {
        id.clone()
    }
}
impl From<FrontendNodeId> for Option<String> {
    fn from(id: FrontendNodeId) -> Option<String> {
        Some(id.to_string())
    }
}
impl From<FrontendNodeId> for String {
    fn from(id: FrontendNodeId) -> String {
        id.to_string()
    }
}
impl From<&FrontendNodeId> for String {
    fn from(id: &FrontendNodeId) -> String {
        id.to_string()
    }
}
impl Default for FrontendNodeId {
    fn default() -> Self {
        FrontendNodeId(uuid::Uuid::default().to_string())
    }
}
impl FrontendNodeId {
    pub fn from<T: Into<String>>(id: T) -> Self {
        FrontendNodeId(id.into())
    }
    pub fn new() -> Self {
        FrontendNodeId(uuid::Uuid::new_v4().to_string())
    }
}
impl std::ops::Deref for FrontendNodeId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterFrontendNode {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the csi-node instance (gRPC).
    pub endpoint: std::net::SocketAddr,
}

impl RegisterFrontendNode {
    pub fn new(id: FrontendNodeId, endpoint: std::net::SocketAddr) -> Self {
        Self { id, endpoint }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeregisterFrontendNode {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
}

impl DeregisterFrontendNode {
    pub fn new(id: FrontendNodeId) -> Self {
        Self { id }
    }
}

impl From<DeregisterFrontendNode> for FrontendNodeKey {
    fn from(value: DeregisterFrontendNode) -> Self {
        Self(value.id)
    }
}

impl From<FrontendNodeSpec> for v1::frontend_node::FrontendNodeSpec {
    fn from(frontend_node_spec: FrontendNodeSpec) -> Self {
        v1::frontend_node::FrontendNodeSpec {
            id: frontend_node_spec.id.to_string(),
            grpc_endpoint: frontend_node_spec.endpoint.to_string(),
            labels: None,
        }
    }
}

impl From<FrontendNodeState> for v1::frontend_node::FrontendNodeState {
    fn from(_frontend_node_state: FrontendNodeState) -> Self {
        v1::frontend_node::FrontendNodeState {}
    }
}

impl From<FrontendNode> for v1::frontend_node::FrontendNode {
    fn from(frontend_node: FrontendNode) -> Self {
        v1::frontend_node::FrontendNode {
            id: frontend_node.id.to_string(),
            spec: Some(frontend_node.spec.into()),
            state: frontend_node.state.map(|state| state.into()),
        }
    }
}

impl From<FrontendNodes> for v1::frontend_node::FrontendNodes {
    fn from(frontend_nodes: FrontendNodes) -> Self {
        v1::frontend_node::FrontendNodes {
            entries: frontend_nodes
                .entries
                .iter()
                .map(|frontend_node| frontend_node.clone().into())
                .collect(),
            next_token: frontend_nodes.next_token,
        }
    }
}
