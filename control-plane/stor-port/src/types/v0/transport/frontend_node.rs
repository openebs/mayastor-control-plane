use crate::{
    rpc_impl_string_id, rpc_impl_string_id_inner,
    types::v0::{
        store::frontend_node::{FrontendNodeLabels, FrontendNodeSpec, FrontendNodeSpecKey},
        transport::Filter,
    },
};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

rpc_impl_string_id!(FrontendNodeId, "ID of a frontend node");

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterFrontendNode {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the csi-node instance (gRPC).
    pub endpoint: std::net::SocketAddr,
    /// Frontend Node labels.
    pub labels: Option<FrontendNodeLabels>,
}

impl RegisterFrontendNode {
    pub fn new(
        id: FrontendNodeId,
        endpoint: std::net::SocketAddr,
        labels: Option<FrontendNodeLabels>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeregisterFrontendNode {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
}

impl From<DeregisterFrontendNode> for FrontendNodeSpecKey {
    fn from(value: DeregisterFrontendNode) -> Self {
        (&value.id).into()
    }
}

impl DeregisterFrontendNode {
    pub fn new(id: FrontendNodeId) -> Self {
        Self { id }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrontendNode {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the csi-node instance (gRPC).
    pub spec: FrontendNodeSpec,
    /// Deemed status of the frontend node.
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

/// Status of the FrontendNode.
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, Display, Eq, PartialEq)]
pub enum FrontendNodeStatus {
    /// FrontendNode is deemed online if it has not missed the
    /// registration.
    Online,
    /// FrontendNode is deemed offline if has missed the
    /// registration.
    Offline,
}

/// Frontend node state.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrontendNodeState {
    /// Frontend Node identification.
    pub id: FrontendNodeId,
    /// Endpoint of the frontend instance.
    pub endpoint: std::net::SocketAddr,
    /// Deemed status of the frontend node.
    pub status: FrontendNodeStatus,
}

impl From<FrontendNodeSpec> for FrontendNodeState {
    fn from(spec: FrontendNodeSpec) -> Self {
        Self {
            id: spec.id.clone(),
            endpoint: spec.endpoint,
            status: FrontendNodeStatus::Online,
        }
    }
}

/// Get frontend node/nodes by filter.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetFrontendNodes {
    filter: Filter,
}
impl GetFrontendNodes {
    /// New get frontend node request.
    pub fn new(filter: Filter) -> Self {
        Self { filter }
    }
    /// Return `Self` to request all frontend nodes (`None`) or a specific node (`FrontendNodeId`).
    pub fn from(frontend_node_id: impl Into<Option<FrontendNodeId>>) -> Self {
        let frontend_node_id = frontend_node_id.into();
        Self {
            filter: frontend_node_id.map_or(Filter::None, Filter::FrontendNode),
        }
    }
    /// Get the inner `Filter`.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}
