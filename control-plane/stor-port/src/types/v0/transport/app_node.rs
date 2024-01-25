use crate::{
    rpc_impl_string_id, rpc_impl_string_id_inner,
    types::v0::{
        store::app_node::{AppNodeLabels, AppNodeSpec, AppNodeSpecKey},
        transport::Filter,
    },
};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

rpc_impl_string_id!(AppNodeId, "ID of a app node");

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterAppNode {
    /// App Node identification.
    pub id: AppNodeId,
    /// Endpoint of the csi-node instance (gRPC).
    pub endpoint: std::net::SocketAddr,
    /// App Node labels.
    pub labels: Option<AppNodeLabels>,
}

impl RegisterAppNode {
    pub fn new(
        id: AppNodeId,
        endpoint: std::net::SocketAddr,
        labels: Option<AppNodeLabels>,
    ) -> Self {
        Self {
            id,
            endpoint,
            labels,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeregisterAppNode {
    /// App Node identification.
    pub id: AppNodeId,
}

impl From<DeregisterAppNode> for AppNodeSpecKey {
    fn from(value: DeregisterAppNode) -> Self {
        (&value.id).into()
    }
}

impl DeregisterAppNode {
    pub fn new(id: AppNodeId) -> Self {
        Self { id }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AppNode {
    /// App Node identification.
    pub id: AppNodeId,
    /// Endpoint of the csi-node instance (gRPC).
    pub spec: AppNodeSpec,
    /// Deemed status of the app node.
    pub state: Option<AppNodeState>,
}

impl AppNode {
    pub fn new(spec: AppNodeSpec, state: Option<AppNodeState>) -> Self {
        Self {
            id: spec.id.clone(),
            spec,
            state,
        }
    }
}

/// Status of the AppNode.
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, Display, Eq, PartialEq)]
pub enum AppNodeStatus {
    /// AppNode is deemed online if it has not missed the
    /// registration.
    Online,
    /// AppNode is deemed offline if has missed the
    /// registration.
    Offline,
}

/// App node state.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AppNodeState {
    /// App Node identification.
    pub id: AppNodeId,
    /// Endpoint of the app instance.
    pub endpoint: std::net::SocketAddr,
    /// Deemed status of the app node.
    pub status: AppNodeStatus,
}

impl From<AppNodeSpec> for AppNodeState {
    fn from(spec: AppNodeSpec) -> Self {
        Self {
            id: spec.id.clone(),
            endpoint: spec.endpoint,
            status: AppNodeStatus::Online,
        }
    }
}

/// Get app node by filter.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetAppNode {
    filter: Filter,
}
impl GetAppNode {
    /// New get app node request.
    pub fn new(filter: Filter) -> Self {
        Self { filter }
    }
    /// Return `Self` to request all app nodes (`None`) or a specific node (`AppNodeId`).
    pub fn from(app_node_id: impl Into<Option<AppNodeId>>) -> Self {
        let app_node_id = app_node_id.into();
        Self {
            filter: app_node_id.map_or(Filter::None, Filter::AppNode),
        }
    }
    /// Get the inner `Filter`.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}
