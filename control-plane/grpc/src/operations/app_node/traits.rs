use crate::{
    app_node,
    app_node::{
        registration::{DeregisterRequest, RegisterRequest},
        GetAppNodeRequest,
    },
    common::StringMapValue,
    context::Context,
    misc::traits::ValidateRequestTypes,
    operations::Pagination,
};
use std::net::{AddrParseError, SocketAddr};
use stor_port::{
    transport_api::{v0::AppNodes, ReplyError, ResourceKind},
    types::v0::{
        store::app_node::{AppNodeLabels, AppNodeSpec},
        transport::{
            AppNode, AppNodeId, AppNodeState, AppNodeStatus, DeregisterAppNode, Filter,
            RegisterAppNode,
        },
    },
};

/// Trait to be implemented for App Node operations.
#[tonic::async_trait]
pub trait AppNodeOperations: Send + Sync {
    /// Get app nodes based on the filters.
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<AppNode, ReplyError>;
    /// List app nodes based on the filters.
    async fn list(
        &self,
        pagination: Option<Pagination>,
        ctx: Option<Context>,
    ) -> Result<AppNodes, ReplyError>;
    /// Register a app node to control plane.
    async fn register_app_node(
        &self,
        req: &dyn AppNodeRegisterInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Deregister a app node from control plane.
    async fn deregister_app_node(
        &self,
        req: &dyn AppNodeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
}

/// Trait to be implemented for App Node Registration operation.
pub trait AppNodeRegisterInfo: Send + Sync {
    /// App Node Id of the csi instance.
    fn app_node_id(&self) -> AppNodeId;
    /// Grpc endpoint of the csi instance.
    fn grpc_endpoint(&self) -> SocketAddr;
    /// Labels to be set on the app node.
    fn labels(&self) -> Option<AppNodeLabels>;
}

/// Trait to be implemented for Deregister operation.
pub trait AppNodeInfo: Send + Sync {
    /// App Node Id of the csi instance.
    fn app_node_id(&self) -> AppNodeId;
}

impl AppNodeRegisterInfo for RegisterAppNode {
    fn app_node_id(&self) -> AppNodeId {
        self.id.clone()
    }

    fn grpc_endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    fn labels(&self) -> Option<AppNodeLabels> {
        self.labels.clone()
    }
}

/// Validated RegisterAppNodeRequest
#[derive(Debug)]
pub struct ValidatedRegisterAppNodeRequest {
    inner: RegisterRequest,
    grpc_endpoint: SocketAddr,
}

impl AppNodeRegisterInfo for ValidatedRegisterAppNodeRequest {
    fn app_node_id(&self) -> AppNodeId {
        self.inner.id.clone().into()
    }

    fn grpc_endpoint(&self) -> SocketAddr {
        self.grpc_endpoint
    }

    fn labels(&self) -> Option<AppNodeLabels> {
        self.inner.labels.clone().map(|l| l.value)
    }
}

impl ValidateRequestTypes for RegisterRequest {
    type Validated = ValidatedRegisterAppNodeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedRegisterAppNodeRequest {
            grpc_endpoint: self
                .grpc_endpoint
                .parse()
                .map_err(|error: AddrParseError| {
                    ReplyError::invalid_argument(
                        ResourceKind::AppNode,
                        "register_app_node_request.app_node.grpc_endpoint",
                        error.to_string(),
                    )
                })?,
            inner: self,
        })
    }
}

impl AppNodeInfo for DeregisterAppNode {
    fn app_node_id(&self) -> AppNodeId {
        self.id.clone()
    }
}
impl AppNodeInfo for DeregisterRequest {
    fn app_node_id(&self) -> AppNodeId {
        self.id.clone().into()
    }
}

impl From<&dyn AppNodeInfo> for DeregisterRequest {
    fn from(value: &dyn AppNodeInfo) -> Self {
        Self {
            id: value.app_node_id().to_string(),
        }
    }
}

impl From<&dyn AppNodeInfo> for DeregisterAppNode {
    fn from(value: &dyn AppNodeInfo) -> Self {
        Self {
            id: value.app_node_id(),
        }
    }
}

impl From<&dyn AppNodeRegisterInfo> for RegisterRequest {
    fn from(value: &dyn AppNodeRegisterInfo) -> Self {
        Self {
            id: value.app_node_id().to_string(),
            grpc_endpoint: value.grpc_endpoint().to_string(),
            labels: value
                .labels()
                .map(|labels| StringMapValue { value: labels }),
        }
    }
}

impl From<&dyn AppNodeRegisterInfo> for RegisterAppNode {
    fn from(value: &dyn AppNodeRegisterInfo) -> Self {
        Self {
            id: value.app_node_id(),
            endpoint: value.grpc_endpoint(),
            labels: value.labels(),
        }
    }
}

impl TryFrom<crate::app_node::AppNodeSpec> for AppNodeSpec {
    type Error = ReplyError;

    fn try_from(value: crate::app_node::AppNodeSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            endpoint: value
                .grpc_endpoint
                .parse()
                .map_err(|error: AddrParseError| {
                    ReplyError::invalid_argument(
                        ResourceKind::AppNode,
                        "get_app_node_request.app_node.endpoint",
                        error.to_string(),
                    )
                })?,
            labels: value.labels.map(|l| l.value),
        })
    }
}

impl From<AppNodeSpec> for crate::app_node::AppNodeSpec {
    fn from(value: AppNodeSpec) -> Self {
        Self {
            id: value.id.into(),
            labels: value
                .labels
                .map(|labels| crate::common::StringMapValue { value: labels }),
            grpc_endpoint: value.endpoint.to_string(),
        }
    }
}

impl From<app_node::AppNodeStatus> for AppNodeStatus {
    fn from(src: app_node::AppNodeStatus) -> Self {
        match src {
            app_node::AppNodeStatus::Online => Self::Online,
            app_node::AppNodeStatus::Offline => Self::Offline,
        }
    }
}

impl From<AppNodeStatus> for app_node::AppNodeStatus {
    fn from(src: AppNodeStatus) -> Self {
        match src {
            AppNodeStatus::Online => Self::Online,
            AppNodeStatus::Offline => Self::Offline,
        }
    }
}
impl TryFrom<crate::app_node::AppNodeState> for AppNodeState {
    type Error = ReplyError;

    fn try_from(value: crate::app_node::AppNodeState) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            endpoint: value
                .grpc_endpoint
                .parse()
                .map_err(|error: AddrParseError| {
                    ReplyError::invalid_argument(
                        ResourceKind::AppNode,
                        "get_app_node_request.app_node.endpoint",
                        error.to_string(),
                    )
                })?,
            status: match app_node::AppNodeStatus::try_from(value.status) {
                Ok(status) => Ok(status.into()),
                Err(error) => Err(Self::Error::invalid_argument(
                    ResourceKind::AppNode,
                    "app_node.state.status",
                    error,
                )),
            }?,
        })
    }
}

impl From<AppNodeState> for crate::app_node::AppNodeState {
    fn from(value: AppNodeState) -> Self {
        let status: app_node::AppNodeStatus = value.status.into();
        Self {
            id: value.id.into(),
            grpc_endpoint: value.endpoint.to_string(),
            status: status as i32,
        }
    }
}

impl From<AppNode> for crate::app_node::AppNode {
    fn from(value: AppNode) -> Self {
        Self {
            id: value.id.to_string(),
            spec: Some(value.spec.into()),
            state: value.state.map(|state| state.into()),
        }
    }
}

impl From<AppNodes> for crate::app_node::AppNodes {
    fn from(value: AppNodes) -> Self {
        let mut entries: Vec<crate::app_node::AppNode> = Vec::with_capacity(value.entries.len());
        for app_node in value.entries {
            entries.push(app_node.into())
        }
        Self {
            entries,
            next_token: value.next_token,
        }
    }
}

impl TryFrom<crate::app_node::AppNode> for AppNode {
    type Error = ReplyError;

    fn try_from(value: crate::app_node::AppNode) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            spec: match value.spec {
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::AppNode,
                        "app_node.spec",
                    ))
                }
                Some(spec) => AppNodeSpec::try_from(spec)?,
            },
            state: match value.state {
                None => None,
                Some(state) => Some(AppNodeState::try_from(state)?),
            },
        })
    }
}

impl TryFrom<crate::app_node::AppNodes> for AppNodes {
    type Error = ReplyError;
    fn try_from(grpc_app_nodes: crate::app_node::AppNodes) -> Result<Self, Self::Error> {
        let mut app_nodes: Vec<AppNode> = Vec::with_capacity(grpc_app_nodes.entries.len());
        for app_node in grpc_app_nodes.entries {
            app_nodes.push(AppNode::try_from(app_node)?)
        }
        Ok(AppNodes {
            entries: app_nodes,
            next_token: grpc_app_nodes.next_token,
        })
    }
}

impl TryFrom<Filter> for GetAppNodeRequest {
    type Error = ReplyError;

    fn try_from(value: Filter) -> Result<Self, Self::Error> {
        match value {
            Filter::AppNode(id) => Ok(Self {
                filter: Some(app_node::get_app_node_request::Filter::AppNode(
                    crate::common::AppNodeFilter {
                        app_node_id: id.to_string(),
                    },
                )),
            }),
            _ => Err(ReplyError::invalid_argument(
                ResourceKind::AppNode,
                "filter",
                "invalid filter for get app node request",
            )),
        }
    }
}
