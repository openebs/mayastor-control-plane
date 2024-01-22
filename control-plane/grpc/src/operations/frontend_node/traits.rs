use crate::{
    common::StringMapValue,
    context::Context,
    frontend,
    frontend::{
        registration::{DeregisterRequest, RegisterRequest},
        GetFrontendNodeRequest, ListFrontendNodesRequest,
    },
    misc::traits::ValidateRequestTypes,
    operations::Pagination,
};
use std::net::{AddrParseError, SocketAddr};
use stor_port::{
    transport_api::{v0::FrontendNodes, ReplyError, ResourceKind},
    types::v0::{
        store::frontend_node::{FrontendNodeLabels, FrontendNodeSpec},
        transport::{
            DeregisterFrontendNode, Filter, FrontendNode, FrontendNodeId, FrontendNodeState,
            FrontendNodeStatus, RegisterFrontendNode,
        },
    },
};

/// Trait to be implemented for Frontend Node operations.
#[tonic::async_trait]
pub trait FrontendNodeOperations: Send + Sync {
    /// Get frontend node based on the filters.
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<FrontendNode, ReplyError>;
    /// List frontend nodes based on the filters.
    async fn list(
        &self,
        filter: Filter,
        pagination: Option<Pagination>,
        ctx: Option<Context>,
    ) -> Result<FrontendNodes, ReplyError>;
    /// Register a frontend node to control plane.
    async fn register_frontend_node(
        &self,
        req: &dyn FrontendNodeRegisterInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Deregister a frontend node from control plane.
    async fn deregister_frontend_node(
        &self,
        req: &dyn FrontendNodeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
}

/// Trait to be implemented for Frontend Node Registration operation.
pub trait FrontendNodeRegisterInfo: Send + Sync {
    /// Frontend Node Id of the csi instance.
    fn frontend_node_id(&self) -> FrontendNodeId;
    /// Grpc endpoint of the csi instance.
    fn grpc_endpoint(&self) -> SocketAddr;
    /// Labels to be set on the frontend node.
    fn labels(&self) -> Option<FrontendNodeLabels>;
}

/// Trait to be implemented for Deregister operation.
pub trait FrontendNodeInfo: Send + Sync {
    /// Frontend Node Id of the csi instance.
    fn frontend_node_id(&self) -> FrontendNodeId;
}

impl FrontendNodeRegisterInfo for RegisterFrontendNode {
    fn frontend_node_id(&self) -> FrontendNodeId {
        self.id.clone()
    }

    fn grpc_endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    fn labels(&self) -> Option<FrontendNodeLabels> {
        self.labels.clone()
    }
}

/// Validated RegisterFrontendNodeRequest
#[derive(Debug)]
pub struct ValidatedRegisterFrontendNodeRequest {
    inner: RegisterRequest,
    grpc_endpoint: SocketAddr,
}

impl FrontendNodeRegisterInfo for ValidatedRegisterFrontendNodeRequest {
    fn frontend_node_id(&self) -> FrontendNodeId {
        self.inner.id.clone().into()
    }

    fn grpc_endpoint(&self) -> SocketAddr {
        self.grpc_endpoint
    }

    fn labels(&self) -> Option<FrontendNodeLabels> {
        self.inner.labels.clone().map(|l| l.value)
    }
}

impl ValidateRequestTypes for RegisterRequest {
    type Validated = ValidatedRegisterFrontendNodeRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedRegisterFrontendNodeRequest {
            grpc_endpoint: self
                .grpc_endpoint
                .parse()
                .map_err(|error: AddrParseError| {
                    ReplyError::invalid_argument(
                        ResourceKind::FrontendNode,
                        "register_frontend_node_request.frontend_node.grpc_endpoint",
                        error.to_string(),
                    )
                })?,
            inner: self,
        })
    }
}

impl FrontendNodeInfo for DeregisterFrontendNode {
    fn frontend_node_id(&self) -> FrontendNodeId {
        self.id.clone()
    }
}
impl FrontendNodeInfo for DeregisterRequest {
    fn frontend_node_id(&self) -> FrontendNodeId {
        self.id.clone().into()
    }
}

impl From<&dyn FrontendNodeInfo> for DeregisterRequest {
    fn from(value: &dyn FrontendNodeInfo) -> Self {
        Self {
            id: value.frontend_node_id().to_string(),
        }
    }
}

impl From<&dyn FrontendNodeInfo> for DeregisterFrontendNode {
    fn from(value: &dyn FrontendNodeInfo) -> Self {
        Self {
            id: value.frontend_node_id(),
        }
    }
}

impl From<&dyn FrontendNodeRegisterInfo> for RegisterRequest {
    fn from(value: &dyn FrontendNodeRegisterInfo) -> Self {
        Self {
            id: value.frontend_node_id().to_string(),
            grpc_endpoint: value.grpc_endpoint().to_string(),
            labels: value
                .labels()
                .map(|labels| StringMapValue { value: labels }),
        }
    }
}

impl From<&dyn FrontendNodeRegisterInfo> for RegisterFrontendNode {
    fn from(value: &dyn FrontendNodeRegisterInfo) -> Self {
        Self {
            id: value.frontend_node_id(),
            endpoint: value.grpc_endpoint(),
            labels: value.labels(),
        }
    }
}

impl TryFrom<crate::frontend::FrontendNodeSpec> for FrontendNodeSpec {
    type Error = ReplyError;

    fn try_from(value: crate::frontend::FrontendNodeSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            endpoint: value
                .grpc_endpoint
                .parse()
                .map_err(|error: AddrParseError| {
                    ReplyError::invalid_argument(
                        ResourceKind::FrontendNode,
                        "get_frontend_node_request.frontend_node.endpoint",
                        error.to_string(),
                    )
                })?,
            labels: value.labels.map(|l| l.value),
        })
    }
}

impl From<FrontendNodeSpec> for crate::frontend::FrontendNodeSpec {
    fn from(value: FrontendNodeSpec) -> Self {
        Self {
            id: value.id.into(),
            labels: value
                .labels
                .map(|labels| crate::common::StringMapValue { value: labels }),
            grpc_endpoint: value.endpoint.to_string(),
        }
    }
}

impl From<frontend::FrontendNodeStatus> for FrontendNodeStatus {
    fn from(src: frontend::FrontendNodeStatus) -> Self {
        match src {
            frontend::FrontendNodeStatus::Online => Self::Online,
            frontend::FrontendNodeStatus::Offline => Self::Offline,
        }
    }
}

impl From<FrontendNodeStatus> for frontend::FrontendNodeStatus {
    fn from(src: FrontendNodeStatus) -> Self {
        match src {
            FrontendNodeStatus::Online => Self::Online,
            FrontendNodeStatus::Offline => Self::Offline,
        }
    }
}
impl TryFrom<crate::frontend::FrontendNodeState> for FrontendNodeState {
    type Error = ReplyError;

    fn try_from(value: crate::frontend::FrontendNodeState) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            endpoint: value
                .grpc_endpoint
                .parse()
                .map_err(|error: AddrParseError| {
                    ReplyError::invalid_argument(
                        ResourceKind::FrontendNode,
                        "get_frontend_node_request.frontend_node.endpoint",
                        error.to_string(),
                    )
                })?,
            status: match frontend::FrontendNodeStatus::try_from(value.status) {
                Ok(status) => Ok(status.into()),
                Err(error) => Err(Self::Error::invalid_argument(
                    ResourceKind::FrontendNode,
                    "frontend_node.state.status",
                    error,
                )),
            }?,
        })
    }
}

impl From<FrontendNodeState> for crate::frontend::FrontendNodeState {
    fn from(value: FrontendNodeState) -> Self {
        let status: frontend::FrontendNodeStatus = value.status.into();
        Self {
            id: value.id.into(),
            grpc_endpoint: value.endpoint.to_string(),
            status: status as i32,
        }
    }
}

impl From<FrontendNode> for crate::frontend::FrontendNode {
    fn from(value: FrontendNode) -> Self {
        Self {
            id: value.id.to_string(),
            spec: Some(value.spec.into()),
            state: value.state.map(|state| state.into()),
        }
    }
}

impl From<FrontendNodes> for crate::frontend::FrontendNodes {
    fn from(value: FrontendNodes) -> Self {
        let mut entries: Vec<crate::frontend::FrontendNode> =
            Vec::with_capacity(value.entries.len());
        for frontend_node in value.entries {
            entries.push(frontend_node.into())
        }
        Self {
            entries,
            next_token: value.next_token,
        }
    }
}

impl TryFrom<crate::frontend::FrontendNode> for FrontendNode {
    type Error = ReplyError;

    fn try_from(value: crate::frontend::FrontendNode) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            spec: match value.spec {
                None => {
                    return Err(ReplyError::missing_argument(
                        ResourceKind::FrontendNode,
                        "frontend_node.spec",
                    ))
                }
                Some(spec) => FrontendNodeSpec::try_from(spec)?,
            },
            state: match value.state {
                None => None,
                Some(state) => Some(FrontendNodeState::try_from(state)?),
            },
        })
    }
}

impl TryFrom<crate::frontend::FrontendNodes> for FrontendNodes {
    type Error = ReplyError;
    fn try_from(grpc_frontend_nodes: crate::frontend::FrontendNodes) -> Result<Self, Self::Error> {
        let mut frontend_nodes: Vec<FrontendNode> =
            Vec::with_capacity(grpc_frontend_nodes.entries.len());
        for frontend_node in grpc_frontend_nodes.entries {
            frontend_nodes.push(FrontendNode::try_from(frontend_node)?)
        }
        Ok(FrontendNodes {
            entries: frontend_nodes,
            next_token: grpc_frontend_nodes.next_token,
        })
    }
}

impl TryFrom<Filter> for GetFrontendNodeRequest {
    type Error = ReplyError;

    fn try_from(value: Filter) -> Result<Self, Self::Error> {
        match value {
            Filter::FrontendNode(id) => Ok(Self {
                filter: Some(frontend::get_frontend_node_request::Filter::FrontendNode(
                    crate::common::FrontendNodeFilter {
                        frontend_node_id: id.to_string(),
                    },
                )),
            }),
            _ => Err(ReplyError::invalid_argument(
                ResourceKind::FrontendNode,
                "filter",
                "invalid filter for get frontend node request",
            )),
        }
    }
}

pub(crate) fn to_list_frontend_node_request(
    pagination: Option<Pagination>,
    filter: Filter,
) -> Result<ListFrontendNodesRequest, ReplyError> {
    let filter = match filter {
        Filter::None => None,
        _ => {
            return Err(ReplyError::invalid_argument(
                ResourceKind::FrontendNode,
                "filter",
                "invalid filter for get frontend node request",
            ))
        }
    };
    let pagination = pagination.map(|p| p.into());
    Ok(ListFrontendNodesRequest { filter, pagination })
}
