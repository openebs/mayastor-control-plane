use common_lib::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{node, Deregister, NodeId, Register},
};
use rpc::{
    v1::registration::{DeregisterRequest, RegisterRequest},
    v1_alpha::registration::{
        DeregisterRequest as V1AlphaDeregisterRequest, RegisterRequest as V1AlphaRegisterRequest,
    },
};
use std::str::FromStr;

/// New type to wrap grpc ApiVersion type.
pub struct ApiVersion(pub rpc::v1::registration::ApiVersion);

/// Operations to be supportes by the Registration Service.
#[tonic::async_trait]
pub trait RegistrationOperations: Send + Sync {
    /// Register a dataplane node to controlplane.
    async fn register(&self, req: &dyn RegisterInfo) -> Result<(), ReplyError>;
    /// Deregister a dataplane node to controlplane.
    async fn deregister(&self, req: &dyn DeregisterInfo) -> Result<(), ReplyError>;
}

/// Trait to be implemented for Register operation.
pub trait RegisterInfo: Send + Sync {
    /// Node Id of the IoEngine instance.
    fn node_id(&self) -> NodeId;
    /// Grpc endpoint of the IoEngine instance.
    fn grpc_endpoint(&self) -> String;
    /// Api-version supported by the dataplane.
    fn api_version(&self) -> Option<Vec<node::ApiVersion>>;
}

/// Trait to be implemented for Register operation.
pub trait DeregisterInfo: Send + Sync {
    /// Node Id of the IoEngine instance.
    fn node_id(&self) -> NodeId;
}

impl RegisterInfo for Register {
    fn node_id(&self) -> NodeId {
        self.id.clone()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.to_string()
    }

    fn api_version(&self) -> Option<Vec<node::ApiVersion>> {
        self.api_versions.clone()
    }
}

impl RegisterInfo for RegisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.clone()
    }

    fn api_version(&self) -> Option<Vec<node::ApiVersion>> {
        Some(
            self.api_version
                .clone()
                .into_iter()
                .map(|v| {
                    ApiVersion(rpc::v1::registration::ApiVersion::from_i32(v).unwrap_or_default())
                        .into()
                })
                .collect(),
        )
    }
}

impl RegisterInfo for V1AlphaRegisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.clone()
    }

    fn api_version(&self) -> Option<Vec<node::ApiVersion>> {
        // Older versions support only V0
        Some(vec![node::ApiVersion::V0])
    }
}

impl DeregisterInfo for Deregister {
    fn node_id(&self) -> NodeId {
        self.id.clone()
    }
}

impl DeregisterInfo for DeregisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }
}

impl DeregisterInfo for V1AlphaDeregisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }
}

impl TryFrom<&dyn RegisterInfo> for Register {
    type Error = ReplyError;
    fn try_from(register: &dyn RegisterInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            id: register.node_id(),
            grpc_endpoint: std::net::SocketAddr::from_str(&register.grpc_endpoint()).map_err(
                |error| {
                    Self::Error::invalid_argument(
                        ResourceKind::Node,
                        "register.grpc_endpoint",
                        error.to_string(),
                    )
                },
            )?,
            api_versions: register.api_version(),
        })
    }
}

impl From<&dyn DeregisterInfo> for Deregister {
    fn from(register: &dyn DeregisterInfo) -> Self {
        Self {
            id: register.node_id(),
        }
    }
}

impl From<node::ApiVersion> for ApiVersion {
    fn from(v: node::ApiVersion) -> Self {
        match v {
            node::ApiVersion::V0 => Self(rpc::v1::registration::ApiVersion::V0),
            node::ApiVersion::V1 => Self(rpc::v1::registration::ApiVersion::V1),
        }
    }
}

impl From<ApiVersion> for node::ApiVersion {
    fn from(v: ApiVersion) -> Self {
        match v {
            ApiVersion(rpc::v1::registration::ApiVersion::V0) => Self::V0,
            ApiVersion(rpc::v1::registration::ApiVersion::V1) => Self::V1,
        }
    }
}
