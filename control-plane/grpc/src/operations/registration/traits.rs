use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{APIVersion, Deregister, NodeId, Register},
};
use rpc::{
    v1::registration::{DeregisterRequest, RegisterRequest},
    v1_alpha::registration::{
        DeregisterRequest as V1AlphaDeregisterRequest, RegisterRequest as V1AlphaRegisterRequest,
    },
};

/// new type to wrap grpc ApiVersion type
pub struct ApiVersion(pub rpc::v1::registration::ApiVersion);

/// Operations to be supportes by the Registration Service
#[tonic::async_trait]
pub trait RegistrationOperations: Send + Sync {
    /// Register a dataplane node to controlplane
    async fn register(&self, req: &dyn RegisterInfo) -> Result<(), ReplyError>;
    /// Deregister a dataplane node to controlplane
    async fn deregister(&self, req: &dyn DeregisterInfo) -> Result<(), ReplyError>;
}

/// Trait to be implemented for Register operation
pub trait RegisterInfo: Send + Sync {
    /// Id of the IoEngine instance
    fn node_id(&self) -> NodeId;
    /// Grpc endpoint of the IoEngine instance
    fn grpc_endpoint(&self) -> String;
    /// api-version supported by the dataplane
    fn api_version(&self) -> Option<Vec<APIVersion>>;
}

/// Trait to be implemented for Register operation
pub trait DeregisterInfo: Send + Sync {
    /// Id of the IoEngine instance
    fn node_id(&self) -> NodeId;
}

impl RegisterInfo for Register {
    fn node_id(&self) -> NodeId {
        self.id.clone()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.clone()
    }

    fn api_version(&self) -> Option<Vec<APIVersion>> {
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

    fn api_version(&self) -> Option<Vec<APIVersion>> {
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

    fn api_version(&self) -> Option<Vec<APIVersion>> {
        // Older versions support only V0
        Some(vec![APIVersion::V0])
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

impl From<&dyn RegisterInfo> for Register {
    fn from(register: &dyn RegisterInfo) -> Self {
        Self {
            id: register.node_id(),
            grpc_endpoint: register.grpc_endpoint(),
            api_versions: register.api_version(),
        }
    }
}

impl From<&dyn DeregisterInfo> for Deregister {
    fn from(register: &dyn DeregisterInfo) -> Self {
        Self {
            id: register.node_id(),
        }
    }
}

impl From<APIVersion> for ApiVersion {
    fn from(v: APIVersion) -> Self {
        match v {
            APIVersion::V0 => ApiVersion(rpc::v1::registration::ApiVersion::V0),
            APIVersion::V1 => ApiVersion(rpc::v1::registration::ApiVersion::V1),
        }
    }
}

impl From<ApiVersion> for APIVersion {
    fn from(v: ApiVersion) -> Self {
        match v {
            ApiVersion(rpc::v1::registration::ApiVersion::V0) => Self::V0,
            ApiVersion(rpc::v1::registration::ApiVersion::V1) => Self::V1,
        }
    }
}
