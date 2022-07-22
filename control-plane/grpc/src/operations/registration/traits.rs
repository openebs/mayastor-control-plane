use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{Deregister, NodeId, Register},
};
use rpc::registration::{DeregisterRequest, RegisterRequest};

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
}

impl RegisterInfo for RegisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.clone()
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

impl From<&dyn RegisterInfo> for Register {
    fn from(register: &dyn RegisterInfo) -> Self {
        Self {
            id: register.node_id(),
            grpc_endpoint: register.grpc_endpoint(),
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
