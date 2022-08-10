use crate::operations::registration::traits::RegistrationOperations;

use rpc::v1::registration::{
    registration_server, registration_server::Registration, DeregisterRequest, RegisterRequest,
};
use std::sync::Arc;
use tonic::{Code, Response};

/// RPC Registration Server
#[derive(Clone)]
pub struct RegistrationServer {
    /// Service which executes the operations.
    service: Arc<dyn RegistrationOperations>,
}

impl RegistrationServer {
    /// returns a new Registration server with the service implementing Registration operations
    pub fn new(service: Arc<dyn RegistrationOperations>) -> Self {
        Self { service }
    }
    /// coverts the Registration server to its corresponding grpc server type
    pub fn into_grpc_server(self) -> registration_server::RegistrationServer<RegistrationServer> {
        registration_server::RegistrationServer::new(self)
    }
}

/// Implementation of the RPC methods.
#[tonic::async_trait]
impl Registration for RegistrationServer {
    async fn register(
        &self,
        request: tonic::Request<RegisterRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        match self.service.register(&req).await {
            Ok(()) => Ok(Response::new(())),
            Err(_) => Err(tonic::Status::new(Code::Aborted, "".to_string())),
        }
    }
    async fn deregister(
        &self,
        request: tonic::Request<DeregisterRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        match self.service.deregister(&req).await {
            Ok(()) => Ok(Response::new(())),
            Err(_) => Err(tonic::Status::new(Code::Aborted, "".to_string())),
        }
    }
}
