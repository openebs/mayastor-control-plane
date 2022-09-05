use crate::operations::registration::traits::RegistrationOperations;

use rpc::{
    v1::registration::{
        registration_server, registration_server::Registration, DeregisterRequest, RegisterRequest,
    },
    v1_alpha::registration::{
        registration_server as v1_alpha_registration_server,
        registration_server::Registration as V1AlphaRegistration,
        DeregisterRequest as V1AlphaDeregisterRequest, RegisterRequest as V1AlphaRegisterRequest,
    },
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
    /// Returns a new Registration server with the service implementing Registration operations.
    pub fn new(service: Arc<dyn RegistrationOperations>) -> Self {
        Self { service }
    }
    /// Converts the Registration server to its corresponding v1 grpc registration server type.
    pub fn into_v1_grpc_server(
        self,
    ) -> registration_server::RegistrationServer<RegistrationServer> {
        registration_server::RegistrationServer::new(self)
    }
    /// Converts the Registration server to its corresponding v1 alpha registration grpc server
    /// type.
    pub fn into_v1_alpha_grpc_server(
        self,
    ) -> v1_alpha_registration_server::RegistrationServer<RegistrationServer> {
        v1_alpha_registration_server::RegistrationServer::new(self)
    }
}

/// Implementation of the v1 registration RPC methods.
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

/// Implementation of the v1 alpha registration RPC methods.
#[tonic::async_trait]
impl V1AlphaRegistration for RegistrationServer {
    async fn register(
        &self,
        request: tonic::Request<V1AlphaRegisterRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        match self.service.register(&req).await {
            Ok(()) => Ok(Response::new(())),
            Err(_) => Err(tonic::Status::new(Code::Aborted, "".to_string())),
        }
    }
    async fn deregister(
        &self,
        request: tonic::Request<V1AlphaDeregisterRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        match self.service.deregister(&req).await {
            Ok(()) => Ok(Response::new(())),
            Err(_) => Err(tonic::Status::new(Code::Aborted, "".to_string())),
        }
    }
}
