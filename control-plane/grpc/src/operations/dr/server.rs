use crate::{dr, dr::dr_grpc_server, operations::dr::traits::DrOperations};

use std::sync::Arc;
use tonic::Response;

/// RPC Dr Server
#[derive(Clone)]
pub struct DrServer {
    /// Service which executes the operations.
    service: Arc<dyn DrOperations>,
}

impl DrServer {
    /// Returns a new Dr Server with the service implementing the dr operations.
    pub fn new(service: Arc<dyn DrOperations>) -> Self {
        Self { service }
    }
    /// Converts the Dr Server into its corresponding grpc server type.
    pub fn into_grpc_server(self) -> dr_grpc_server::DrGrpcServer<DrServer> {
        dr_grpc_server::DrGrpcServer::new(self)
    }
}

/// Implementation of the RPC methods.
#[tonic::async_trait]
impl dr_grpc_server::DrGrpc for DrServer {
    async fn probe(
        &self,
        _request: tonic::Request<dr::ProbeRequest>,
    ) -> Result<tonic::Response<dr::ProbeResponse>, tonic::Status> {
        match self.service.probe(None).await {
            Ok(ready) => Ok(Response::new(dr::ProbeResponse { ready })),
            Err(_) => Ok(Response::new(dr::ProbeResponse { ready: false })),
        }
    }
}
