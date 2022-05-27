use crate::{
    operations::registry::traits::RegistryOperations,
    registry::{
        get_specs_reply,
        registry_grpc_server::{RegistryGrpc, RegistryGrpcServer},
        GetSpecsReply, GetSpecsRequest,
    },
};
use std::sync::Arc;
use tonic::Response;

/// gRPC Registry Server
#[derive(Clone)]
pub struct RegistryServer {
    /// Service which executes the operations.
    service: Arc<dyn RegistryOperations>,
}

impl RegistryServer {
    /// returns a new registry server  with the service implementing registry operations
    pub fn new(service: Arc<dyn RegistryOperations>) -> Self {
        Self { service }
    }
    /// converts the registry server to its corresponding grpc server type
    pub fn into_grpc_server(self) -> RegistryGrpcServer<Self> {
        RegistryGrpcServer::new(self)
    }
}

#[tonic::async_trait]
impl RegistryGrpc for RegistryServer {
    async fn get_specs(
        &self,
        request: tonic::Request<GetSpecsRequest>,
    ) -> Result<tonic::Response<GetSpecsReply>, tonic::Status> {
        let req: GetSpecsRequest = request.into_inner();
        match self.service.get_specs(&req, None).await {
            Ok(specs) => Ok(Response::new(GetSpecsReply {
                reply: Some(get_specs_reply::Reply::Specs(specs.into())),
            })),
            Err(err) => Ok(Response::new(GetSpecsReply {
                reply: Some(get_specs_reply::Reply::Error(err.into())),
            })),
        }
    }
}
