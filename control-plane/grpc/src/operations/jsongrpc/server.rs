use crate::{
    jsongrpc,
    jsongrpc::{json_grpc_reply, json_grpc_server},
    operations::jsongrpc::traits::JsonGrpcOperations,
};

use std::sync::Arc;
use tonic::Response;

/// RPC JsonGrpc Server
#[derive(Clone)]
pub struct JsonGrpcServer {
    /// Service which executes the operations.
    service: Arc<dyn JsonGrpcOperations>,
}

impl JsonGrpcServer {
    /// returns a new JsonGrpc Server with the service implementing JsonGrpc operations
    pub fn new(service: Arc<dyn JsonGrpcOperations>) -> Self {
        Self { service }
    }
    /// coverts the JsonGrpc Server to its corresponding grpc server type
    pub fn into_grpc_server(self) -> json_grpc_server::JsonGrpcServer<JsonGrpcServer> {
        json_grpc_server::JsonGrpcServer::new(self)
    }
}

/// Implementation of the RPC methods.
#[tonic::async_trait]
impl json_grpc_server::JsonGrpc for JsonGrpcServer {
    async fn json_grpc_call(
        &self,
        request: tonic::Request<jsongrpc::JsonGrpcRequest>,
    ) -> Result<tonic::Response<jsongrpc::JsonGrpcReply>, tonic::Status> {
        match self.service.call(&request.into_inner(), None).await {
            Ok(value) => Ok(Response::new(jsongrpc::JsonGrpcReply {
                reply: Some(json_grpc_reply::Reply::Response(value.to_string())),
            })),
            Err(err) => Ok(Response::new(jsongrpc::JsonGrpcReply {
                reply: Some(json_grpc_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn probe(
        &self,
        _request: tonic::Request<jsongrpc::ProbeRequest>,
    ) -> Result<tonic::Response<jsongrpc::ProbeResponse>, tonic::Status> {
        match self.service.probe(None).await {
            Ok(resp) => Ok(Response::new(jsongrpc::ProbeResponse { ready: resp })),
            Err(_) => Ok(Response::new(jsongrpc::ProbeResponse { ready: false })),
        }
    }
}
