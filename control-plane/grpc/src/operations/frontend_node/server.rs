use crate::{
    frontend,
    frontend::{
        frontend_node_grpc_server,
        registration::{registration_server, DeregisterRequest, RegisterRequest},
        GetFrontendNodeReply, GetFrontendNodeRequest, ListFrontendNodesReply,
        ListFrontendNodesRequest,
    },
    misc::traits::ValidateRequestTypes,
    operations::frontend_node::traits::FrontendNodeOperations,
};
use std::sync::Arc;
use stor_port::types::v0::transport::Filter;
use tonic::{Request, Response, Status};

/// gRPC Frontend Node Server.
#[derive(Clone)]
pub struct FrontendNodeServer {
    /// Service which executes the operations.
    service: Arc<dyn FrontendNodeOperations>,
}

impl FrontendNodeServer {
    /// Create a new instance of the gRPC frontend node server.
    pub fn new(service: Arc<dyn FrontendNodeOperations>) -> Self {
        Self { service }
    }
    /// Converts the Frontend Node Registration server to its corresponding v1 grpc registration
    /// server type.
    pub fn into_v1_registration_grpc_server(
        self,
    ) -> registration_server::RegistrationServer<FrontendNodeServer> {
        registration_server::RegistrationServer::new(self)
    }
    /// Converts the Frontend Node server to its corresponding v1 grpc server type.
    pub fn into_v1_grpc_server(
        self,
    ) -> frontend_node_grpc_server::FrontendNodeGrpcServer<FrontendNodeServer> {
        frontend_node_grpc_server::FrontendNodeGrpcServer::new(self)
    }
}

#[tonic::async_trait]
impl registration_server::Registration for FrontendNodeServer {
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner().validated()?;
        match self.service.register_frontend_node(&request, None).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(err.into()),
        }
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        match self.service.deregister_frontend_node(&request, None).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(err.into()),
        }
    }
}

#[tonic::async_trait]
impl frontend_node_grpc_server::FrontendNodeGrpc for FrontendNodeServer {
    async fn get_frontend_node(
        &self,
        request: Request<GetFrontendNodeRequest>,
    ) -> Result<Response<GetFrontendNodeReply>, Status> {
        let req: GetFrontendNodeRequest = request.into_inner();
        let Some(filter) = req.filter else {
            return Err(Status::invalid_argument("frontend node id not provided"));
        };
        let filter = match filter {
            frontend::get_frontend_node_request::Filter::FrontendNode(filter) => {
                Filter::FrontendNode(filter.frontend_node_id.into())
            }
        };
        match self.service.get(filter, None).await {
            Ok(node) => Ok(Response::new(GetFrontendNodeReply {
                frontend_node: Some(node.into()),
            })),
            Err(err) => Err(err.into()),
        }
    }

    async fn list_frontend_nodes(
        &self,
        request: Request<ListFrontendNodesRequest>,
    ) -> Result<Response<ListFrontendNodesReply>, Status> {
        let req: ListFrontendNodesRequest = request.into_inner();
        let filter = match req.filter {
            None => Filter::None,
            _ => {
                return Err(Status::invalid_argument(
                    "invalid filter provided for list request",
                ))
            }
        };
        let pagination = req.pagination.map(|p| p.into());
        match self.service.list(filter, pagination, None).await {
            Ok(nodes) => Ok(Response::new(ListFrontendNodesReply {
                frontend_nodes: Some(nodes.into()),
            })),
            Err(err) => Err(err.into()),
        }
    }
}
