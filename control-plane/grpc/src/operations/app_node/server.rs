use crate::{
    app_node,
    app_node::{
        app_node_grpc_server,
        registration::{registration_server, DeregisterRequest, RegisterRequest},
        GetAppNodeReply, GetAppNodeRequest, ListAppNodesReply, ListAppNodesRequest,
    },
    misc::traits::ValidateRequestTypes,
    operations::app_node::traits::AppNodeOperations,
};
use std::sync::Arc;
use stor_port::types::v0::transport::Filter;
use tonic::{Request, Response, Status};

/// gRPC App Node Server.
#[derive(Clone)]
pub struct AppNodeServer {
    /// Service which executes the operations.
    service: Arc<dyn AppNodeOperations>,
}

impl AppNodeServer {
    /// Create a new instance of the gRPC app node server.
    pub fn new(service: Arc<dyn AppNodeOperations>) -> Self {
        Self { service }
    }
    /// Converts the App Node Registration server to its corresponding v1 grpc registration
    /// server type.
    pub fn into_v1_registration_grpc_server(
        self,
    ) -> registration_server::RegistrationServer<AppNodeServer> {
        registration_server::RegistrationServer::new(self)
    }
    /// Converts the App Node server to its corresponding v1 grpc server type.
    pub fn into_v1_grpc_server(self) -> app_node_grpc_server::AppNodeGrpcServer<AppNodeServer> {
        app_node_grpc_server::AppNodeGrpcServer::new(self)
    }
}

#[tonic::async_trait]
impl registration_server::Registration for AppNodeServer {
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner().validated()?;
        match self.service.register_app_node(&request, None).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(err.into()),
        }
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        match self.service.deregister_app_node(&request, None).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(err.into()),
        }
    }
}

#[tonic::async_trait]
impl app_node_grpc_server::AppNodeGrpc for AppNodeServer {
    async fn get_app_node(
        &self,
        request: Request<GetAppNodeRequest>,
    ) -> Result<Response<GetAppNodeReply>, Status> {
        let req: GetAppNodeRequest = request.into_inner();
        let Some(filter) = req.filter else {
            return Err(Status::invalid_argument("app node id not provided"));
        };
        let filter = match filter {
            app_node::get_app_node_request::Filter::AppNode(filter) => {
                Filter::AppNode(filter.app_node_id.into())
            }
        };
        match self.service.get(filter, None).await {
            Ok(node) => Ok(Response::new(GetAppNodeReply {
                app_node: Some(node.into()),
            })),
            Err(err) => Err(err.into()),
        }
    }

    async fn list_app_nodes(
        &self,
        request: Request<ListAppNodesRequest>,
    ) -> Result<Response<ListAppNodesReply>, Status> {
        let req: ListAppNodesRequest = request.into_inner();
        let pagination = req.pagination.map(|p| p.into());
        match self.service.list(pagination, None).await {
            Ok(nodes) => Ok(Response::new(ListAppNodesReply {
                app_nodes: Some(nodes.into()),
            })),
            Err(err) => Err(err.into()),
        }
    }
}
