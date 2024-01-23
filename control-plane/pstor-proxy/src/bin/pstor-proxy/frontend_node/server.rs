use crate::frontend_node::service::Service;
use pstor_proxy::{
    types::{
        frontend_node::{DeregisterFrontendNode, RegisterFrontendNode},
        misc::{Filter, Pagination},
    },
    v1::{
        frontend_node::{
            get_frontend_nodes_reply, FrontendNodeGrpc, FrontendNodeGrpcServer,
            GetFrontendNodesReply, GetFrontendNodesRequest,
        },
        registration::{DeregisterRequest, RegisterRequest, Registration, RegistrationServer},
    },
};
use std::net::AddrParseError;
use tonic::{Request, Response, Status};

/// RPC Frontend Node Registration Server
#[derive(Clone)]
pub(crate) struct FrontendNodeRegistrationServer {
    /// Service which executes the operations.
    service: Service,
}

impl FrontendNodeRegistrationServer {
    /// Returns a new FrontendNodeRegistrationServer with the service implementing FrontendNode
    /// operations.
    pub(crate) fn new(service: Service) -> Self {
        Self { service }
    }
    /// Converts the FrontendNodeRegistrationServer to its corresponding grpc server type.
    pub(crate) fn into_grpc_server(self) -> RegistrationServer<FrontendNodeRegistrationServer> {
        RegistrationServer::new(self)
    }
}

#[tonic::async_trait]
impl Registration for FrontendNodeRegistrationServer {
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.service
            .register_frontend_node(&RegisterFrontendNode::new(
                req.id.into(),
                req.grpc_endpoint
                    .parse()
                    .map_err(|error: AddrParseError| Status::invalid_argument(error.to_string()))?,
            ))
            .await?;
        Ok(Response::new(()))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.service
            .deregister_frontend_node(&DeregisterFrontendNode::new(req.id.into()))
            .await?;
        Ok(Response::new(()))
    }
}

/// RPC Frontend Node Server
#[derive(Clone)]
pub(crate) struct FrontendNodeServer {
    /// Service which executes the operations.
    service: Service,
}

impl FrontendNodeServer {
    /// Returns a new FrontendNodeServer with the service implementing FrontendNode operations.
    pub(crate) fn new(service: Service) -> Self {
        Self { service }
    }
    /// Converts the FrontendNodeServer to its corresponding grpc server type.
    pub(crate) fn into_grpc_server(self) -> FrontendNodeGrpcServer<FrontendNodeServer> {
        FrontendNodeGrpcServer::new(self)
    }
}

#[tonic::async_trait]
impl FrontendNodeGrpc for FrontendNodeServer {
    async fn get_frontend_nodes(
        &self,
        request: Request<GetFrontendNodesRequest>,
    ) -> Result<Response<GetFrontendNodesReply>, Status> {
        let req: GetFrontendNodesRequest = request.into_inner();
        let filter = match req.filter {
            Some(filter) => match Filter::try_from(filter) {
                Ok(filter) => filter,
                Err(err) => {
                    return Ok(Response::new(GetFrontendNodesReply {
                        reply: Some(get_frontend_nodes_reply::Reply::Error(err)),
                    }))
                }
            },
            None => Filter::None,
        };

        let pagination: Option<Pagination> = req.pagination.map(|p| p.into());
        match self
            .service
            .get_frontend_nodes(filter, req.ignore_notfound, pagination)
            .await
        {
            Ok(frontend_nodes) => Ok(Response::new(GetFrontendNodesReply {
                reply: Some(get_frontend_nodes_reply::Reply::FrontendNodes(
                    frontend_nodes.into(),
                )),
            })),
            Err(err) => Ok(Response::new(GetFrontendNodesReply {
                reply: Some(get_frontend_nodes_reply::Reply::Error(err.into())),
            })),
        }
    }
}
