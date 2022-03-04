use crate::{
    blockdevice::{GetBlockDevicesReply, GetBlockDevicesRequest},
    node,
    node::{
        get_nodes_reply,
        node_grpc_server::{NodeGrpc, NodeGrpcServer},
        GetNodesReply, GetNodesRequest, ProbeRequest, ProbeResponse,
    },
    operations::node::traits::NodeOperations,
};
use std::sync::Arc;
use tonic::{Request, Response};

/// gRPC Node Server
#[derive(Clone)]
pub struct NodeServer {
    /// Service which executes the operations.
    service: Arc<dyn NodeOperations>,
}

impl NodeServer {
    /// returns a new nodeserver with the service implementing node operations
    pub fn new(service: Arc<dyn NodeOperations>) -> Self {
        Self { service }
    }
    /// converts the nodeserver to its corresponding grpc server type
    pub fn into_grpc_server(self) -> NodeGrpcServer<Self> {
        NodeGrpcServer::new(self)
    }
}

#[tonic::async_trait]
impl NodeGrpc for NodeServer {
    async fn get_nodes(
        &self,
        request: Request<GetNodesRequest>,
    ) -> Result<tonic::Response<node::GetNodesReply>, tonic::Status> {
        let req: GetNodesRequest = request.into_inner();
        let filter = req.filter.map(Into::into).unwrap_or_default();
        match self.service.get(filter, None).await {
            Ok(nodes) => Ok(Response::new(GetNodesReply {
                reply: Some(get_nodes_reply::Reply::Nodes(nodes.into())),
            })),
            Err(err) => Ok(Response::new(GetNodesReply {
                reply: Some(get_nodes_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn probe(
        &self,
        _request: tonic::Request<ProbeRequest>,
    ) -> Result<tonic::Response<ProbeResponse>, tonic::Status> {
        match self.service.probe(None).await {
            Ok(resp) => Ok(Response::new(ProbeResponse { ready: resp })),
            Err(_) => Ok(Response::new(ProbeResponse { ready: false })),
        }
    }
    async fn get_block_devices(
        &self,
        _request: tonic::Request<GetBlockDevicesRequest>,
    ) -> Result<tonic::Response<GetBlockDevicesReply>, tonic::Status> {
        todo!()
    }
}
