use crate::{
    blockdevice::{get_block_devices_reply, GetBlockDevicesReply, GetBlockDevicesRequest},
    node,
    node::{
        cordon_node_reply, drain_node_reply, get_nodes_reply,
        node_grpc_server::{NodeGrpc, NodeGrpcServer},
        uncordon_node_reply, CordonNodeReply, CordonNodeRequest, DrainNodeReply, DrainNodeRequest,
        GetNodesReply, GetNodesRequest, ProbeRequest, ProbeResponse, UncordonNodeReply,
        UncordonNodeRequest,
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
        request: tonic::Request<GetBlockDevicesRequest>,
    ) -> Result<tonic::Response<GetBlockDevicesReply>, tonic::Status> {
        let req: GetBlockDevicesRequest = request.into_inner();
        match self.service.get_block_devices(&req, None).await {
            Ok(blockdevices) => Ok(Response::new(GetBlockDevicesReply {
                reply: Some(get_block_devices_reply::Reply::Blockdevices(
                    blockdevices.into(),
                )),
            })),
            Err(err) => Ok(Response::new(GetBlockDevicesReply {
                reply: Some(get_block_devices_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn cordon_node(
        &self,
        request: tonic::Request<CordonNodeRequest>,
    ) -> Result<tonic::Response<CordonNodeReply>, tonic::Status> {
        let req: CordonNodeRequest = request.into_inner();
        match self.service.cordon(req.node_id.into(), req.label).await {
            Ok(node) => Ok(Response::new(CordonNodeReply {
                reply: Some(cordon_node_reply::Reply::Node(node.into())),
            })),
            Err(err) => Ok(Response::new(CordonNodeReply {
                reply: Some(cordon_node_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn uncordon_node(
        &self,
        request: tonic::Request<UncordonNodeRequest>,
    ) -> Result<tonic::Response<UncordonNodeReply>, tonic::Status> {
        let req: UncordonNodeRequest = request.into_inner();
        match self.service.uncordon(req.node_id.into(), req.label).await {
            Ok(node) => Ok(Response::new(UncordonNodeReply {
                reply: Some(uncordon_node_reply::Reply::Node(node.into())),
            })),
            Err(err) => Ok(Response::new(UncordonNodeReply {
                reply: Some(uncordon_node_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn drain_node(
        &self,
        request: tonic::Request<DrainNodeRequest>,
    ) -> Result<tonic::Response<DrainNodeReply>, tonic::Status> {
        let req: DrainNodeRequest = request.into_inner();
        match self.service.drain(req.node_id.into(), req.label).await {
            Ok(node) => Ok(Response::new(DrainNodeReply {
                reply: Some(drain_node_reply::Reply::Node(node.into())),
            })),
            Err(err) => Ok(Response::new(DrainNodeReply {
                reply: Some(drain_node_reply::Reply::Error(err.into())),
            })),
        }
    }
}
