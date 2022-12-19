use tonic::{Request, Response, Status};

use crate::{
    ha_cluster_agent::{
        ha_cluster_rpc_server::{HaClusterRpc, HaClusterRpcServer},
        HaNodeInfo, ReportFailedNvmePathsRequest,
    },
    ha_node_agent::{
        get_nvme_controller_response,
        ha_node_rpc_server::{HaNodeRpc, HaNodeRpcServer},
        GetNvmeControllerRequest, GetNvmeControllerResponse, ReplacePathRequest,
    },
    operations::ha_node::traits::{ClusterAgentOperations, NodeAgentOperations, NodeInfoConv},
};
use std::sync::Arc;

/// RPC cluster-node server
pub struct NodeAgentServer {
    service: Arc<dyn NodeAgentOperations>,
}

impl NodeAgentServer {
    /// returns a new cluster-agent server with the service implementing cluster-agent operations
    pub fn new(svc: Arc<dyn NodeAgentOperations>) -> Self {
        Self { service: svc }
    }

    /// converts the node-agent server to corresponding grpc server type
    pub fn into_grpc_server(self) -> HaNodeRpcServer<Self> {
        HaNodeRpcServer::new(self)
    }
}

#[tonic::async_trait]
impl HaNodeRpc for NodeAgentServer {
    async fn replace_path(
        &self,
        request: tonic::Request<ReplacePathRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let msg = request.into_inner();

        match self.service.replace_path(&msg, None).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(err.into()),
        }
    }

    async fn get_nvme_controller(
        &self,
        request: Request<GetNvmeControllerRequest>,
    ) -> Result<Response<GetNvmeControllerResponse>, Status> {
        let msg = request.into_inner();
        match self.service.get_nvme_controller(&msg, None).await {
            Ok(val) => Ok(Response::new(GetNvmeControllerResponse {
                reply: Some(get_nvme_controller_response::Reply::NvmeControllers(
                    val.into(),
                )),
            })),
            Err(err) => Err(err.into()),
        }
    }
}

/// RPC cluster-agent server
pub struct ClusterAgentServer {
    service: Arc<dyn ClusterAgentOperations>,
}

impl ClusterAgentServer {
    /// returns a new cluster-agent server with the service implementing cluster-agent operations
    pub fn new(svc: Arc<dyn ClusterAgentOperations>) -> Self {
        ClusterAgentServer { service: svc }
    }

    /// converts the cluster-agent server to corresponding grpc server type
    pub fn into_grpc_server(self) -> HaClusterRpcServer<Self> {
        HaClusterRpcServer::new(self)
    }
}

#[tonic::async_trait]
impl HaClusterRpc for ClusterAgentServer {
    async fn register_node_agent(
        &self,
        request: tonic::Request<HaNodeInfo>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let nodeinfo = request.into_inner();
        match self
            .service
            .register(&NodeInfoConv::try_from(nodeinfo)?, None)
            .await
        {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(Status::internal(format!(
                "Failed to register node-agent: {:?}",
                err
            ))),
        }
    }
    async fn report_failed_nvme_paths(
        &self,
        request: tonic::Request<ReportFailedNvmePathsRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let pathinfo = request.into_inner();
        match self.service.report_failed_nvme_paths(&pathinfo, None).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(Status::internal(format!(
                "Failed to report path: {:?}",
                err
            ))),
        }
    }
}
