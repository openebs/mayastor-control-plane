use crate::{
    context::{Client, Context, TracedChannel},
    ha_cluster_agent::ha_cluster_rpc_client::HaClusterRpcClient,
    ha_node_agent::{get_nvme_controller_response::Reply, ha_node_rpc_client::HaNodeRpcClient},
    operations::ha_node::traits::{
        ClusterAgentOperations, GetControllerInfo, NodeAgentOperations, NodeInfo, ReplacePathInfo,
        ReportFailedPathsInfo,
    },
};
use common_lib::{
    transport_api::{v0::NvmeSubsystems, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::transport::MessageIdVs,
};
use std::ops::Deref;
use tonic::transport::Uri;

/// Cluster-Agent RPC client
pub struct ClusterAgentClient {
    inner: Client<HaClusterRpcClient<TracedChannel>>,
}

impl ClusterAgentClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, HaClusterRpcClient::new).await;
        Self { inner: client }
    }
}

impl Deref for ClusterAgentClient {
    type Target = Client<HaClusterRpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[tonic::async_trait]
impl ClusterAgentOperations for ClusterAgentClient {
    #[tracing::instrument(
        name = "ClusterAgentClient::register",
        level = "debug",
        skip(self),
        err
    )]
    async fn register(
        &self,
        request: &dyn NodeInfo,
        context: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, context, MessageIdVs::RegisterHaNode);
        let _ = self.client().register_node_agent(req).await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "ClusterAgentClient::report_failed_nvme_paths",
        level = "debug",
        skip(self),
        err
    )]
    /// Report failed NVMe paths.
    async fn report_failed_nvme_paths(
        &self,
        request: &dyn ReportFailedPathsInfo,
        context: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, context, MessageIdVs::ReportFailedPaths);
        self.client().report_failed_nvme_paths(req).await?;
        Ok(())
    }
}

/// Node agent RPC Client.
pub struct NodeAgentClient {
    inner: Client<HaNodeRpcClient<TracedChannel>>,
}

impl NodeAgentClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, HaNodeRpcClient::new).await;
        Self { inner: client }
    }
}

impl Deref for NodeAgentClient {
    type Target = Client<HaNodeRpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[tonic::async_trait]
impl NodeAgentOperations for NodeAgentClient {
    #[tracing::instrument(
        name = "NodeAgentClient::replace_path",
        level = "debug",
        skip(self),
        err
    )]
    async fn replace_path(
        &self,
        request: &dyn ReplacePathInfo,
        context: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, context, MessageIdVs::ReplacePathInfo);
        match self.client().replace_path(req).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    #[tracing::instrument(
        name = "NodeAgentClient::get_nvme_controller",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_nvme_controller(
        &self,
        request: &dyn GetControllerInfo,
        context: Option<Context>,
    ) -> Result<NvmeSubsystems, ReplyError> {
        let req = self.request(request, context, MessageIdVs::GetNvmeSubsystems);
        match self.client().get_nvme_controller(req).await {
            Ok(get_nvme_controller_response) => {
                match get_nvme_controller_response.into_inner().reply {
                    Some(Reply::NvmeControllers(controllers)) => Ok(controllers.into()),
                    Some(Reply::Error(err)) => Err(err.into()),
                    _ => Err(ReplyError::invalid_response(ResourceKind::Unknown)),
                }
            }
            Err(_) => Err(ReplyError::invalid_response(ResourceKind::Unknown)),
        }
    }
}
