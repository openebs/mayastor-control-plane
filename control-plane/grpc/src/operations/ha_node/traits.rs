use crate::ha_cluster_agent::{FailedNvmePath, HaNodeInfo, ReportFailedNvmePathsRequest};
use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{cluster_agent::NodeAgentInfo, FailedPath, ReportFailedPaths},
};

/// ClusterAgentOperations trait implemented by client which supports cluster-agent operations
#[tonic::async_trait]
pub trait ClusterAgentOperations: Send + Sync {
    /// Register node with cluster-agent
    async fn register(&self, request: &dyn NodeInfo) -> Result<(), ReplyError>;

    /// Report failed NVMe paths.
    async fn report_failed_nvme_paths(
        &self,
        request: &dyn ReportFailedPathsInfo,
    ) -> Result<(), ReplyError>;
}

/// NodeInfo trait for the node-agent registration to be implemented by entities which want to
/// use this operation
pub trait NodeInfo: Send + Sync + std::fmt::Debug {
    /// node name on which node-agent is running
    fn node(&self) -> String;
    /// endpoint of node-agent GRPC server
    fn endpoint(&self) -> String;
}

impl NodeInfo for NodeAgentInfo {
    fn node(&self) -> String {
        self.node_name().to_owned()
    }

    fn endpoint(&self) -> String {
        self.endpoint().to_string()
    }
}

impl NodeInfo for HaNodeInfo {
    fn node(&self) -> String {
        self.nodename.clone()
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
}

/// Trait to be implemented for ReportFailedNvmePaths operation.
pub trait ReportFailedPathsInfo: Send + Sync + std::fmt::Debug {
    /// Id of the io-engine instance
    fn node(&self) -> String;

    /// List of failed NVMe paths.
    fn failed_paths(&self) -> Vec<FailedPath>;
}

impl ReportFailedPathsInfo for ReportFailedPaths {
    fn node(&self) -> String {
        self.node_name().to_string()
    }

    fn failed_paths(&self) -> Vec<FailedPath> {
        self.failed_paths()
    }
}

impl From<&dyn ReportFailedPathsInfo> for ReportFailedNvmePathsRequest {
    fn from(info: &dyn ReportFailedPathsInfo) -> Self {
        Self {
            nodename: info.node(),
            failed_paths: info.failed_paths().into_iter().map(|p| p.into()).collect(),
        }
    }
}

impl From<FailedPath> for FailedNvmePath {
    fn from(path: FailedPath) -> Self {
        Self {
            target_nqn: path.target_nqn().to_string(),
        }
    }
}
