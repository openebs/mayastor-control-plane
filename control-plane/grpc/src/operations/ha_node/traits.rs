use crate::ha_cluster_agent::{
    FailedNvmePath, HaNodeInfo, ReplacePathRequest, ReportFailedNvmePathsRequest,
};
use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{cluster_agent::NodeAgentInfo, FailedPath, ReportFailedPaths},
    IntoVec,
};

/// NodeAgentOperations trait implemented by client which supports cluster-agent operations
#[tonic::async_trait]
pub trait NodeAgentOperations: Send + Sync {
    /// Replace failed NVMe path for target NQN.
    async fn replace_path(&self, request: &dyn ReplacePathInfo) -> Result<(), ReplyError>;
}

/// ReplacePathInfo trait for the failed path replacement to be implemented by entities
/// which want to use this operation.
pub trait ReplacePathInfo: Send + Sync + std::fmt::Debug {
    /// NQN of the target
    fn target_nqn(&self) -> String;
    /// URI of the new path
    fn new_path(&self) -> String;
}

impl ReplacePathInfo for ReplacePathRequest {
    fn target_nqn(&self) -> String {
        self.target_nqn.clone()
    }

    fn new_path(&self) -> String {
        self.new_path.clone()
    }
}

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
    /// Id of the application node.
    fn node(&self) -> String;

    /// List of failed NVMe paths.
    fn failed_paths(&self) -> Vec<FailedPath>;
}

impl ReportFailedPathsInfo for ReportFailedPaths {
    fn node(&self) -> String {
        self.node_name().to_string()
    }

    fn failed_paths(&self) -> Vec<FailedPath> {
        self.failed_paths().clone()
    }
}

impl From<&dyn ReportFailedPathsInfo> for ReportFailedNvmePathsRequest {
    fn from(info: &dyn ReportFailedPathsInfo) -> Self {
        Self {
            nodename: info.node(),
            failed_paths: info.failed_paths().into_vec(),
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
