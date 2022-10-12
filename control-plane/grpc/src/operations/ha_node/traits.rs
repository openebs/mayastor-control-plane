use crate::{
    context::Context,
    ha_cluster_agent::{
        FailedNvmePath, HaNodeInfo, ReplacePathRequest, ReportFailedNvmePathsRequest,
    },
};
use common_lib::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{
        cluster_agent::NodeAgentInfo, FailedPath, ReplacePath, ReportFailedPaths,
    },
    IntoVec,
};
use std::net::SocketAddr;

/// NodeAgentOperations trait implemented by client which supports cluster-agent operations
#[tonic::async_trait]
pub trait NodeAgentOperations: Send + Sync {
    /// Replace failed NVMe path for target NQN.
    async fn replace_path(
        &self,
        request: &dyn ReplacePathInfo,
        context: Option<Context>,
    ) -> Result<(), ReplyError>;
}

/// ReplacePathInfo trait for the failed path replacement to be implemented by entities
/// which want to use this operation.
pub trait ReplacePathInfo: Send + Sync + std::fmt::Debug {
    /// NQN of the target
    fn target_nqn(&self) -> String;
    /// URI of the new path
    fn new_path(&self) -> String;
}

impl ReplacePathInfo for ReplacePath {
    fn target_nqn(&self) -> String {
        self.target().to_string()
    }

    fn new_path(&self) -> String {
        self.new_path().to_string()
    }
}

impl ReplacePathInfo for ReplacePathRequest {
    fn target_nqn(&self) -> String {
        self.target_nqn.clone()
    }
    fn new_path(&self) -> String {
        self.new_path.clone()
    }
}

impl From<&dyn ReplacePathInfo> for ReplacePathRequest {
    fn from(src: &dyn ReplacePathInfo) -> Self {
        Self {
            target_nqn: src.target_nqn(),
            new_path: src.new_path(),
        }
    }
}

/// ClusterAgentOperations trait implemented by client which supports cluster-agent operations
#[tonic::async_trait]
pub trait ClusterAgentOperations: Send + Sync {
    /// Register node with cluster-agent.
    async fn register(
        &self,
        request: &dyn NodeInfo,
        context: Option<Context>,
    ) -> Result<(), ReplyError>;

    /// Report failed NVMe paths.
    async fn report_failed_nvme_paths(
        &self,
        request: &dyn ReportFailedPathsInfo,
        context: Option<Context>,
    ) -> Result<(), ReplyError>;
}

/// NodeInfo trait for the node-agent registration to be implemented by entities which want to
/// use this operation
pub trait NodeInfo: Send + Sync + std::fmt::Debug {
    /// node name on which node-agent is running
    fn node(&self) -> String;
    /// endpoint of node-agent GRPC server
    fn endpoint(&self) -> SocketAddr;
}

/// Intermediate struct to convert grpc to control plane object.
#[derive(Debug)]
pub struct NodeInfoConv {
    node: String,
    endpoint: SocketAddr,
}

impl NodeInfo for NodeAgentInfo {
    fn node(&self) -> String {
        self.node_name().to_owned()
    }

    fn endpoint(&self) -> SocketAddr {
        self.endpoint()
    }
}

impl TryFrom<HaNodeInfo> for NodeInfoConv {
    type Error = ReplyError;

    fn try_from(value: HaNodeInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            node: value.nodename,
            endpoint: value.endpoint.parse().map_err(|_err| {
                ReplyError::invalid_argument(
                    ResourceKind::Unknown,
                    "endpoint",
                    "Failed parsing node endpoint".to_string(),
                )
            })?,
        })
    }
}

impl NodeInfo for NodeInfoConv {
    fn node(&self) -> String {
        self.node.clone()
    }

    fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

impl From<&dyn NodeInfo> for HaNodeInfo {
    fn from(src: &dyn NodeInfo) -> Self {
        Self {
            nodename: src.node(),
            endpoint: src.endpoint().to_string(),
        }
    }
}

/// Trait to be implemented for ReportFailedNvmePaths operation.
pub trait ReportFailedPathsInfo: Send + Sync + std::fmt::Debug {
    /// Id of the application node.
    fn node(&self) -> String;
    /// Node agent's Grpc address
    fn endpoint(&self) -> SocketAddr;
    /// List of failed NVMe paths.
    fn failed_paths(&self) -> Vec<FailedPath>;
}

impl ReportFailedPathsInfo for ReportFailedPaths {
    fn node(&self) -> String {
        self.node_name().to_string()
    }

    fn endpoint(&self) -> SocketAddr {
        self.endpoint()
    }
    fn failed_paths(&self) -> Vec<FailedPath> {
        self.failed_paths().clone()
    }
}

impl ReportFailedPathsInfo for ReportFailedNvmePathsRequest {
    fn node(&self) -> String {
        self.nodename.clone()
    }

    fn endpoint(&self) -> SocketAddr {
        self.endpoint
            .parse::<SocketAddr>()
            .expect("Could not get node agent's grpc address")
    }

    fn failed_paths(&self) -> Vec<FailedPath> {
        self.failed_paths
            .iter()
            .map(|p| FailedPath::new(p.target_nqn.to_string()))
            .collect()
    }
}

impl From<&dyn ReportFailedPathsInfo> for ReportFailedNvmePathsRequest {
    fn from(info: &dyn ReportFailedPathsInfo) -> Self {
        Self {
            nodename: info.node(),
            endpoint: info.endpoint().to_string(),
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
