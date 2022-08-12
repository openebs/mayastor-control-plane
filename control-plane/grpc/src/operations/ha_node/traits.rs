use crate::ha_cluster_agent::HaNodeInfo;
use common_lib::{transport_api::ReplyError, types::v0::transport::cluster_agent::NodeAgentInfo};

/// ClusterAgentOperations trait implemented by client which supports cluster-agent operations
#[tonic::async_trait]
pub trait ClusterAgentOperations: Send + Sync {
    /// Register node with cluster-agent
    async fn register(&self, request: &dyn NodeInfo) -> Result<(), ReplyError>;
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
