use common_lib::transport_api::ReplyError;
use tonic::transport::Uri;

/// ClusterAgentOperations trait implemented by client which supports cluster-agent operations
#[tonic::async_trait]
pub trait ClusterAgentOperations: Send + Sync {
    /// Register node with cluster-agent
    async fn register(&self, node_name: String, endpoint: Uri) -> Result<(), ReplyError>;
}
