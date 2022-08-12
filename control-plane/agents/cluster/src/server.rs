use anyhow::anyhow;
use common_lib::transport_api::{ReplyError, ResourceKind};
use grpc::operations::ha_node::{
    server::ClusterAgentServer,
    traits::{ClusterAgentOperations, NodeInfo},
};
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::Server;

pub struct ClusterAgent {
    endpoint: SocketAddr,
}

impl ClusterAgent {
    pub fn new(endpoint: SocketAddr) -> Self {
        ClusterAgent { endpoint }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let r = ClusterAgentServer::new(Arc::new(ClusterAgentSvc {}));
        Server::builder()
            .add_service(r.into_grpc_server())
            .serve(self.endpoint)
            .await
            .map_err(|err| anyhow!("Failed to start server: {err}"))
    }
}

struct ClusterAgentSvc {}

#[tonic::async_trait]
impl ClusterAgentOperations for ClusterAgentSvc {
    async fn register(&self, request: &dyn NodeInfo) -> Result<(), ReplyError> {
        if request.node().is_empty() {
            return Err(ReplyError::missing_argument(
                ResourceKind::Unknown,
                "node_name",
            ));
        }

        if request.endpoint().is_empty() {
            return Err(ReplyError::missing_argument(
                ResourceKind::Unknown,
                "endpoint",
            ));
        }

        tracing::trace!(agent = request.node(), "node successfully registered");
        Ok(())
    }
}
