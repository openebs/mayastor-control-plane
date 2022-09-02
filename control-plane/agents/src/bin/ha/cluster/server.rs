use common_lib::transport_api::{ReplyError, ResourceKind};
use grpc::{
    context::Context,
    operations::ha_node::{
        server::ClusterAgentServer,
        traits::{ClusterAgentOperations, NodeInfo, ReportFailedPathsInfo},
    },
};
use std::{net::SocketAddr, sync::Arc};

/// High-level object that represents HA Cluster agent gRPC server.
pub(crate) struct ClusterAgent {
    endpoint: SocketAddr,
}

impl ClusterAgent {
    /// Returns a new `Self` with the given parameters.
    pub(crate) fn new(endpoint: SocketAddr) -> Self {
        ClusterAgent { endpoint }
    }
    /// Runs this server as a future until a shutdown signal is received.
    pub(crate) async fn run(&self) -> Result<(), agents::ServiceError> {
        let r = ClusterAgentServer::new(Arc::new(ClusterAgentSvc {}));
        agents::Service::builder()
            .with_service(r.into_grpc_server())
            .run_err(self.endpoint)
            .await
    }
}

struct ClusterAgentSvc {}

#[tonic::async_trait]
impl ClusterAgentOperations for ClusterAgentSvc {
    async fn register(
        &self,
        request: &dyn NodeInfo,
        _context: Option<Context>,
    ) -> Result<(), ReplyError> {
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

    async fn report_failed_nvme_paths(
        &self,
        _request: &dyn ReportFailedPathsInfo,
        _context: Option<Context>,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::unimplemented(
            "NVMe path reporting is not yet implemented".to_string(),
        ))
    }
}
