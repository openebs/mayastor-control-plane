use crate::{nodes::NodeList, volume::VolumeMover};
use grpc::{
    context::Context,
    operations::ha_node::{
        server::ClusterAgentServer,
        traits::{ClusterAgentOperations, NodeInfo, ReportFailedPathsInfo},
    },
};
use std::{net::SocketAddr, sync::Arc};
use stor_port::transport_api::{ReplyError, ReplyErrorKind, ResourceKind};

/// High-level object that represents HA Cluster agent gRPC server.
pub(crate) struct ClusterAgent {
    endpoint: SocketAddr,
    nodes: NodeList,
    mover: VolumeMover,
}

impl ClusterAgent {
    /// Returns a new `Self` with the given parameters.
    pub(crate) fn new(endpoint: SocketAddr, nodes: NodeList, mover: VolumeMover) -> Self {
        ClusterAgent {
            endpoint,
            nodes,
            mover,
        }
    }
    /// Runs this server as a future until a shutdown signal is received.
    pub(crate) async fn run(self) -> Result<(), agents::ServiceError> {
        let r = ClusterAgentServer::new(Arc::new(ClusterAgentSvc {
            nodes: self.nodes,
            mover: self.mover,
        }));
        agents::Service::builder()
            .with_service(r.into_grpc_server())
            .run_err(self.endpoint)
            .await
    }
}

struct ClusterAgentSvc {
    nodes: NodeList,
    mover: VolumeMover,
}

#[tonic::async_trait]
impl ClusterAgentOperations for ClusterAgentSvc {
    #[tracing::instrument(level = "info", skip(self), err, fields(node.id = %request.node(), node.endpoint = %request.endpoint()))]
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
        self.nodes
            .register_node(request.node().into(), request.endpoint())
            .await;
        tracing::trace!(agent = request.node(), "node successfully registered");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), err, fields(node.id = %request.node()))]
    async fn report_failed_nvme_paths(
        &self,
        request: &dyn ReportFailedPathsInfo,
        _context: Option<Context>,
    ) -> Result<(), ReplyError> {
        let mut v: Vec<(String, String)> = Vec::new();

        for x in request.failed_paths().into_iter() {
            let nodes = self.nodes.clone();
            match nodes
                .report_failed_path(
                    request.node().into(),
                    x.target_nqn().to_string(),
                    self.mover.clone(),
                    request.endpoint(),
                )
                .await
            {
                Ok(_) => continue,
                Err(err) => v.push((x.target_nqn().to_string(), err.to_string())),
            }
        }

        if !v.is_empty() {
            let mut e = ReplyError {
                kind: ReplyErrorKind::WithMessage,
                resource: ResourceKind::Unknown,
                source: "".into(),
                extra: "".into(),
            };
            v.iter().for_each(|x| {
                e.extend(&x.0, &x.1);
            });
            return Err(e);
        }
        Ok(())
    }
}
