use grpc::{context::Context, operations::dr::traits::DrOperations};
use stor_port::transport_api::ReplyError;

/// The disaster recovery agent.
///
/// Scaffolding: it serves the liveness probe so that it can be deployed, waited on and
/// have its wiring exercised. The replication work it will drive - snapshotting sources on
/// a schedule, following a peer on the destination - is added on top of this.
#[derive(Debug, Clone, Default)]
pub(crate) struct DrAgentSvc {}

impl DrAgentSvc {
    /// Returns a new dr agent service.
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl DrOperations for DrAgentSvc {
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        Ok(true)
    }
}
