use crate::context::Context;
use stor_port::transport_api::ReplyError;

/// All dr agent operations to be a part of the DrOperations trait.
///
/// The agent is driven by its own schedule rather than by callers, so for now this carries
/// only the liveness probe. Operations that manage relationships and failover join it as
/// they are built.
#[tonic::async_trait]
pub trait DrOperations: Send + Sync {
    /// Liveness probe for the dr service.
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
}
