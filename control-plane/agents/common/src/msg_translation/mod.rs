use crate::errors::SvcError;

/// translate between transport types and v0 gRPC type
pub mod v0;

/// translate between transport types and v1 grpc type
pub mod v1;

/// Trait for converting io-engine messages to agent messages, fallibly.
pub trait TryIoEngineToAgent {
    /// Message bus message type.
    type AgentMessage;
    /// Conversion of io-engine message to agent message.
    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError>;
}

/// Trait for converting io-engine messages to agent messages.
pub trait IoEngineToAgent {
    /// Message bus message type.
    type AgentMessage;
    /// Conversion of io-engine message to agent message.
    fn to_agent(&self) -> Self::AgentMessage;
}
