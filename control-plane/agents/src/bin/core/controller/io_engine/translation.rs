use agents::errors::SvcError;

/// Trait for converting io-engine messages to agent messages, fallibly.
pub(super) trait TryIoEngineToAgent {
    /// Agents message type.
    type AgentMessage;
    /// Conversion of io-engine message to agent message.
    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError>;
}

/// Trait for converting io-engine messages to agent messages.
pub(super) trait IoEngineToAgent {
    /// Agents message type.
    type AgentMessage;
    /// Conversion of io-engine message to agent message.
    fn to_agent(&self) -> Self::AgentMessage;
}
