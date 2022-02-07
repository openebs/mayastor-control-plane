use common_lib::{mbus_api::TimeoutOptions, types::v0::message_bus::MessageIdVs};
use std::time::Duration;

/// Request specific minimum timeouts
/// zeroing replicas on create/destroy takes some time (observed up to 7seconds)
/// nexus creation by itself can take up to 4 seconds... it can take even longer if etcd is not up
#[derive(Debug, Clone)]
pub struct RequestMinTimeout {
    replica: Duration,
    nexus: Duration,
}

impl Default for RequestMinTimeout {
    fn default() -> Self {
        Self {
            replica: Duration::from_secs(10),
            nexus: Duration::from_secs(30),
        }
    }
}
impl RequestMinTimeout {
    /// minimum timeout for a replica operation
    pub fn replica(&self) -> Duration {
        self.replica
    }
    /// minimum timeout for a nexus operation
    pub fn nexus(&self) -> Duration {
        self.nexus
    }
}

/// get the default timeout for each type of request if a timeout is not specified.
/// timeouts vary with different types of requests
pub fn timeout_grpc(op_id: MessageIdVs, min_timeout: Duration) -> Duration {
    let base_timeout = RequestMinTimeout::default();
    let timeout = match op_id {
        MessageIdVs::CreateVolume => base_timeout.replica() * 3 + base_timeout.nexus(),
        MessageIdVs::DestroyVolume => base_timeout.replica() * 3 + base_timeout.nexus(),
        MessageIdVs::PublishVolume => base_timeout.nexus(),
        MessageIdVs::UnpublishVolume => base_timeout.nexus(),

        MessageIdVs::CreateNexus => base_timeout.nexus(),
        MessageIdVs::DestroyNexus => base_timeout.nexus(),

        MessageIdVs::CreateReplica => base_timeout.replica(),
        MessageIdVs::DestroyReplica => base_timeout.replica(),
        _ => min_timeout,
    };
    timeout.max(min_timeout).min(Duration::from_secs(59))
}

/// context to be sent along with each request encapsulating the extra add ons that changes the
/// behaviour of each request.
pub struct Context {
    timeout_opts: Option<TimeoutOptions>,
}
impl Context {
    /// generate a new context with the provided timeout options
    pub fn new(timeout_opts: impl Into<Option<TimeoutOptions>>) -> Self {
        Self {
            timeout_opts: timeout_opts.into(),
        }
    }
    pub fn timeout_opts(&self) -> Option<TimeoutOptions> {
        self.timeout_opts.clone()
    }
}
