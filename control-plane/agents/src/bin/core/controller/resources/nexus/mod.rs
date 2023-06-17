use super::{ResourceMutex, ResourceUid};
use stor_port::types::v0::{
    store::nexus::{NexusSpec, NexusState},
    transport::{NexusId, RebuildHistory},
};

impl ResourceMutex<NexusSpec> {
    /// Get the resource uuid.
    pub fn uuid(&self) -> &NexusId {
        &self.immutable_ref().uuid
    }
}

impl ResourceUid for ResourceMutex<NexusSpec> {
    type Uid = NexusId;
    fn uid(&self) -> &Self::Uid {
        &self.immutable_ref().uuid
    }
}

impl ResourceUid for NexusSpec {
    type Uid = NexusId;
    fn uid(&self) -> &Self::Uid {
        &self.uuid
    }
}

impl ResourceUid for RebuildHistory {
    type Uid = NexusId;
    fn uid(&self) -> &Self::Uid {
        &self.uuid
    }
}

impl ResourceUid for NexusState {
    type Uid = NexusId;
    fn uid(&self) -> &Self::Uid {
        &self.nexus.uuid
    }
}

macro_rules! nexus_log {
    ($Self:tt, $Level:expr, $Message:tt) => {
        match tracing::Span::current().field("nexus.uuid") {
            None => {
                if let Some(volume_uuid) = &$Self.owner {
                    let _span = tracing::span!($Level, "log_event", volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid).entered();
                    tracing::event!($Level, volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid, $Message);
                } else {
                    let _span = tracing::span!($Level, "log_event", nexus.uuid = %$Self.uuid).entered();
                    tracing::event!($Level, nexus.uuid = %$Self.uuid, $Message);
                }
            }
            Some(_) => {
                if let Some(volume_uuid) = &$Self.owner {
                    tracing::event!($Level, volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid, $Message);
                } else {
                    tracing::event!($Level, nexus.uuid = %$Self.uuid, $Message);
                }
            }
        }
    };
}

crate::impl_trace_str_log!(nexus_log, NexusSpec);

macro_rules! nexus_span {
    ($Self:tt, $Level:expr, $func:expr) => {
        match tracing::Span::current().field("nexus.uuid") {
            None => {
                if let Some(volume_uuid) = &$Self.owner {
                    let _span = tracing::span!($Level, "log_event", volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid, nexus.node.id = %$Self.node).entered();
                    $func();
                } else {
                    let _span = tracing::span!($Level, "log_event", nexus.uuid = %$Self.uuid, nexus.node.id = %$Self.node).entered();
                    $func();
                }
            }
            Some(_) => {
                // todo: check for volume uuid as well!
                $func();
            }
        }
    };
}
crate::impl_trace_span!(nexus_span, NexusSpec);
