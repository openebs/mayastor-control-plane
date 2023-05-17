mod snapshot;

use super::{ResourceMutex, ResourceUid};
use stor_port::types::v0::{
    store::volume::{AffinityGroupSpec, VolumeSpec},
    transport::VolumeId,
};

impl ResourceMutex<VolumeSpec> {
    /// Get the resource uuid.
    pub(crate) fn uuid(&self) -> &VolumeId {
        &self.immutable_ref().uuid
    }
}
impl ResourceUid for VolumeSpec {
    type Uid = VolumeId;
    fn uid(&self) -> &Self::Uid {
        &self.uuid
    }
}

impl ResourceUid for AffinityGroupSpec {
    type Uid = String;
    fn uid(&self) -> &Self::Uid {
        self.id()
    }
}

macro_rules! volume_log {
    ($Self:tt, $Level:expr, $Message:tt) => {
        match tracing::Span::current().field("volume.uuid") {
            None => {
                let _span = tracing::span!($Level, "log_event", volume.uuid = %$Self.uuid).entered();
                tracing::event!($Level, volume.uuid = %$Self.uuid, $Message);
            }
            Some(_) => {
                tracing::event!($Level, volume.uuid = %$Self.uuid, $Message);
            }
        }
    };
}
crate::impl_trace_str_log!(volume_log, VolumeSpec);

macro_rules! volume_span {
    ($Self:tt, $Level:expr, $func:expr) => {
        match tracing::Span::current().field("volume.uuid") {
            None => {
                let _span = tracing::span!($Level, "log_event", volume.uuid = %$Self.uuid).entered();
                $func();
            }
            Some(_) => {
                $func();
            }
        }
    };
}
crate::impl_trace_span!(volume_span, VolumeSpec);
