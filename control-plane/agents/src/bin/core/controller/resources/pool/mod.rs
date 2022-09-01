use super::{ResourceMutex, ResourceUid};
use common_lib::types::v0::{
    store::pool::{PoolSpec, PoolState},
    transport::PoolId,
};

impl ResourceMutex<PoolSpec> {
    /// Get the resource id.
    pub fn id(&mut self) -> &PoolId {
        &self.immutable_ref().id
    }
}

impl ResourceUid for PoolSpec {
    type Uid = PoolId;
    fn uid(&self) -> &Self::Uid {
        &self.id
    }
}

impl ResourceUid for PoolState {
    type Uid = PoolId;
    fn uid(&self) -> &Self::Uid {
        &self.pool.id
    }
}

macro_rules! pool_span {
    ($Self:tt, $Level:expr, $func:expr) => {
        match tracing::Span::current().field("pool.id") {
            None => {
                let _span = tracing::span!($Level, "log_event", pool.id = %$Self.id).entered();
                $func();
            }
            Some(_) => {
                $func();
            }
        }
    };
}
crate::impl_trace_span!(pool_span, PoolSpec);
