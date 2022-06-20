use crate::{
    core::registry::Registry,
    watch::watches::{StoreWatch, WatchCfgId},
};
pub use common::errors::SvcError;
pub use common_lib::mbus_api::{Message, MessageId, ReceivedMessage};
use common_lib::{
    mbus_api::{message_bus::v0::Watches, ReplyError},
    types::v0::message_bus::{CreateWatch, DeleteWatch, GetWatches},
};
use grpc::{
    context::Context,
    operations::watch::traits::{GetWatchInfo, WatchInfo, WatchOperations},
};
pub use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub(super) struct Service {
    watch: Arc<Mutex<StoreWatch>>,
}

#[tonic::async_trait]
impl WatchOperations for Service {
    async fn create(&self, req: &dyn WatchInfo, _ctx: Option<Context>) -> Result<(), ReplyError> {
        let create_watch = req.into();
        let service = self.clone();
        Context::spawn(async move { service.create_watch(&create_watch).await }).await??;
        Ok(())
    }

    async fn get(
        &self,
        req: &dyn GetWatchInfo,
        _ctx: Option<Context>,
    ) -> Result<Watches, ReplyError> {
        let get_watches = req.into();
        let watches = self.get_watches(&get_watches).await?;
        Ok(watches)
    }

    async fn destroy(&self, req: &dyn WatchInfo, _ctx: Option<Context>) -> Result<(), ReplyError> {
        let destroy_watch = req.into();
        let service = self.clone();
        Context::spawn(async move { service.delete_watch(&destroy_watch).await }).await??;
        Ok(())
    }
}

/// Watch Agent's Service
impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            watch: Arc::new(Mutex::new(StoreWatch::new(registry))),
        }
    }

    /// Create new resource watch
    #[tracing::instrument(level = "debug", skip(self), err)]
    pub(super) async fn create_watch(&self, request: &CreateWatch) -> Result<(), SvcError> {
        self.watch
            .lock()
            .await
            .create_watch(
                &WatchCfgId::from(request),
                &request.callback,
                &request.watch_type,
            )
            .await?;
        Ok(())
    }

    /// Get resource watch
    #[tracing::instrument(level = "debug", skip(self), err)]
    pub(super) async fn get_watches(&self, request: &GetWatches) -> Result<Watches, SvcError> {
        self.watch
            .lock()
            .await
            .get_watches(&WatchCfgId::from(request))
            .await
    }

    /// Delete resource watch
    #[tracing::instrument(level = "debug", skip(self), err)]
    pub(super) async fn delete_watch(&self, request: &DeleteWatch) -> Result<(), SvcError> {
        self.watch
            .lock()
            .await
            .delete_watch(
                &WatchCfgId::from(request),
                &request.callback,
                &request.watch_type,
            )
            .await
    }
}
