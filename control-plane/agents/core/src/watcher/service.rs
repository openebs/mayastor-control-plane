use crate::{
    core::registry::Registry,
    watcher::watch::{StoreWatcher, WatchCfgId},
};
pub use common::errors::SvcError;
pub use common_lib::mbus_api::{Message, MessageId, ReceivedMessage};
use common_lib::{
    mbus_api::message_bus::v0::Watches,
    types::v0::message_bus::mbus::{CreateWatch, DeleteWatch, GetWatchers},
};
pub use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub(super) struct Service {
    registry: Registry,
    watcher: Arc<Mutex<StoreWatcher>>,
}

/// Watcher Agent's Service
impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            watcher: Arc::new(Mutex::new(StoreWatcher::new(registry.clone()))),
            registry,
        }
    }

    /// Create new resource watch
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_watch(&self, request: &CreateWatch) -> Result<(), SvcError> {
        self.watcher
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
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_watchers(&self, request: &GetWatchers) -> Result<Watches, SvcError> {
        self.watcher
            .lock()
            .await
            .get_watchers(&WatchCfgId::from(request))
            .await
    }

    /// Delete resource watch
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn delete_watch(&self, request: &DeleteWatch) -> Result<(), SvcError> {
        self.watcher
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
