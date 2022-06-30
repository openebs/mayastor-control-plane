use crate::core::registry::Registry;
use common::errors::{Store as SvcStoreError, SvcError};
use common_lib::{
    mbus_api::{message_bus::v0::Watches, ResourceKind},
    types::v0::{
        message_bus::{
            CreateWatch, DeleteWatch, GetWatches, Watch, WatchCallback, WatchResourceId, WatchType,
        },
        store::definitions::{
            ObjectKey, StorableObject, StorableObjectType, Store, StoreError, StoreWatchReceiver,
            WatchEvent,
        },
    },
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{
    cmp::min,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{broadcast::error::TryRecvError, Mutex},
    task::JoinHandle,
};

impl ObjectKey for WatchCfgId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::WatchConfig
    }
    fn key_uuid(&self) -> String {
        self.id.to_string()
    }
}

/// Watch configuration with the resource as a key
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct WatchCfg {
    pub watch_id: WatchCfgId,
    pub watches: Vec<WatchParamsCfg>,
}
impl StorableObject for WatchCfg {
    type Key = WatchCfgId;
    fn key(&self) -> Self::Key {
        self.watch_id.clone()
    }
}

/// Configurable Watch parameters
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct WatchParams {
    /// Watch callback (eg url)
    callback: WatchCallback,
    /// Type of event to watch
    #[serde(rename = "type")]
    type_: WatchType,
}

/// Watch parameters with handle to the watch worker thread
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct WatchParamsCfg {
    /// inner configurable watch parameters
    params: WatchParams,
    /// handle to the watch (logic on the drop)
    #[serde(skip)]
    #[allow(dead_code)]
    handle: Option<WatchHandle>,
}

/// Watch Handle to a watch thread with a cancellation channel
type WatchHandle = Arc<(tokio::sync::broadcast::Sender<()>, JoinHandle<()>)>;

impl Deref for WatchParamsCfg {
    type Target = WatchParams;

    fn deref(&self) -> &Self::Target {
        &self.params
    }
}

/// Uniquely identify a watch resource in the store
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WatchCfgId {
    id: WatchResourceId,
}

impl From<&CreateWatch> for WatchCfgId {
    fn from(req: &CreateWatch) -> Self {
        WatchCfgId { id: req.id.clone() }
    }
}
impl From<&GetWatches> for WatchCfgId {
    fn from(req: &GetWatches) -> Self {
        WatchCfgId {
            id: req.resource.clone(),
        }
    }
}
impl From<&DeleteWatch> for WatchCfgId {
    fn from(req: &DeleteWatch) -> Self {
        WatchCfgId { id: req.id.clone() }
    }
}

/// In memory record of existing watches
/// Gets populated on startup by reading from the store
#[derive(Debug, Clone)]
pub(crate) struct StoreWatch {
    /// clone of the core registry
    pub(crate) registry: Registry,
    /// record of all watches
    watches: Vec<Arc<Mutex<WatchCfg>>>,
}

impl StoreWatch {
    pub fn new(registry: Registry) -> Self {
        Self {
            registry,
            watches: Default::default(),
        }
    }
}

impl WatchCfg {
    /// Create a new Watch configuration for the given `watch_id`
    fn new(watch_id: &WatchCfgId) -> Self {
        WatchCfg {
            watch_id: watch_id.clone(),
            ..Default::default()
        }
    }

    /// Add a new watch element to this watch
    async fn add(
        &mut self,
        watch: &WatchParams,
        store: Arc<Mutex<impl Store + 'static>>,
    ) -> Result<(), SvcError> {
        if self.watches.iter().any(|item| &item.params == watch) {
            return Err(SvcError::WatchAlreadyExists {});
        }

        {
            // make sure the target resource exists
            let mut store = store.lock().await;
            match store.get_kv(&self.watch_id.id.key()).await {
                Ok(_) => Ok(()),
                Err(StoreError::MissingEntry { .. }) => Err(SvcError::WatchResourceNotFound {
                    kind: Self::resource_to_kind(&self.watch_id.id),
                }),
                Err(error) => Err(error.into()),
            }?;
        }

        let handle = self.watch(watch, store).await?;

        let watch = WatchParamsCfg {
            params: watch.clone(),
            handle: Some(handle),
        };
        self.watches.push(watch);
        Ok(())
    }

    /// Map a watch resource to a resource kind
    fn resource_to_kind(resource: &WatchResourceId) -> ResourceKind {
        match &resource {
            WatchResourceId::Node(_) => ResourceKind::Node,
            WatchResourceId::Pool(_) => ResourceKind::Pool,
            WatchResourceId::Replica(_) => ResourceKind::Replica,
            WatchResourceId::ReplicaState(_) => ResourceKind::ReplicaState,
            WatchResourceId::ReplicaSpec(_) => ResourceKind::ReplicaSpec,
            WatchResourceId::Nexus(_) => ResourceKind::Nexus,
            WatchResourceId::Volume(_) => ResourceKind::Volume,
        }
    }

    /// Delete a watch using its parameters
    fn del(&mut self, watch: &WatchParams) -> Result<(), SvcError> {
        if !self.watches.iter().any(|item| &item.params == watch) {
            Err(SvcError::WatchNotFound {})
        } else {
            self.watches.retain(|item| &item.params != watch);
            Ok(())
        }
    }

    /// Register a callback for the element using the store's watch feature
    async fn watch(
        &self,
        watch: &WatchParams,
        store_arc: Arc<Mutex<impl Store + 'static>>,
    ) -> Result<WatchHandle, SvcError> {
        let mut store = store_arc.lock().await;
        let handle = {
            // start watching before writing to the store
            let channel = store.watch_obj(&self.watch_id.id).await?;
            let watch = watch.clone();
            let id = self.watch_id.id.clone();
            let store = store_arc.clone();
            let (cancel_sender, cancel) = tokio::sync::broadcast::channel(1);
            let thread = tokio::spawn(async move {
                Self::watch_worker(cancel, channel, watch, id, store).await;
            });
            Arc::new((cancel_sender, thread))
        };
        // now record the watch in the store
        // if this fails the watch will be cancelled
        store.put_obj(self).await.context(SvcStoreError {})?;
        Ok(handle)
    }

    /// Worker thread which listens for events from the store (etcd) for a
    /// specific watch which is created through `create_watch`.
    async fn watch_worker(
        mut cancel: tokio::sync::broadcast::Receiver<()>,
        mut channel: StoreWatchReceiver,
        params: WatchParams,
        id: WatchResourceId,
        store: Arc<Mutex<impl Store + 'static>>,
    ) {
        let mut last_seen: Option<serde_json::Value> = None;
        loop {
            tokio::select! {
                _cancel = cancel.recv() => {
                    // the watch has been cancelled
                    return;
                },
                watch_event = channel.recv() => {
                    match watch_event {
                        None => {
                            if let Some(chan) =
                                Self::reconnect_watch(&mut cancel, &id, &store).await
                            {
                                if Some(&chan.0) != last_seen.as_ref() {
                                    // we can't know if we missed any event so just
                                    // compare the latest with last seen
                                    Self::notify(&mut cancel, &params.callback).await;
                                }
                                last_seen = Some(chan.0);
                                channel = chan.1;
                            } else {
                                break;
                            }
                        }
                        Some(Err(error)) => {
                            // Should not happen, most likely a deserialize error?
                            tracing::error!("Error watching: {:?}", error);
                        }

                        Some(Ok(result)) => {
                            match &result {
                                WatchEvent::Put(_, v) => {
                                    last_seen = Some(v.clone());
                                }
                                WatchEvent::Delete => {
                                    // resource deleted so we don't need to keep on watching
                                    return;
                                }
                            }
                            Self::notify(&mut cancel, &params.callback).await;
                        }
                    }
                }
            }
        }
    }

    /// Notify the watch using its callback
    async fn notify(cancel: &mut tokio::sync::broadcast::Receiver<()>, callback: &WatchCallback) {
        let mut tries = 0;
        let mut log_failure = true;
        loop {
            match cancel.try_recv() {
                Err(TryRecvError::Empty) => {}
                // dropped or received the cancel signal so bail out
                _ => return,
            };

            match &callback {
                WatchCallback::Uri(uri) => {
                    let request = reqwest::Client::new()
                        .put(uri)
                        .timeout(std::time::Duration::from_secs(1))
                        .send();
                    match request.await {
                        Ok(resp) if resp.status().is_success() => {
                            // notification complete
                            if !log_failure {
                                tracing::info!("Completed notification for url {}", uri);
                            }
                            return;
                        }
                        Ok(resp) => {
                            if log_failure {
                                tracing::error!(
                                    "Notify response for url {} completed with error: {}. Quietly retrying...",
                                    uri,
                                    resp.status().to_string()
                                );
                                log_failure = false;
                            }
                        }
                        Err(error) => {
                            if log_failure {
                                tracing::error!(
                                    "Failed to send notify for url {}, {}. Quietly retrying...",
                                    uri,
                                    error.to_string()
                                );
                                log_failure = false;
                            }
                        }
                    }
                }
            }

            backoff(&mut tries, Duration::from_secs(5)).await;
        }
    }

    /// Reissue a watch for the given resource id.
    /// The actual value is returned as its useful to crudely verify if any
    /// update was missed.
    async fn rewatch(
        id: &WatchResourceId,
        store: &mut impl Store,
    ) -> Option<(serde_json::Value, StoreWatchReceiver)> {
        match store.watch_obj(id).await {
            Ok(channel) => {
                // get the current value
                match store.get_kv(&id.key()).await {
                    Ok(obj) => Some((obj, channel)),
                    // deleted, so bail out
                    Err(StoreError::MissingEntry { .. }) => None,
                    Err(_) => Some((serde_json::Value::default(), channel)),
                }
            }
            Err(_) => {
                // lost connection? we'll just retry again...
                None
            }
        }
    }

    /// The current store implementation (etcd) does not persist the watch if
    /// the connection is lost which means we need to reissue the watch.
    /// todo: this should probably be addressed in the store itself
    async fn reconnect_watch(
        cancel: &mut tokio::sync::broadcast::Receiver<()>,
        id: &WatchResourceId,
        store: &Arc<Mutex<impl Store + 'static>>,
    ) -> Option<(serde_json::Value, StoreWatchReceiver)> {
        // we're still here so let's try to reconnect
        let mut tries = 0;
        loop {
            match cancel.try_recv() {
                Err(TryRecvError::Empty) => {}
                // dropped or received cancel signal
                _ => return None,
            };

            let mut store = store.lock().await;
            if store.online().await {
                return Self::rewatch(id, store.deref_mut()).await;
            }

            backoff(&mut tries, Duration::from_secs(5)).await;
        }
    }
}

/// Simple backoff delay which gets gradually larger up to a `max` duration.
async fn backoff(tries: &mut u32, max: Duration) {
    let cutoff = 4;
    *tries += 1;
    let backoff = if *tries <= cutoff {
        Duration::from_millis(100)
    } else {
        min((*tries - cutoff - 1) * Duration::from_millis(250), max)
    };
    tokio::time::sleep(backoff).await;
}

impl StoreWatch {
    /// Get all the watches for `watch_id`
    pub async fn get_watches(&self, watch_id: &WatchCfgId) -> Result<Watches, SvcError> {
        let watches = match self.get_watch_cfg(watch_id).await {
            Some(db) => {
                let db = db.lock().await;
                db.watches
                    .iter()
                    .map(|e| Watch {
                        id: watch_id.id.clone(),
                        callback: e.callback.clone(),
                        watch_type: e.type_.clone(),
                    })
                    .collect()
            }
            None => vec![],
        };

        Ok(Watches(watches))
    }

    /// Get the watch configuration for `watch_id`
    async fn get_watch_cfg(&self, watch_id: &WatchCfgId) -> Option<Arc<Mutex<WatchCfg>>> {
        for db in &self.watches {
            let found = {
                let db = db.lock().await;
                &db.watch_id == watch_id
            };
            if found {
                return Some(db.clone());
            }
        }
        None
    }

    /// Gets or creates the watch config for `watch_id` if it does not exist
    async fn get_or_create_watch_cfg(&mut self, watch_id: &WatchCfgId) -> Arc<Mutex<WatchCfg>> {
        match self.get_watch_cfg(watch_id).await {
            Some(watch) => watch,
            None => {
                let db = Arc::new(Mutex::new(WatchCfg::new(watch_id)));
                self.watches.push(db.clone());
                db
            }
        }
    }

    /// Create a new watch with given parameters
    pub async fn create_watch(
        &mut self,
        watch_id: &WatchCfgId,
        callback: &WatchCallback,
        type_: &WatchType,
    ) -> Result<(), SvcError> {
        let watch_cfg = self.get_or_create_watch_cfg(watch_id).await;
        let watch = WatchParams {
            callback: callback.clone(),
            type_: type_.clone(),
        };

        let mut watch_cfg = watch_cfg.lock().await;
        watch_cfg.add(&watch, self.registry.store().clone()).await?;
        Ok(())
    }

    /// Delete existing watch with the given parameters
    pub async fn delete_watch(
        &mut self,
        watch_id: &WatchCfgId,
        callback: &WatchCallback,
        type_: &WatchType,
    ) -> Result<(), SvcError> {
        let watch_cfg = self.get_or_create_watch_cfg(watch_id).await;
        let mut watch_cfg = watch_cfg.lock().await;
        let watch = WatchParams {
            callback: callback.clone(),
            type_: type_.clone(),
        };
        watch_cfg.del(&watch)?;
        Ok(())
    }
}
