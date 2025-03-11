mod snapshot;

use super::{ResourceMutex, ResourceUid};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};
use stor_port::{
    pstor,
    types::v0::{
        store::{
            nexus_persistence::{NexusInfo, NexusInfoKey},
            volume::{AffinityGroupSpec, VolumeSpec},
        },
        transport::VolumeId,
    },
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

use pstor::{StoreKv, StoreKvWatcher};
use stor_port::types::v0::transport::NexusId;

pub(crate) struct VolumeHealthWatcher {
    watcher: Box<dyn StoreKvWatcher>,
    key_prefix: String,
    health: Arc<Mutex<BTreeMap<uuid::Uuid, Arc<NexusInfo>>>>,
}
impl std::fmt::Debug for VolumeHealthWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VolumeHealthWatcher").finish()
    }
}

impl VolumeHealthWatcher {
    /// Create a new `Self`.
    pub(crate) fn new(store: &impl StoreKv) -> Self {
        let health = Arc::new(Mutex::new(BTreeMap::new()));

        let health_cln = health.clone();
        let watcher = store.kv_watcher(move |arg| {
            let cnt = pstor::WatchResult::Continue;

            if arg.value.is_empty() {
                match NexusInfoKey::parse_id(arg.updated_key) {
                    Ok(id) => {
                        tracing::debug!(key=%arg.updated_key, %id, "Removing key");
                        health_cln.lock().remove(id.uuid());
                    }
                    Err(error) => {
                        tracing::warn!(key=%arg.updated_key, error, "Received unexpected PStor Update");
                    }
                }
                return cnt;
            }

            let Ok(nexus_info) = serde_json::from_str::<NexusInfo>(arg.value) else {
                tracing::error!(
                    key = arg.updated_key,
                    value = arg.value,
                    "Failed to parse health value information"
                );
                return cnt;
            };

            match nexus_info.with_key(arg.updated_key) {
                Ok(Some(info)) => {
                    tracing::debug!(?info, "Updating Health info");
                    health_cln.lock().insert(*info.uuid, Arc::new(info));
                }
                Ok(None) => {
                    tracing::warn!(key=%arg.updated_key, "Received unexpected PStor Update");
                }
                Err(error) => tracing::warn!(key=%arg.updated_key, %error, "Failed to parse uuids"),
            }

            cnt
        });

        Self {
            watcher: Box::new(watcher),
            key_prefix: NexusInfoKey::key_prefix(),
            health,
        }
    }
    /// Get the health key prefix.
    pub(crate) fn key_prefix(&self) -> &str {
        &self.key_prefix
    }
    /// Start the watcher.
    /// All registered pstor key updates will be propagated via the callback.
    pub(crate) async fn init(&self) -> Result<(), agents::errors::SvcError> {
        self.watcher
            .watch(pstor::WatchKey::new(NexusInfoKey::key_prefix()), ())?;
        Ok(())
    }
    /// If the health info hasn't been added yet, insert it.
    pub(crate) fn if_empty_insert(&self, info: NexusInfo) {
        let mut health = self.health.lock();
        if health.get(&info.uuid).is_none() {
            health.insert(*info.uuid, Arc::new(info));
        }
    }
    /// Get the volume health info for the given target.
    pub(crate) fn health(&self, target: &NexusId) -> Option<Arc<NexusInfo>> {
        self.health.lock().get(target.uuid()).cloned()
    }
    /// Expose a retain interface, allowing clean up of objects.
    pub(crate) fn retain<F: FnMut(&uuid::Uuid, &mut Arc<NexusInfo>) -> bool>(&self, retain: F) {
        self.health.lock().retain(retain);
    }
}
