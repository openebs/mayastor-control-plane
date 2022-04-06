use async_trait::async_trait;
use etcd_client::Error;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Error as SerdeError, Value};
use snafu::Snafu;
use strum_macros::Display;
use tokio::sync::mpsc::Receiver;

/// Definition of errors that can be returned from the key-value store.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum StoreError {
    /// Failed to connect to the key-value store.
    #[snafu(display("Failed to connect to store. Error {}", source))]
    Connect { source: Error },
    /// Failed to 'put' an entry in the store.
    #[snafu(display(
        "Failed to 'put' entry with key {} and value {:?}. Error {}",
        key,
        value,
        source
    ))]
    Put {
        key: String,
        value: String,
        source: Error,
    },
    /// Failed to 'get' an entry from the store.
    #[snafu(display("Failed to 'get' entry with key {}. Error {}", key, source))]
    Get { key: String, source: Error },
    /// Failed to 'get' an entry, with the given prefix, from the store.
    #[snafu(display("Failed to 'get' entry with prefix {}. Error {}", prefix, source))]
    GetPrefix { prefix: String, source: Error },
    /// Failed to find an entry with the given key.
    #[snafu(display("Entry with key {} not found.", key))]
    MissingEntry { key: String },
    /// Failed to 'delete' an entry from the store.
    #[snafu(display("Failed to 'delete' entry with key {}. Error {}", key, source))]
    Delete { key: String, source: Error },
    /// Failed to 'watch' an entry in the store.
    #[snafu(display("Failed to 'watch' entry with key {}. Error {}", key, source))]
    Watch { key: String, source: Error },
    /// Empty key.
    #[snafu(display("Failed to get key as string. Error {}", source))]
    KeyString { source: Error },
    /// Empty value.
    #[snafu(display("Failed to get value as string. Error {}", source))]
    ValueString { source: Error },
    /// Failed to deserialise value.
    #[snafu(display("Failed to deserialise value {}. Error {}", value, source))]
    DeserialiseValue { value: String, source: SerdeError },
    /// Failed to serialise value.
    #[snafu(display("Failed to serialise value. Error {}", source))]
    SerialiseValue { source: SerdeError },
    /// Failed to run operation within a timeout.
    #[snafu(display("Timed out during {} operation after {:?}", operation, timeout))]
    Timeout {
        operation: String,
        timeout: std::time::Duration,
    },
    #[snafu(display("Failed to grab the lease lock, reason: '{}'", reason))]
    FailedLock { reason: String },
    #[snafu(display("Etcd is not ready, reason: '{}'", reason))]
    NotReady { reason: String },
}

/// Representation of a watch event.
#[derive(Debug)]
pub enum WatchEvent {
    // Put operation containing the key and value
    Put(String, Value),
    // Delete operation
    Delete,
}

/// Store keys type trait
pub trait StoreKey: Sync + ToString {}
impl<T> StoreKey for T where T: Sync + ToString {}
/// Store value type trait
pub trait StoreValue: Sync + serde::Serialize {}
impl<T> StoreValue for T where T: Sync + serde::Serialize {}

/// Trait defining the operations that can be performed on a key-value store.
#[async_trait]
pub trait Store: Sync + Send + Clone {
    /// Put entry into the store.
    async fn put_kv<K: StoreKey, V: StoreValue>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), StoreError>;
    /// Get an entry from the store.
    async fn get_kv<K: StoreKey>(&mut self, key: &K) -> Result<Value, StoreError>;
    /// Delete an entry from the store.
    async fn delete_kv<K: StoreKey>(&mut self, key: &K) -> Result<(), StoreError>;
    /// Watch for changes to the entry with the given key.
    /// Returns a channel which will be signalled when an event occurs.
    async fn watch_kv<K: StoreKey>(
        &mut self,
        key: &K,
    ) -> Result<Receiver<Result<WatchEvent, StoreError>>, StoreError>;

    async fn put_obj<O: StorableObject>(&mut self, object: &O) -> Result<(), StoreError>;

    async fn get_obj<O: StorableObject>(&mut self, _key: &O::Key) -> Result<O, StoreError>;

    /// Returns a vector of tuples. Each tuple represents a key-value pair.
    async fn get_values_prefix(
        &mut self,
        key_prefix: &str,
    ) -> Result<Vec<(String, Value)>, StoreError>;

    async fn watch_obj<K: ObjectKey>(&mut self, key: &K) -> Result<StoreWatchReceiver, StoreError>;

    async fn online(&mut self) -> bool;
}

pub type StoreWatchReceiver = Receiver<Result<WatchEvent, StoreError>>;

/// Implemented by Keys of Storable Objects, eg: VolumeId
pub trait ObjectKey: Sync + Send {
    fn key(&self) -> String {
        generate_key(self)
    }
    fn key_type(&self) -> StorableObjectType;
    fn key_uuid(&self) -> String;
}

/// Implemented by objects which get stored in the store, eg: Volume
#[async_trait]
pub trait StorableObject: Serialize + Sync + Send + DeserializeOwned {
    type Key: ObjectKey;

    fn key(&self) -> Self::Key;
}

/// All types of objects which are storable in our store
#[derive(Display, Copy, Clone, Debug)]
pub enum StorableObjectType {
    WatchConfig,
    Volume,
    Nexus,
    NexusSpec,
    NexusState,
    NexusInfo,
    Node,
    NodeSpec,
    Pool,
    PoolSpec,
    Replica,
    ReplicaState,
    ReplicaSpec,
    VolumeSpec,
    VolumeState,
    ChildSpec,
    ChildState,
    CoreRegistryConfig,
    StoreLeaseLock,
    StoreLeaseOwner,
}

/// Returns the key prefix that should is used for the keys, when running from within the cluster.
pub fn key_prefix() -> String {
    let namespace = std::env::var("MY_POD_NAMESPACE").unwrap_or_else(|_| "default".into());
    build_key_prefix(crate::platform::platform_info(), namespace)
}
/// Returns the key prefix that is used for the keys.
/// The platform info and namespace where the product is running must be specified.
pub fn build_key_prefix(
    cluster_uid: &dyn crate::platform::PlatformInfo,
    namespace: String,
) -> String {
    let api_version = 0;
    format!(
        "{}/apis/v{}/clusters/{}/namespaces/{}",
        crate::ETCD_KEY_PREFIX,
        api_version,
        cluster_uid.uid(),
        namespace
    )
}
/// Returns the control plane prefix that should be used for the keys, in conjunction
/// with a `StorableObjectType` type.
pub fn key_prefix_obj(obj_type: StorableObjectType) -> String {
    format!("{}/{}", key_prefix(), obj_type)
}

/// Create a key based on the object's key trait.
pub fn generate_key<K: ObjectKey + ?Sized>(k: &K) -> String {
    format!("{}/{}", key_prefix_obj(k.key_type()), k.key_uuid())
}
