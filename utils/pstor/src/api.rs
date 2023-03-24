use crate::{common::ApiVersion, products::v2::generate_key, Error};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::Receiver;

/// Trait defining the operations that can be performed on a key-value store.
#[async_trait]
pub trait Store: StoreKv + StoreObj + Sync + Send + Clone {
    async fn online(&mut self) -> bool;
}

/// Trait defining the operations that can be performed on a key-value store.
/// This is strictly intended for a KV type access.
#[async_trait]
pub trait StoreKv: Sync + Send + Clone {
    /// Puts the given `V` value into the under the given `K` key.
    async fn put_kv<K: StoreKey, V: StoreValue>(&mut self, key: &K, value: &V)
        -> Result<(), Error>;
    /// Get the value from the given `K` key entry from the store.
    async fn get_kv<K: StoreKey>(&mut self, key: &K) -> Result<Value, Error>;
    /// Deletes the given `K` key entry from the store.
    async fn delete_kv<K: StoreKey>(&mut self, key: &K) -> Result<(), Error>;
    /// Watches for changes under the given `K` key entry.
    /// Returns a channel which is signalled when an event occurs.
    /// # Warning: Events may be lost if we are restarted.
    async fn watch_kv<K: StoreKey>(&mut self, key: &K) -> Result<StoreWatchReceiver, Error>;

    /// Returns a vector of tuples. Each tuple represents a key-value pair.
    async fn get_values_prefix(&mut self, key_prefix: &str) -> Result<Vec<(String, Value)>, Error>;
    /// Returns a vector of tuples. Each tuple represents a key-value pair.
    async fn get_values_paged(
        &mut self,
        key_prefix: &str,
        limit: i64,
    ) -> Result<Vec<(String, Value)>, Error>;
    /// Deletes all key values from a given prefix.
    async fn delete_values_prefix(&mut self, key_prefix: &str) -> Result<(), Error>;
}

/// Trait defining the operations that can be performed on a key-value store using object semantics.
/// It allows for abstracting the key component into the `StorableObject` itself.
#[async_trait]
pub trait StoreObj: StoreKv + Sync + Send + Clone {
    /// Puts the given `O` object into the store.
    async fn put_obj<O: StorableObject>(&mut self, object: &O) -> Result<(), Error>;
    /// Gets the object `O` through its `O::Key`.
    async fn get_obj<O: StorableObject>(&mut self, _key: &O::Key) -> Result<O, Error>;
    /// Watches for changes under the given `K` object key entry.
    /// Returns a channel which is signalled when an event occurs.
    /// # Warning: Events may be lost if we are restarted.
    async fn watch_obj<K: ObjectKey>(&mut self, key: &K) -> Result<StoreWatchReceiver, Error>;
}

/// Store keys type trait.
pub trait StoreKey: Sync + ToString {}
impl<T> StoreKey for T where T: Sync + ToString {}
/// Store value type trait.
pub trait StoreValue: Sync + serde::Serialize {}
impl<T> StoreValue for T where T: Sync + serde::Serialize {}
/// Representation of a watch event.
#[derive(Debug)]
pub enum WatchEvent {
    /// Put operation containing the key and value.
    Put(String, Value),
    /// Delete operation.
    Delete,
}
/// Channel used to receive events from a watch setup through `StoreKv::watch_kv`.
pub type StoreWatchReceiver = Receiver<Result<WatchEvent, Error>>;

/// Implemented by Keys of Storable Objects.
pub trait ObjectKey: Sync + Send {
    type Kind: AsRef<str>;

    fn key(&self) -> String {
        generate_key(self)
    }
    fn version(&self) -> ApiVersion;
    fn key_type(&self) -> Self::Kind;
    fn key_uuid(&self) -> String;
}
/// Implemented by objects which get stored in the store.
#[async_trait]
pub trait StorableObject: Serialize + Sync + Send + DeserializeOwned {
    type Key: ObjectKey;

    fn key(&self) -> Self::Key;
}
