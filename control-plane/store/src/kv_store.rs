use async_trait::async_trait;
use etcd_client::Error;
use serde_json::{Error as SerdeError, Value};
use snafu::Snafu;
use tokio::sync::mpsc::Receiver;

/// Definition of errors that can be returned from the key-value store.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum StoreError {
    /// Failed to connect to the key-value store.
    #[snafu(display("Failed to connect to store. Error {}", source))]
    Connect { source: Error },
    /// Failed to 'put' an entry in the store.
    #[snafu(display(
        "Failed to 'put' entry with key {} and value {}. Error {}",
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
    #[snafu(display(
        "Failed to 'get' entry with key {}. Error {}",
        key,
        source
    ))]
    Get { key: String, source: Error },
    /// Failed to find an entry with the given key.
    #[snafu(display("Entry with key {} not found.", key))]
    MissingEntry { key: String },
    /// Failed to 'delete' an entry from the store.
    #[snafu(display(
        "Failed to 'delete' entry with key {}. Error {}",
        key,
        source
    ))]
    Delete { key: String, source: Error },
    /// Failed to 'watch' an entry in the store.
    #[snafu(display(
        "Failed to 'watch' entry with key {}. Error {}",
        key,
        source
    ))]
    Watch { key: String, source: Error },
    /// Empty key.
    #[snafu(display("Failed to get key as string. Error {}", source))]
    KeyString { source: Error },
    /// Failed to deserialise key.
    #[snafu(display("Failed to deserialise key {}. Error {}", key, source))]
    DeserialiseKey { key: String, source: SerdeError },
    /// Empty value.
    #[snafu(display("Failed to get value as string. Error {}", source))]
    ValueString { source: Error },
    /// Failed to deserialise value.
    #[snafu(display(
        "Failed to deserialise value {}. Error {}",
        value,
        source
    ))]
    DeserialiseValue { value: String, source: SerdeError },
}

/// Representation of a watch event.
pub enum WatchEvent {
    // Put operation containing the key and value
    Put(Value, Value),
    // Delete operation
    Delete,
}

/// Trait defining the operations that can be performed on a key-value store.
#[async_trait]
pub trait Store {
    /// Put entry into the store.
    async fn put(
        &mut self,
        key: &Value,
        value: &Value,
    ) -> Result<(), StoreError>;
    /// Get an entry from the store.
    async fn get(&mut self, key: &Value) -> Result<Value, StoreError>;
    /// Delete an entry from the store.
    async fn delete(&mut self, key: &Value) -> Result<(), StoreError>;
    /// Watch for changes to the entry with the given key.
    /// Returns a channel which will be signalled when an event occurs.
    async fn watch(
        &mut self,
        key: &Value,
    ) -> Result<Receiver<Result<WatchEvent, StoreError>>, StoreError>;
}
