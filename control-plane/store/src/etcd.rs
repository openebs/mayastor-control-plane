use crate::kv_store::{
    Connect,
    Delete,
    DeserialiseKey,
    DeserialiseValue,
    Get,
    KeyString,
    Put,
    Store,
    StoreError,
    StoreError::MissingEntry,
    ValueString,
    Watch,
    WatchEvent,
};
use async_trait::async_trait;
use etcd_client::{Client, EventType, KeyValue, WatchStream, Watcher};
use serde_json::Value;
use snafu::ResultExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// etcd client
pub struct Etcd(Client);

impl Etcd {
    /// Create a new instance of the etcd client
    pub async fn new(endpoint: &str) -> Result<Self, StoreError> {
        Ok(Self(
            Client::connect([endpoint], None)
                .await
                .context(Connect {})?,
        ))
    }
}

#[async_trait]
impl Store for Etcd {
    /// 'Put' a key-value pair into etcd.
    async fn put(
        &mut self,
        key: &Value,
        value: &Value,
    ) -> Result<(), StoreError> {
        self.0
            .put(key.to_string(), value.to_string(), None)
            .await
            .context(Put {
                key: key.to_string(),
                value: value.to_string(),
            })?;
        Ok(())
    }

    /// 'Get' the value for the given key from etcd.
    async fn get(&mut self, key: &Value) -> Result<Value, StoreError> {
        let resp = self.0.get(key.to_string(), None).await.context(Get {
            key: key.to_string(),
        })?;
        match resp.kvs().first() {
            Some(kv) => {
                let v = kv.value_str().context(ValueString {})?;
                Ok(serde_json::from_str(v).context(DeserialiseValue {
                    value: v.to_string(),
                })?)
            }
            None => Err(MissingEntry {
                key: key.to_string(),
            }),
        }
    }

    /// 'Delete' the entry with the given key from etcd.
    async fn delete(&mut self, key: &Value) -> Result<(), StoreError> {
        self.0.delete(key.to_string(), None).await.context(Delete {
            key: key.to_string(),
        })?;
        Ok(())
    }

    /// 'Watch' the etcd entry with the given key.
    /// A receiver channel is returned which is signalled when the entry with
    /// the given key is changed.
    async fn watch(
        &mut self,
        key: &Value,
    ) -> Result<Receiver<Result<WatchEvent, StoreError>>, StoreError> {
        let (sender, receiver) = channel(100);
        let (watcher, stream) =
            self.0.watch(key.to_string(), None).await.context(Watch {
                key: key.to_string(),
            })?;
        watch(watcher, stream, sender);
        Ok(receiver)
    }
}

/// Watch for events in the key-value store.
/// When an event occurs, a WatchEvent is sent over the channel.
/// When a 'delete' event is received, the watcher stops watching.
fn watch(
    _watcher: Watcher,
    mut stream: WatchStream,
    mut sender: Sender<Result<WatchEvent, StoreError>>,
) {
    // For now we spawn a thread for each value that is watched.
    // If we find that we are watching lots of events, this can be optimised.
    // TODO: Optimise the spawning of threads if required.
    tokio::spawn(async move {
        loop {
            let response = match stream.message().await {
                Ok(msg) => {
                    match msg {
                        Some(resp) => resp,
                        // If there is no message, continue to wait for the
                        // next one.
                        None => continue,
                    }
                }
                Err(e) => {
                    // If we failed to get a message, continue to wait for the
                    // next one.
                    tracing::error!("Failed to get message with error {}", e);
                    continue;
                }
            };

            for event in response.events() {
                match event.event_type() {
                    EventType::Put => {
                        if let Some(kv) = event.kv() {
                            let result = match deserialise_kv(&kv) {
                                Ok((key, value)) => {
                                    Ok(WatchEvent::Put(key, value))
                                }
                                Err(e) => Err(e),
                            };
                            if sender.send(result).await.is_err() {
                                // Send only fails if the receiver is closed, so
                                // just stop watching.
                                return;
                            }
                        }
                    }
                    EventType::Delete => {
                        // Send only fails if the receiver is closed. We are
                        // returning here anyway, so the error doesn't need to
                        // be handled.
                        let _ = sender.send(Ok(WatchEvent::Delete)).await;
                        return;
                    }
                }
            }
        }
    });
}

/// Deserialise a key-value pair into serde_json::Value representations.
fn deserialise_kv(kv: &KeyValue) -> Result<(Value, Value), StoreError> {
    let key_str = kv.key_str().context(KeyString {})?;
    let key = serde_json::from_str(key_str).context(DeserialiseKey {
        key: key_str.to_string(),
    })?;
    let value_str = kv.value_str().context(ValueString {})?;
    let value = serde_json::from_str(value_str).context(DeserialiseValue {
        value: value_str.to_string(),
    })?;
    Ok((key, value))
}
