use crate::switchover::SwitchOverRequest;
use common_lib::{
    store::etcd::Etcd,
    types::v0::store::{
        definitions::{
            key_prefix_obj, ObjectKey, StorableObject, StorableObjectType, Store, StoreError,
        },
        switchover::SwitchOverSpec,
    },
};
use futures::lock::Mutex;
use http::Uri;
use std::{convert::TryFrom, sync::Arc, time::Duration};
use tracing::{debug, error};

/// Represent object to access Etcd.
#[derive(Debug, Clone)]
pub struct EtcdStore {
    store: Arc<Mutex<Etcd>>,
    timeout: Duration,
}

impl EtcdStore {
    /// Create a new Etcd client.
    pub async fn new(endpoint: Uri, timeout: Duration) -> Result<Self, StoreError> {
        match tokio::time::timeout(timeout, async { Etcd::new(&endpoint.to_string()).await }).await
        {
            Ok(v) => {
                let store = v?;
                Ok(Self {
                    store: Arc::new(Mutex::new(store)),
                    timeout,
                })
            }
            Err(error) => {
                error!(%error, "Failed to create persistent store client");
                Err(StoreError::Timeout {
                    operation: "connect".to_string(),
                    timeout,
                })
            }
        }
    }

    /// Serialized write to the persistent store.
    pub async fn store_obj<O: StorableObject>(&self, object: &O) -> Result<(), anyhow::Error> {
        let mut store = self.store.lock().await;
        match tokio::time::timeout(self.timeout, async move { store.put_obj(object).await }).await {
            Ok(result) => result.map_err(Into::into),
            Err(error) => {
                error!(%error, "Failed to write to persistent store");
                Err(StoreError::Timeout {
                    operation: "Put".to_string(),
                    timeout: self.timeout,
                }
                .into())
            }
        }
    }

    /// Delete the object from the persistent store.
    pub async fn delete_obj<O: StorableObject>(&self, object: &O) -> Result<(), anyhow::Error> {
        let mut store = self.store.lock().await;
        match tokio::time::timeout(self.timeout, async move {
            store.delete_kv(&object.key().key()).await
        })
        .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                error!(%error, "Failed to delete from persistent store");
                Err(StoreError::Timeout {
                    operation: "Delete".to_string(),
                    timeout: self.timeout,
                }
                .into())
            }
        }
    }

    /// Get incomplete requests stored in Etcd.
    /// Request with error or path published is considered a complete request.
    pub async fn fetch_incomplete_requests(&self) -> Result<Vec<SwitchOverRequest>, anyhow::Error> {
        let mut store = self.store.lock().await;
        let key = key_prefix_obj(StorableObjectType::SwitchOver);
        let store_entries = store.get_values_prefix(&key).await?;

        debug!("fetched entries from etcd");

        let entries = store_entries
            .into_iter()
            .map(|v| v.1)
            .map(serde_json::from_value)
            .collect::<Result<Vec<SwitchOverSpec>, serde_json::Error>>()?;

        let mut partial_entries = vec![];
        for spec in entries {
            let req = SwitchOverRequest::try_from(&spec)?;
            partial_entries.push(req)
        }

        Ok(partial_entries)
    }
}
