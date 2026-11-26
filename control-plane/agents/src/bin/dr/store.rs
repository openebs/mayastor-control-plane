use stor_port::{
    pstor::{
        etcd::Etcd, key_prefix_obj, ObjectKey, StorableObject, StorableObjectType, StoreKv,
        StoreObj, API_VERSION,
    },
    types::v0::store::definitions::StoreError,
};

use futures::lock::Mutex;
use http::Uri;
use std::{sync::Arc, time::Duration};
use tracing::error;

/// The dr agent's own handle on the persistent store.
///
/// Core is not in this path: the pairs and the volume entries are the dr agent's
/// resources, and the dr agent reads them on every cycle, so putting them behind another
/// agent's api would buy nothing and cost a hop. This mirrors what the ha agent does with
/// its switchover requests.
#[derive(Debug, Clone)]
pub(crate) struct Store {
    store: Arc<Mutex<Etcd>>,
    timeout: Duration,
}

impl Store {
    /// Connect to the persistent store.
    pub(crate) async fn new(endpoint: &Uri, timeout: Duration) -> Result<Self, StoreError> {
        let endpoint = endpoint.to_string();
        let connect = Etcd::new(&endpoint);
        let store = timed("connect", timeout, connect).await??;
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            timeout,
        })
    }

    /// Write an object.
    pub(crate) async fn put<T: StorableObject>(&self, object: &T) -> Result<(), StoreError> {
        let mut store = self.store.lock().await;
        timed("put", self.timeout, store.put_obj(object)).await?
    }

    /// Remove an object.
    pub(crate) async fn delete<T: StorableObject>(&self, spec: &T) -> Result<(), StoreError> {
        let mut store = self.store.lock().await;
        let key = spec.key().key();
        let delete = store.delete_kv(&key);
        timed("delete", self.timeout, delete).await?
    }

    /// Every object of a type this cluster has stored.
    ///
    /// Read once at startup: without it a restarted agent does not know the relationships
    /// it is already party to.
    pub(crate) async fn list<T: StorableObject>(
        &self,
        kind: StorableObjectType,
    ) -> Result<Vec<T>, StoreError> {
        let mut store = self.store.lock().await;
        let what = kind.to_string();
        let prefix = key_prefix_obj(kind, API_VERSION);
        let entries = timed("get", self.timeout, store.get_values_prefix(&prefix)).await??;
        let specs = entries.into_iter().filter_map(|(key, value)| {
            match serde_json::from_value(value) {
                Ok(spec) => Some(spec),
                Err(error) => {
                    // One unreadable entry should not stop the agent starting, but it must
                    // be visible: the relationship it describes will not be served.
                    error!(%key, %error, "Ignoring an unreadable {what}");
                    None
                }
            }
        });
        Ok(specs.collect())
    }
}

/// Run a store operation, reporting a timeout as the store's own error rather than as
/// whatever the caller would have made of elapsing.
async fn timed<F: std::future::Future>(
    operation: &str,
    timeout: Duration,
    op: F,
) -> Result<F::Output, StoreError> {
    tokio::time::timeout(timeout, op)
        .await
        .map_err(|_| StoreError::Timeout {
            operation: operation.to_string(),
            timeout,
        })
}
