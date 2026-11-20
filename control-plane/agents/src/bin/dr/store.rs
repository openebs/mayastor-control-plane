use stor_port::{
    pstor::{
        etcd::Etcd, key_prefix_obj, ObjectKey, StorableObject, StorableObjectType, StoreKv,
        StoreObj, API_VERSION,
    },
    types::v0::store::{definitions::StoreError, dr::DrClusterPairSpec},
};

use futures::lock::Mutex;
use http::Uri;
use std::{sync::Arc, time::Duration};
use tracing::error;

/// The dr agent's own handle on the persistent store.
///
/// Core is not in this path: the pair is the dr agent's resource, and the dr agent reads
/// it on every cycle, so putting it behind another agent's api would buy nothing and cost
/// a hop. This mirrors what the ha agent does with its switchover requests.
#[derive(Debug, Clone)]
pub(crate) struct PairStore {
    store: Arc<Mutex<Etcd>>,
    timeout: Duration,
}

impl PairStore {
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

    /// Write a pair.
    pub(crate) async fn put(&self, spec: &DrClusterPairSpec) -> Result<(), StoreError> {
        let mut store = self.store.lock().await;
        timed("put", self.timeout, store.put_obj(spec)).await?
    }

    /// Remove a pair.
    pub(crate) async fn delete(&self, spec: &DrClusterPairSpec) -> Result<(), StoreError> {
        let mut store = self.store.lock().await;
        let key = spec.key().key();
        let delete = store.delete_kv(&key);
        timed("delete", self.timeout, delete).await?
    }

    /// Every pair this cluster has stored.
    ///
    /// Read once at startup: without it a restarted agent does not know the relationships
    /// it is already party to.
    pub(crate) async fn list(&self) -> Result<Vec<DrClusterPairSpec>, StoreError> {
        let mut store = self.store.lock().await;
        let prefix = key_prefix_obj(StorableObjectType::DrClusterPairSpec, API_VERSION);
        let entries = timed("get", self.timeout, store.get_values_prefix(&prefix)).await??;
        let specs = entries.into_iter().filter_map(|(key, value)| {
            match serde_json::from_value(value) {
                Ok(spec) => Some(spec),
                Err(error) => {
                    // One unreadable entry should not stop the agent starting, but it must
                    // be visible: the relationship it describes will not be served.
                    error!(%key, %error, "Ignoring an unreadable dr cluster pair");
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
