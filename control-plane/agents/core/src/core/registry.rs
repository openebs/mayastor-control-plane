//! Registry containing all mayastor instances which register themselves via the
//! `Register` Message.
//! Said instances may also send `Deregister` to unregister themselves
//! during node/pod shutdown/restart. When this happens the node state is
//! set as `Unknown`. It's TBD how to detect when a node is really going
//! away for good.
//!
//! A mayastor instance sends `Register` every N seconds as sort of a keep
//! alive message.
//! A watchful watchdog is started for each node and it will change the
//! state of said node to `Offline` if it is not petted before its
//! `deadline`.
//!
//! Each instance also contains the known nexus, pools and replicas that live in
//! said instance.
use super::{specs::*, wrapper::NodeWrapper};
use crate::core::wrapper::InternalOps;
use common::errors::SvcError;
use mbus_api::v0::NodeId;
use std::{collections::HashMap, sync::Arc};
use store::{
    etcd::Etcd,
    store::{StorableObject, Store, StoreError},
};
use tokio::sync::{Mutex, RwLock};

/// Registry containing all mayastor instances (aka nodes)
pub type Registry = RegistryInner<Etcd>;

/// Generic Registry Inner with a Store trait
#[derive(Clone, Debug)]
pub struct RegistryInner<S: Store> {
    /// the actual state of the node
    pub(crate) nodes: Arc<RwLock<HashMap<NodeId, Arc<Mutex<NodeWrapper>>>>>,
    /// spec (aka desired state) for the various resources
    pub(crate) specs: ResourceSpecsLocked,
    /// period to refresh the cache
    cache_period: std::time::Duration,
    pub(crate) store: Arc<Mutex<S>>,
    /// store gRPC operation timeout
    store_timeout: std::time::Duration,
    /// reconciliation period when no work is being done
    pub(crate) reconcile_idle_period: std::time::Duration,
    /// reconciliation period when work is pending
    pub(crate) reconcile_period: std::time::Duration,
}

impl Registry {
    /// Create a new registry with the `cache_period` to reload the cache, the
    /// `store_url` to connect to, a `store_timeout` for store operations
    /// and a `reconcile_period` for reconcile operations
    pub async fn new(
        cache_period: std::time::Duration,
        store_url: String,
        store_timeout: std::time::Duration,
        reconcile_period: std::time::Duration,
        reconcile_idle_period: std::time::Duration,
    ) -> Self {
        let store = Etcd::new(&store_url)
            .await
            .expect("Should connect to the persistent store");
        let registry = Self {
            nodes: Default::default(),
            specs: ResourceSpecsLocked::new(),
            cache_period,
            store: Arc::new(Mutex::new(store)),
            store_timeout,
            reconcile_period,
            reconcile_idle_period,
        };
        registry.start();
        registry
    }

    /// Serialized write to the persistent store
    pub async fn store_obj<O: StorableObject>(&self, object: &O) -> Result<(), SvcError> {
        let mut store = self.store.lock().await;
        match tokio::time::timeout(
            self.store_timeout,
            async move { store.put_obj(object).await },
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(_) => Err(StoreError::Timeout {
                operation: "Put".to_string(),
                timeout: self.store_timeout,
            }
            .into()),
        }
    }

    /// Check if the persistent store is currently online
    pub async fn store_online(&self) -> bool {
        let mut store = self.store.lock().await;
        store.online().await
    }

    /// Start the worker thread which updates the registry
    fn start(&self) {
        let registry = self.clone();
        tokio::spawn(async move {
            registry.poller().await;
        });
        self.specs.start(self.clone());
    }

    /// Poll each node for resource updates
    async fn poller(&self) {
        loop {
            let nodes = self.nodes.read().await.clone();
            for (_, node) in nodes.iter() {
                let lock = node.grpc_lock().await;
                let _guard = lock.lock().await;

                let mut node_clone = node.lock().await.clone();
                if node_clone.reload().await.is_ok() {
                    // update node in the registry
                    *node.lock().await = node_clone;
                }
            }
            self.trace_all().await;
            tokio::time::delay_for(self.cache_period).await;
        }
    }
    async fn trace_all(&self) {
        let registry = self.nodes.read().await;
        tracing::debug!("Registry update: {:?}", registry);
    }
}
