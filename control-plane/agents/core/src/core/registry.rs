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
use mbus_api::v0::NodeId;
use std::{collections::HashMap, sync::Arc};
use store::{etcd::Etcd, store::Store};
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
    period: std::time::Duration,
    pub(crate) store: Arc<Mutex<S>>,
}

impl Registry {
    /// Create a new registry with the `period` to reload the cache
    pub async fn new(period: std::time::Duration, store_url: String) -> Self {
        let store = Etcd::new(&store_url)
            .await
            .expect("Should connect to the persistent store");
        let registry = Self {
            nodes: Default::default(),
            specs: Default::default(),
            period,
            store: Arc::new(Mutex::new(store)),
        };
        registry.start();
        registry
    }

    /// Start thread which updates the registry
    fn start(&self) {
        let registry = self.clone();
        tokio::spawn(async move {
            registry.poller().await;
        });
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
            tokio::time::delay_for(self.period).await;
        }
    }
    async fn trace_all(&self) {
        let registry = self.nodes.read().await;
        tracing::debug!("Registry update: {:?}", registry);
    }
}
