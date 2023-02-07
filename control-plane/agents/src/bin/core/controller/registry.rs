//! Registry containing all io-engine instances which register themselves via the
//! `Register` Message.
//! Said instances may also send `Deregister` to unregister themselves
//! during node/pod shutdown/restart. When this happens the node state is
//! set as `Unknown`. It's TBD how to detect when a node is really going
//! away for good.
//!
//! An io-engine instance sends `Register` every N seconds as sort of a keep
//! alive message.
//! A watchful watchdog is started for each node and it will change the
//! state of said node to `Offline` if it is not petted before its
//! `deadline`.
//!
//! Each instance also contains the known nexus, pools and replicas that live in
//! said instance.
use super::{resources::operations_helper::*, wrapper::NodeWrapper};
use crate::controller::{
    reconciler::ReconcilerControl,
    task_poller::{PollEvent, PollTriggerEvent},
    wrapper::InternalOps,
};
use agents::errors::SvcError;
use common_lib::{
    store::etcd::Etcd,
    types::v0::{
        store::{
            definitions::{StorableObject, Store, StoreError, StoreKey},
            registry::{ControlPlaneService, CoreRegistryConfig, NodeRegistration},
            volume::InitiatorAC,
        },
        transport::{HostNqn, NodeId},
    },
    HostAccessControl,
};
use std::{
    collections::HashMap,
    future::Future,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

/// Registry containing all io-engine instances (aka nodes).
#[derive(Clone, Debug)]
pub(crate) struct Registry {
    inner: Arc<RegistryInner<Etcd>>,
}

/// Map that stores the actual state of the nodes.
pub(crate) type NodesMapLocked = Arc<RwLock<HashMap<NodeId, Arc<RwLock<NodeWrapper>>>>>;

impl Deref for Registry {
    type Target = Arc<RegistryInner<Etcd>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Number of rebuilds.
pub(crate) type NumRebuilds = u32;

/// Generic Registry Inner with a Store trait.
#[derive(Debug)]
pub(crate) struct RegistryInner<S: Store> {
    /// the actual state of the nodes.
    nodes: NodesMapLocked,
    /// spec (aka desired state) of the various resources.
    specs: ResourceSpecsLocked,
    /// period to refresh the cache.
    cache_period: std::time::Duration,
    store: Arc<Mutex<S>>,
    /// store gRPC operation timeout.
    store_timeout: std::time::Duration,
    /// reconciliation period when no work is being done.
    reconcile_idle_period: std::time::Duration,
    /// reconciliation period when work is pending.
    reconcile_period: std::time::Duration,
    reconciler: ReconcilerControl,
    config: CoreRegistryConfig,
    /// system-wide maximum number of concurrent rebuilds allowed.
    max_rebuilds: Option<NumRebuilds>,
    /// Enablement of host access control.
    host_acl: Vec<HostAccessControl>,
}

impl Registry {
    /// Create a new registry with the `cache_period` to reload the cache, the
    /// `store_url` to connect to, a `store_timeout` for store operations
    /// and a `reconcile_period` for reconcile operations.
    /// todo: move cmdline args into it's own config container.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        cache_period: std::time::Duration,
        store_url: String,
        store_timeout: std::time::Duration,
        store_lease_tll: std::time::Duration,
        reconcile_period: std::time::Duration,
        reconcile_idle_period: std::time::Duration,
        max_rebuilds: Option<NumRebuilds>,
        host_acl: Vec<HostAccessControl>,
    ) -> Self {
        let store_endpoint = Self::format_store_endpoint(&store_url);
        tracing::info!("Connecting to persistent store at {}", store_endpoint);
        let store = Etcd::new_leased(
            [&store_endpoint],
            ControlPlaneService::CoreAgent,
            store_lease_tll,
        )
        .await
        .expect("Should connect to the persistent store");
        tracing::info!("Connected to persistent store at {}", store_endpoint);
        let registry = Self {
            inner: Arc::new(RegistryInner {
                nodes: Default::default(),
                specs: ResourceSpecsLocked::new(),
                cache_period,
                store: Arc::new(Mutex::new(store.clone())),
                store_timeout,
                reconcile_period,
                reconcile_idle_period,
                reconciler: ReconcilerControl::new(),
                config: Self::get_config_or_panic(store).await,
                max_rebuilds,
                host_acl,
            }),
        };
        registry.init().await;
        registry
    }

    /// Formats the store endpoint with a default port if one isn't supplied.
    fn format_store_endpoint(endpoint: &str) -> String {
        match endpoint.contains(':') {
            true => endpoint.to_string(),
            false => format!("{}:{}", endpoint, "2379"),
        }
    }

    /// Get the `CoreRegistryConfig` from etcd, if it exists, or use the default.
    /// If the mayastor_v1 config exists, then reuse it.
    async fn get_config_or_panic<S: Store>(mut store: S) -> CoreRegistryConfig {
        let config = CoreRegistryConfig::new(NodeRegistration::Automatic);
        match store.get_obj(&config.key()).await {
            Ok(store_config) => store_config,
            Err(StoreError::MissingEntry { .. }) => {
                store.put_obj(&config).await.expect(
                    "Must be able to access the persistent store to persist configuration information",
                );
                config
            },
            Err(error) => panic!(
                "Must be able to access the persistent store to load configuration information. Got error: '{:#?}'", error
            ),
        }
    }
    /// Get the `CoreRegistryConfig`
    pub(crate) fn config(&self) -> &CoreRegistryConfig {
        &self.config
    }

    /// reconciliation period when no work is being done.
    pub(crate) fn reconcile_idle_period(&self) -> std::time::Duration {
        self.reconcile_idle_period
    }
    /// reconciliation period when work is pending.
    pub(crate) fn reconcile_period(&self) -> std::time::Duration {
        self.reconcile_period
    }

    /// Get a reference to the actual state of the nodes.
    pub(crate) fn nodes(&self) -> &NodesMapLocked {
        &self.nodes
    }
    /// Get a reference to the locked resource specs object.
    pub(crate) fn specs(&self) -> &ResourceSpecsLocked {
        &self.specs
    }

    /// Serialized write to the persistent store.
    pub(crate) async fn store_obj<O: StorableObject>(&self, object: &O) -> Result<(), SvcError> {
        let store = self.store.clone();
        match tokio::time::timeout(self.store_timeout, async move {
            // todo: is it still necessary to sync updates to the store?
            //  otherwise should make methods immutable
            let mut store = store.lock().await;
            Self::op_with_threshold(async move { store.put_obj(object).await }).await
        })
        .await
        {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err(StoreError::Timeout {
                operation: "Put".to_string(),
                timeout: self.store_timeout,
            }
            .into()),
        }
    }

    /// Serialized read from the persistent store.
    pub(crate) async fn load_obj<O: StorableObject>(&self, key: &O::Key) -> Result<O, SvcError> {
        let store = self.store.clone();
        match tokio::time::timeout(self.store_timeout, async move {
            let mut store = store.lock().await;
            Self::op_with_threshold(async move { store.get_obj(key).await }).await
        })
        .await
        {
            Ok(obj) => Ok(obj?),
            Err(_) => Err(StoreError::Timeout {
                operation: "Get".to_string(),
                timeout: self.store_timeout,
            }
            .into()),
        }
    }

    /// Serialized delete to the persistent store.
    pub(crate) async fn delete_kv<K: StoreKey>(&self, key: &K) -> Result<(), SvcError> {
        let store = self.store.clone();
        match tokio::time::timeout(self.store_timeout, async move {
            let mut store = store.lock().await;
            Self::op_with_threshold(async move { store.delete_kv(key).await }).await
        })
        .await
        {
            Ok(result) => match result {
                Ok(_) => Ok(()),
                // already deleted, no problem
                Err(StoreError::MissingEntry { .. }) => {
                    tracing::warn!("Entry with key {} missing from store.", key.to_string());
                    Ok(())
                }
                Err(error) => Err(SvcError::from(error)),
            },
            Err(_) => Err(SvcError::from(StoreError::Timeout {
                operation: "Delete".to_string(),
                timeout: self.store_timeout,
            })),
        }
    }

    async fn op_with_threshold<F, O>(future: F) -> O
    where
        F: Future<Output = O>,
    {
        let start = std::time::Instant::now();
        let result = future.await;
        let warn_threshold = std::time::Duration::from_secs(1);
        if start.elapsed() > warn_threshold {
            // todo: ratelimit this warning?
            tracing::warn!("Store operation took longer than {:?}", warn_threshold);
        }
        result
    }

    /// Get a reference to the persistent store
    pub(crate) fn store(&self) -> &Arc<Mutex<Etcd>> {
        &self.store
    }

    /// Check if the persistent store is currently online.
    pub(crate) async fn store_online(&self) -> bool {
        let store = self.store.clone();
        tokio::time::timeout(self.store_timeout, async move {
            store.lock().await.online().await
        })
        .await
        .unwrap_or(false)
    }

    /// Start the worker thread which updates the registry.
    pub(crate) async fn start(&self) {
        let registry = self.clone();
        tokio::spawn(async move {
            registry.poller().await;
        });
        let registry = self.clone();
        self.reconciler.start(registry).await;
    }

    /// Stops the core registry, which at the moment only revokes the persistent store lease.
    pub(crate) async fn stop(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            let store = self.store.lock().await;
            store.revoke().await
        })
        .await
        .ok();
    }

    /// Initialise the registry with the content of the persistent store.
    async fn init(&self) {
        let mut store = self.store.lock().await;
        self.specs.init(store.deref_mut()).await;
    }

    /// Send a triggered event signal to the reconciler module.
    pub(crate) async fn notify(&self, event: PollTriggerEvent) {
        self.reconciler.notify(PollEvent::Triggered(event)).await
    }

    /// Poll each node for resource updates.
    async fn poller(&self) {
        loop {
            {
                // Clone the nodes so we don't hold the read lock on the nodes list while
                // we may be busy or waiting on node information being fetched.
                let nodes = self.nodes().read().await.clone();

                let polled = nodes
                    .into_iter()
                    .map(|(_, node)| Self::poll_node(node))
                    .collect::<Vec<_>>();

                // Polls all nodes "concurrently".
                // This means we still have to wait for all of them, would it be better to have
                // a completely separate poll task for each node?
                futures::future::join_all(polled).await;
            }
            tokio::time::sleep(self.cache_period).await;
        }
    }

    /// Poll the given node for resource updates.
    async fn poll_node(node: Arc<RwLock<NodeWrapper>>) {
        let (id, online) = {
            let node = node.read().await;
            (node.id().clone(), node.is_online())
        };
        if online {
            if let Err(error) = node.update_all(false).await {
                tracing::error!(node.id = %id, %error, "Failed to reload node");
            }
        }
    }

    /// Determine if a rebuild is allowed to start.
    /// Constrain the number of system-wide rebuilds to the maximum specified.
    /// If a maximum is not specified, do not limit the number of rebuilds.
    pub(crate) async fn rebuild_allowed(&self) -> Result<(), SvcError> {
        match self.max_rebuilds {
            Some(max_rebuilds) => {
                let mut num_rebuilds = 0;
                for (_id, node_wrapper) in self.nodes.read().await.iter() {
                    num_rebuilds += node_wrapper.read().await.num_rebuilds();
                }

                if num_rebuilds < max_rebuilds {
                    Ok(())
                } else {
                    Err(SvcError::MaxRebuilds { max_rebuilds })
                }
            }
            None => Ok(()),
        }
    }

    /// Returns whether or not the node with the given ID is cordoned.
    pub(crate) fn node_cordoned(&self, node_id: &NodeId) -> Result<bool, SvcError> {
        Ok(self.specs.node(node_id)?.cordoned())
    }

    /// Get the allowed host nqn for the given dataplane node.
    pub(crate) async fn node_nqn(&self, node_id: &NodeId) -> Result<Vec<HostNqn>, SvcError> {
        Ok(match self.host_acl.contains(&HostAccessControl::Replicas) {
            true => {
                let node = self.node_state(node_id).await?.node_nqn;
                node.into_iter().collect::<Vec<_>>()
            }
            false => vec![],
        })
    }

    /// Get the allowed nqn's for the given request, if enabled.
    pub(crate) fn host_acl_nodename(
        &self,
        req: HostAccessControl,
        nodes: &[String],
    ) -> Vec<InitiatorAC> {
        match self.host_acl.contains(&req) {
            true => nodes
                .iter()
                .map(|nodename| {
                    InitiatorAC::new(nodename.clone(), HostNqn::from_nodename(nodename))
                })
                .collect(),
            false => vec![],
        }
    }
}
