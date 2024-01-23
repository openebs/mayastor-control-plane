use crate::{error::SvcError, resource_map::ResourceMutexMap};
use parking_lot::RwLock;
use pstor::{
    etcd::Etcd, key_prefix_obj, ObjectKey, StorableObject, StorableObjectType, Store, StoreKey,
    StoreKv, StoreObj, API_VERSION,
};
use pstor_proxy::types::{
    frontend_node::{
        DeregisterFrontendNode, FrontendNode, FrontendNodeId, FrontendNodeKey, FrontendNodeSpec,
        RegisterFrontendNode,
    },
    misc::{PaginatedResult, Pagination},
};
use serde::de::DeserializeOwned;
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::Mutex;

const PSTOR_PROXY_SERVICE_NAME: &str = "pstor-proxy";

/// Registry containing all io-engine instances (aka nodes).
#[derive(Clone, Debug)]
pub(crate) struct Registry {
    inner: Arc<RegistryInner<Etcd>>,
}

impl Deref for Registry {
    type Target = Arc<RegistryInner<Etcd>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Locked Resource Specs.
#[derive(Default, Clone, Debug)]
pub(crate) struct ResourceSpecsLocked(Arc<RwLock<ResourceSpecs>>);

impl Deref for ResourceSpecsLocked {
    type Target = Arc<RwLock<ResourceSpecs>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Resource Specs.
#[derive(Default, Debug)]
pub(crate) struct ResourceSpecs {
    pub(crate) frontend_nodes: ResourceMutexMap<FrontendNodeId, FrontendNodeSpec>,
}

/// Generic Registry Inner with a Store trait.
#[derive(Debug)]
pub(crate) struct RegistryInner<S: Store> {
    /// spec (aka desired state) of the various resources.
    specs: ResourceSpecsLocked,
    store: Arc<Mutex<S>>,
    /// store gRPC operation timeout.
    store_timeout: std::time::Duration,
}

impl Registry {
    pub(crate) async fn new(
        store_url: String,
        store_timeout: std::time::Duration,
        store_lease_tll: std::time::Duration,
    ) -> Result<Self, SvcError> {
        let store_endpoint = Self::format_store_endpoint(&store_url);
        tracing::info!("Connecting to persistent store at {}", store_endpoint);
        let store = Etcd::new_leased(
            [&store_url],
            PSTOR_PROXY_SERVICE_NAME.into(),
            store_lease_tll,
        )
        .await
        .map_err(|error| SvcError::Store { source: error })?;
        tracing::info!("Connected to persistent store at {}", store_endpoint);

        let registry = Self {
            inner: Arc::new(RegistryInner {
                specs: ResourceSpecsLocked::new(),
                store: Arc::new(Mutex::new(store.clone())),
                store_timeout,
            }),
        };

        registry.init().await?;

        Ok(registry)
    }

    /// Formats the store endpoint with a default port if one isn't supplied.
    fn format_store_endpoint(endpoint: &str) -> String {
        match endpoint.contains(':') {
            true => endpoint.to_string(),
            false => format!("{}:{}", endpoint, "2379"),
        }
    }

    /// Initialise the registry with the content of the persistent store.
    async fn init(&self) -> Result<(), SvcError> {
        let mut store = self.store.lock().await;
        self.specs.init(store.deref_mut()).await?;
        Ok(())
    }

    /// Stops the pstor-proxy registry, which revokes the persistent store lease.
    pub(crate) async fn stop(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            let store = self.store.lock().await;
            store.revoke().await
        })
        .await
        .ok();
    }

    /// Get a reference to the locked resource specs object.
    fn specs(&self) -> &ResourceSpecsLocked {
        &self.specs
    }

    /// Serialized write to the persistent store.
    async fn store_obj<O: StorableObject>(&self, object: &O) -> Result<(), SvcError> {
        let store = self.store.clone();
        match tokio::time::timeout(self.store_timeout, async move {
            let mut store = store.lock().await;
            Self::op_with_threshold(async move { store.put_obj(object).await }).await
        })
        .await
        {
            Ok(result) => result.map_err(|error| SvcError::Store { source: error }),
            Err(_) => Err(SvcError::Store {
                source: pstor::error::Error::Timeout {
                    operation: "Put".to_string(),
                    timeout: self.store_timeout,
                },
            }),
        }
    }

    /// Serialized delete to the persistent store.
    async fn delete_kv<K: StoreKey>(&self, key: &K) -> Result<(), SvcError> {
        let store = self.store.clone();
        match tokio::time::timeout(self.store_timeout, async move {
            let mut store = store.lock().await;
            Self::op_with_threshold(async move { store.delete_kv(key).await }).await
        })
        .await
        {
            Ok(result) => match result {
                Ok(_) => Ok(()),
                Err(pstor::Error::MissingEntry { .. }) => {
                    tracing::warn!("Entry with key {} missing from store.", key.to_string());
                    Ok(())
                }
                Err(error) => Err(SvcError::Store { source: error }),
            },
            Err(_) => Err(SvcError::Store {
                source: pstor::error::Error::Timeout {
                    operation: "Delete".to_string(),
                    timeout: self.store_timeout,
                },
            }),
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
            tracing::warn!("Store operation took longer than {:?}", warn_threshold);
        }
        result
    }

    /// Register a frontend node
    pub(crate) async fn register_frontend_node(
        &self,
        frontend_node_spec: &RegisterFrontendNode,
    ) -> Result<(), SvcError> {
        self.specs().register_node(self, frontend_node_spec).await?;
        Ok(())
    }

    /// Deregister a frontend node.
    pub(crate) async fn deregister_frontend_node(
        &self,
        frontend_node_spec: &DeregisterFrontendNode,
    ) -> Result<(), SvcError> {
        self.specs()
            .deregister_node(self, frontend_node_spec)
            .await?;
        Ok(())
    }

    /// Get the frontend node by id.
    pub(crate) async fn frontend_node(
        &self,
        id: &FrontendNodeId,
    ) -> Result<FrontendNode, SvcError> {
        let spec = self.specs().frontend_node_spec(id);
        let Some(spec) = spec else {
            return Err(SvcError::FrontendNodeNotFound {
                resource_id: id.to_string(),
            });
        };
        Ok(FrontendNode::new(spec, None))
    }

    /// Get all frontend nodes.
    pub(crate) async fn frontend_nodes(&self) -> Vec<FrontendNode> {
        let volume_specs = self.specs().frontend_nodes();
        let mut frontend_nodes = Vec::with_capacity(volume_specs.len());
        for spec in volume_specs {
            frontend_nodes.push(FrontendNode::new(spec, None));
        }
        frontend_nodes
    }

    /// Get a paginated subset of volumes.
    pub(super) async fn paginated_frontend_nodes(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<FrontendNode> {
        let frontend_node_specs = self.specs().paginated_frontend_nodes(pagination);
        let mut frontend_nodes = Vec::with_capacity(frontend_node_specs.len());
        let last = frontend_node_specs.last();
        for spec in frontend_node_specs.result() {
            frontend_nodes.push(FrontendNode::new(spec, None));
        }
        PaginatedResult::new(frontend_nodes, last)
    }
}

impl ResourceSpecsLocked {
    pub(crate) fn new() -> Self {
        ResourceSpecsLocked::default()
    }

    /// Initialise the resource specs with the content from the persistent store.
    async fn init<S: Store>(&self, store: &mut S) -> Result<(), SvcError> {
        let spec_types = [StorableObjectType::FrontendNodeSpec];
        for spec in &spec_types {
            self.populate_specs(store, *spec).await?;
        }
        Ok(())
    }

    /// Get the frontend spec.
    fn frontend_node_spec(&self, id: &FrontendNodeId) -> Option<FrontendNodeSpec> {
        let specs = self.read();
        match specs.frontend_nodes.get(id) {
            None => None,
            Some(frontend_node_spec) => {
                let resource = frontend_node_spec.lock().clone();
                Some(resource)
            }
        }
    }

    /// Remove the frontend spec from registry.
    fn remove_frontend_node_spec(&self, id: &FrontendNodeId) {
        let mut specs = self.write();
        specs.frontend_nodes.remove(id);
    }

    async fn populate_specs<S: Store>(
        &self,
        store: &mut S,
        spec_type: StorableObjectType,
    ) -> Result<(), SvcError> {
        let prefix = key_prefix_obj(spec_type, API_VERSION);
        let store_entries = store
            .get_values_prefix(&prefix)
            .await
            .map_err(|error| SvcError::Store { source: error })?;
        let store_values = store_entries.iter().map(|e| e.1.clone()).collect();

        let mut resource_specs = self.0.write();
        if let StorableObjectType::FrontendNodeSpec = spec_type {
            let specs = Self::deserialise_specs::<FrontendNodeSpec>(store_values)
                .map_err(|error| SvcError::SpecDeserialize { source: error })?;
            resource_specs.frontend_nodes.populate(specs);
        }

        Ok(())
    }

    /// Deserialise a vector of serde_json values into specific spec types.
    /// If deserialisation fails for any object, return an error.
    fn deserialise_specs<T>(values: Vec<serde_json::Value>) -> Result<Vec<T>, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        let specs: Vec<Result<T, serde_json::Error>> = values
            .iter()
            .map(|v| serde_json::from_value(v.clone()))
            .collect();

        let mut result = vec![];
        for spec in specs {
            match spec {
                Ok(s) => {
                    result.push(s);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(result)
    }

    /// Create a node spec for the register request
    async fn register_node(
        &self,
        registry: &Registry,
        frontend_node: &RegisterFrontendNode,
    ) -> Result<FrontendNodeSpec, SvcError> {
        let (changed, spec_to_persist) = {
            let mut specs = self.write();
            match specs.frontend_nodes.get(&frontend_node.id) {
                Some(frontend_node_rsc) => {
                    let mut frontend_node_spec = frontend_node_rsc.lock();
                    let changed = frontend_node_spec.endpoint != frontend_node.endpoint;

                    frontend_node_spec.endpoint = frontend_node.endpoint;
                    (changed, frontend_node_spec.clone())
                }
                None => {
                    let node = FrontendNodeSpec::new(
                        frontend_node.id.clone(),
                        frontend_node.endpoint,
                        Default::default(),
                    );
                    specs.frontend_nodes.insert(node.clone());
                    (true, node)
                }
            }
        };
        if changed {
            registry.store_obj(&spec_to_persist).await?;
        }
        Ok(spec_to_persist)
    }

    /// Create a node spec for the register request
    async fn deregister_node(
        &self,
        registry: &Registry,
        node: &DeregisterFrontendNode,
    ) -> Result<(), SvcError> {
        registry
            .delete_kv(&<DeregisterFrontendNode as Into<FrontendNodeKey>>::into(node.clone()).key())
            .await?;
        self.remove_frontend_node_spec(&node.id);
        Ok(())
    }

    fn frontend_nodes(&self) -> Vec<FrontendNodeSpec> {
        let specs = self.read();
        specs
            .frontend_nodes
            .values()
            .map(|v| v.lock().clone())
            .collect()
    }

    /// Get a subset of volumes based on the pagination argument.
    fn paginated_frontend_nodes(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<FrontendNodeSpec> {
        let specs = self.read();

        let num_frontend_nodes = specs.frontend_nodes.len() as u64;
        let max_entries = pagination.max_entries();
        let offset = std::cmp::min(pagination.starting_token(), num_frontend_nodes);
        let mut last_result = false;
        let length = match offset + max_entries >= num_frontend_nodes {
            true => {
                last_result = true;
                num_frontend_nodes - offset
            }
            false => pagination.max_entries(),
        };

        PaginatedResult::new(specs.frontend_nodes.paginate(offset, length), last_result)
    }
}
