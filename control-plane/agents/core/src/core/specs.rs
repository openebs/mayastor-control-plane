use crate::core::registry::Registry;
use std::{collections::HashMap, ops::Deref, sync::Arc};

use tokio::sync::{Mutex, RwLock};

use snafu::{OptionExt, ResultExt, Snafu};
use types::v0::{
    message_bus::mbus::{NexusId, NodeId, PoolId, ReplicaId, VolumeId},
    store::{
        definitions::{key_prefix, StorableObject, StorableObjectType, Store, StoreError},
        nexus::NexusSpec,
        node::NodeSpec,
        pool::PoolSpec,
        replica::ReplicaSpec,
        volume::VolumeSpec,
        SpecTransaction,
    },
};

use common::errors::SvcError;

#[derive(Debug, Snafu)]
enum SpecError {
    /// Failed to get entries from the persistent store.
    #[snafu(display("Failed to get entries from store. Error {}", source))]
    StoreGet { source: StoreError },
    /// Failed to get entries from the persistent store.
    #[snafu(display("Failed to deserialise object type {}", obj_type))]
    Deserialise {
        obj_type: StorableObjectType,
        source: serde_json::Error,
    },
    /// Failed to get entries from the persistent store.
    #[snafu(display("Key does not contain UUID"))]
    KeyUuid {},
}

/// Locked Resource Specs
#[derive(Default, Clone, Debug)]
pub(crate) struct ResourceSpecsLocked(Arc<RwLock<ResourceSpecs>>);

impl Deref for ResourceSpecsLocked {
    type Target = Arc<RwLock<ResourceSpecs>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Resource Specs
#[derive(Default, Debug)]
pub(crate) struct ResourceSpecs {
    pub(crate) volumes: HashMap<VolumeId, Arc<Mutex<VolumeSpec>>>,
    pub(crate) nodes: HashMap<NodeId, Arc<Mutex<NodeSpec>>>,
    pub(crate) nexuses: HashMap<NexusId, Arc<Mutex<NexusSpec>>>,
    pub(crate) pools: HashMap<PoolId, Arc<Mutex<PoolSpec>>>,
    pub(crate) replicas: HashMap<ReplicaId, Arc<Mutex<ReplicaSpec>>>,
}

impl ResourceSpecsLocked {
    pub(crate) fn new() -> Self {
        ResourceSpecsLocked::default()
    }

    /// Initialise the resource specs with the content from the persistent store.
    pub(crate) async fn init<S: Store>(&self, store: &mut S) {
        let spec_types = [
            StorableObjectType::VolumeSpec,
            StorableObjectType::NodeSpec,
            StorableObjectType::NexusSpec,
            StorableObjectType::PoolSpec,
            StorableObjectType::ReplicaSpec,
        ];
        for spec in &spec_types {
            if let Err(e) = self.populate_specs(store, *spec).await {
                panic!(
                    "Failed to initialise resource specs. Err {}.",
                    e.to_string()
                );
            }
        }
    }

    /// Populate the resource specs with data from the persistent store.
    async fn populate_specs<S: Store>(
        &self,
        store: &mut S,
        spec_type: StorableObjectType,
    ) -> Result<(), SpecError> {
        let prefix = key_prefix(spec_type);
        let store_specs = store.get_values_prefix(&prefix).await.context(StoreGet {});
        let mut resource_specs = self.0.write().await;

        assert!(store_specs.is_ok());
        for (key, value) in store_specs.unwrap() {
            // The uuid is assumed to be the last part of the key.
            let id = key.split('/').last().context(KeyUuid {})?;
            match spec_type {
                StorableObjectType::VolumeSpec => {
                    resource_specs.volumes.insert(
                        VolumeId::from(id),
                        Arc::new(Mutex::new(serde_json::from_value(value).context(
                            Deserialise {
                                obj_type: StorableObjectType::VolumeSpec,
                            },
                        )?)),
                    );
                }
                StorableObjectType::NodeSpec => {
                    resource_specs.nodes.insert(
                        NodeId::from(id),
                        Arc::new(Mutex::new(serde_json::from_value(value).context(
                            Deserialise {
                                obj_type: StorableObjectType::NodeSpec,
                            },
                        )?)),
                    );
                }
                StorableObjectType::NexusSpec => {
                    resource_specs.nexuses.insert(
                        NexusId::from(id),
                        Arc::new(Mutex::new(serde_json::from_value(value).context(
                            Deserialise {
                                obj_type: StorableObjectType::NexusSpec,
                            },
                        )?)),
                    );
                }
                StorableObjectType::PoolSpec => {
                    resource_specs.pools.insert(
                        PoolId::from(id),
                        Arc::new(Mutex::new(serde_json::from_value(value).context(
                            Deserialise {
                                obj_type: StorableObjectType::PoolSpec,
                            },
                        )?)),
                    );
                }
                StorableObjectType::ReplicaSpec => {
                    resource_specs.replicas.insert(
                        ReplicaId::from(id),
                        Arc::new(Mutex::new(serde_json::from_value(value).context(
                            Deserialise {
                                obj_type: StorableObjectType::ReplicaSpec,
                            },
                        )?)),
                    );
                }
                _ => {
                    // Not all spec types are persisted in the store.
                    unimplemented!("{} not persisted in store", spec_type);
                }
            };
        }
        Ok(())
    }

    /// Start worker threads
    /// 1. test store connections and commit dirty specs to the store
    pub(crate) fn start(&self, registry: Registry) {
        let this = self.clone();
        tokio::spawn(async move { this.reconcile_dirty_specs(registry).await });
    }

    /// Reconcile dirty specs to the persistent store
    async fn reconcile_dirty_specs(&self, registry: Registry) {
        loop {
            let dirty_replicas = self.reconcile_dirty_replicas(&registry).await;
            let dirty_nexuses = self.reconcile_dirty_nexuses(&registry).await;

            let period = if dirty_nexuses || dirty_replicas {
                registry.reconcile_period
            } else {
                registry.reconcile_idle_period
            };

            tokio::time::delay_for(period).await;
        }
    }

    /// Completes a volume update operation by trying to update the spec in the persistent store.
    /// If the persistent store operation fails then the spec is marked accordingly and the dirty
    /// spec reconciler will attempt to update the store when the store is back online.
    pub(crate) async fn spec_complete_op<R, O, S: SpecTransaction<O> + StorableObject>(
        registry: &Registry,
        result: Result<R, SvcError>,
        spec: Arc<Mutex<S>>,
        mut spec_clone: S,
    ) -> Result<R, SvcError> {
        match result {
            Ok(val) => {
                spec_clone.commit_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = spec.lock().await;
                match stored {
                    Ok(_) => {
                        spec.commit_op();
                        Ok(val)
                    }
                    Err(error) => {
                        spec.set_op_result(true);
                        Err(error)
                    }
                }
            }
            Err(error) => {
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = spec.lock().await;
                match stored {
                    Ok(_) => {
                        spec.clear_op();
                        Err(error)
                    }
                    Err(error) => {
                        spec.set_op_result(false);
                        Err(error)
                    }
                }
            }
        }
    }

    /// Validates the outcome of an intermediate step, part of a transaction operation.
    /// In case of an error, it undoes the changes to the spec.
    /// If the persistent store is unavailable the spec is marked as dirty and the dirty
    /// spec reconciler will attempt to update the store when the store is back online.
    pub(crate) async fn spec_step_op<R, O, S: SpecTransaction<O> + StorableObject>(
        registry: &Registry,
        result: Result<R, SvcError>,
        spec: Arc<Mutex<S>>,
        mut spec_clone: S,
    ) -> Result<R, SvcError> {
        match result {
            Ok(val) => Ok(val),
            Err(error) => {
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = spec.lock().await;
                match stored {
                    Ok(_) => {
                        spec.clear_op();
                        Err(error)
                    }
                    Err(error) => {
                        spec.set_op_result(false);
                        Err(error)
                    }
                }
            }
        }
    }
}
