use crate::core::registry::Registry;
use common::errors::SvcError;
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

use std::{collections::HashMap, ops::Deref, sync::Arc};

use async_trait::async_trait;
use mbus_api::ResourceKind;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock};
use types::v0::store::{definitions::ObjectKey, SpecState};

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

/// This trait is used to encapsulate common behaviour for all different types of resources,
/// including validation rules and error handling.
#[async_trait]
pub trait SpecOperations: Clone + Sized + StorableObject {
    type Create: PartialEq + Sync + Send;
    type State: PartialEq;
    type Status: PartialEq + Sync + Send;
    type UpdateOp: Sync + Send;

    /// Start a create operation and attempt to log the transaction to the store.
    /// In case of error, the log is undone and an error is returned.
    async fn start_create<O>(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<(), SvcError>
    where
        Self: PartialEq<Self::Create>,
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        let mut spec = locked_spec.lock().await;
        spec.start_create_inner(request)?;
        let spec_clone = spec.clone();
        drop(spec);

        if let Err(error) = registry.store_obj(&spec_clone).await {
            let mut spec = locked_spec.lock().await;
            spec.clear_op();
            Err(error)
        } else {
            Ok(())
        }
    }

    /// When a create request is issued we need to validate by verifying that:
    /// 1. a previous create operation is no longer in progress
    /// 2. if it's a retry then it must have the same parameters as the original request
    fn start_create_inner(&mut self, request: &Self::Create) -> Result<(), SvcError>
    where
        Self: PartialEq<Self::Create>,
    {
        // we're busy with another request, try again later
        let _ = self.busy()?;
        if self.state().creating() {
            if self != request {
                Err(SvcError::ReCreateMismatch {
                    id: self.uuid(),
                    kind: self.kind(),
                })
            } else {
                self.start_create_op();
                Ok(())
            }
        } else if self.state().created() {
            Err(SvcError::AlreadyExists {
                kind: self.kind(),
                id: self.uuid(),
            })
        } else {
            Err(SvcError::Deleting {})
        }
    }

    /// Completes a create operation by trying to update the spec in the persistent store.
    /// If the persistent store operation fails then the spec is marked accordingly and the dirty
    /// spec reconciler will attempt to update the store when the store is back online.
    /// todo: The state of the object is left as Creating for now. Determine whether to set it to
    /// Deleted or let the reconciler clean it up.
    async fn complete_create<O, R: Send>(
        result: Result<R, SvcError>,
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
    ) -> Result<R, SvcError>
    where
        Self: SpecTransaction<O>,
    {
        match result {
            Ok(val) => {
                let mut spec_clone = locked_spec.lock().await.clone();
                spec_clone.commit_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock().await;
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
                let mut spec_clone = locked_spec.lock().await.clone();
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock().await;
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

    /// Start a destroy operation and attempt to log the transaction to the store.
    /// In case of error, the log is undone and an error is returned.
    async fn start_destroy<O>(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
        del_owned: bool,
    ) -> Result<(), SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        let mut spec = locked_spec.lock().await;
        // we're busy with another request, try again later
        let _ = spec.busy()?;
        if spec.state().deleted() {
            Ok(())
        } else if !del_owned && spec.owned() {
            Err(SvcError::InUse {
                kind: spec.kind(),
                id: spec.uuid(),
            })
        } else {
            spec.set_updating(true);
            drop(spec);

            // resource specific validation rules
            if let Err(error) = Self::validate_destroy(&locked_spec, registry).await {
                let mut spec = locked_spec.lock().await;
                spec.set_updating(false);
                return Err(error);
            }
            let mut spec = locked_spec.lock().await;

            // once we've started, there's no going back...
            spec.set_state(SpecState::Deleting);

            spec.start_destroy_op();
            let spec_clone = spec.clone();
            drop(spec);

            if let Err(error) = registry.store_obj(&spec_clone).await {
                let mut spec = locked_spec.lock().await;
                spec.clear_op();
                Err(error)
            } else {
                Ok(())
            }
        }
    }

    /// Completes a destroy operation by trying to delete the spec from the persistent store.
    /// If the persistent store operation fails then the spec is marked accordingly and the dirty
    /// spec reconciler will attempt to update the store when the store is back online.
    async fn complete_destroy<O, R: Send>(
        result: Result<R, SvcError>,
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
    ) -> Result<R, SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        let key = locked_spec.lock().await.key();
        match result {
            Ok(val) => {
                let mut spec_clone = locked_spec.lock().await.clone();
                spec_clone.commit_op();
                let deleted = registry.delete_kv(&key.key()).await;
                match deleted {
                    Ok(_) => {
                        Self::remove_spec(locked_spec, registry).await;
                        let mut spec = locked_spec.lock().await;
                        spec.commit_op();
                        Ok(val)
                    }
                    Err(error) => {
                        let mut spec = locked_spec.lock().await;
                        spec.set_op_result(true);
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let mut spec_clone = locked_spec.lock().await.clone();
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock().await;
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

    /// Start a destroy operation and attempt to log the transaction to the store.
    /// In case of error, the log is undone and an error is returned.
    async fn start_update(
        registry: &Registry,
        locked_spec: &Arc<Mutex<Self>>,
        status: &Self::Status,
        update_operation: Self::UpdateOp,
    ) -> Result<Self, SvcError>
    where
        Self: PartialEq<Self::Status>,
        Self: SpecTransaction<Self::UpdateOp>,
        Self: StorableObject,
    {
        let mut spec = locked_spec.lock().await;
        let spec_clone = spec.start_update_inner(status, update_operation, false)?;
        drop(spec);

        if let Err(error) = registry.store_obj(&spec_clone).await {
            let mut spec = locked_spec.lock().await;
            spec.clear_op();
            Err(error)
        } else {
            Ok(spec_clone)
        }
    }

    /// Checks that the object ready to accept a new update operation
    fn start_update_inner(
        &mut self,
        status: &Self::Status,
        operation: Self::UpdateOp,
        reconciling: bool,
    ) -> Result<Self, SvcError>
    where
        Self: PartialEq<Self::Status>,
    {
        // we're busy right now, try again later
        let _ = self.busy()?;

        if !self.state().created() {
            Err(SvcError::PendingCreation {
                id: self.uuid(),
                kind: self.kind(),
            })
        } else {
            // if it's not part of a reconcile effort
            // validate the operation itself against the spec and status
            if !reconciling && (self != status) {
                Err(SvcError::NotReady {
                    id: self.uuid(),
                    kind: self.kind(),
                })
            } else {
                self.start_update_op(status, operation)?;
                Ok(self.clone())
            }
        }
    }

    /// Completes a volume update operation by trying to update the spec in the persistent store.
    /// If the persistent store operation fails then the spec is marked accordingly and the dirty
    /// spec reconciler will attempt to update the store when the store is back online.
    async fn complete_update<R: Send, O>(
        registry: &Registry,
        result: Result<R, SvcError>,
        locked_spec: Arc<Mutex<Self>>,
        mut spec_clone: Self,
    ) -> Result<R, SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        match result {
            Ok(val) => {
                spec_clone.commit_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock().await;
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
                let mut spec = locked_spec.lock().await;
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
    async fn validate_update_step<R: Send, O>(
        registry: &Registry,
        result: Result<R, SvcError>,
        locked_spec: &Arc<Mutex<Self>>,
        spec_clone: &Self,
    ) -> Result<R, SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        match result {
            Ok(val) => Ok(val),
            Err(error) => {
                let mut spec_clone = spec_clone.clone();
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock().await;
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

    /// Check if the object is free to be modified or if it's still busy
    fn busy(&self) -> Result<(), SvcError> {
        if self.updating() {
            return Err(SvcError::Conflict {});
        } else if self.dirty() {
            return Err(SvcError::StoreSave {
                kind: self.kind(),
                id: self.uuid(),
            });
        }
        Ok(())
    }

    /// Start an update operation (not all resources support this currently)
    fn start_update_op(
        &mut self,
        _status: &Self::Status,
        _operation: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        unimplemented!();
    }
    /// Used for resource specific validation rules
    async fn validate_destroy(
        _locked_spec: &Arc<Mutex<Self>>,
        _registry: &Registry,
    ) -> Result<(), SvcError> {
        Ok(())
    }
    /// Start a create transaction
    fn start_create_op(&mut self);
    /// Start a destroy transaction
    fn start_destroy_op(&mut self);
    /// Remove the object from the global Spec List
    async fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry);
    /// Set the updating flag
    fn set_updating(&mut self, updating: bool);
    /// Check if the object is currently being updated
    fn updating(&self) -> bool;
    /// Check if the object is dirty -> needs to be flushed to the persistent store
    fn dirty(&self) -> bool;
    /// Get the kind (for log messages)
    fn kind(&self) -> ResourceKind;
    /// Get the UUID as a string (for log messages)
    fn uuid(&self) -> String;
    /// Get the state of the object
    fn state(&self) -> SpecState<Self::State>;
    /// Set the state of the object
    fn set_state(&mut self, state: SpecState<Self::State>);
    /// Check if the object is owned by another
    fn owned(&self) -> bool {
        false
    }
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
}
