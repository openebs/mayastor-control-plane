use crate::core::registry::Registry;
use parking_lot::{Mutex, RwLock};
use std::{ops::Deref, sync::Arc};

use common_lib::types::v0::{
    message_bus::{NexusId, NodeId, PoolId, ReplicaId, VolumeId},
    store::{
        definitions::{
            key_prefix, ObjectKey, StorableObject, StorableObjectType, Store, StoreError,
        },
        nexus::NexusSpec,
        node::NodeSpec,
        pool::PoolSpec,
        replica::ReplicaSpec,
        volume::VolumeSpec,
        SpecTransaction,
    },
};

use crate::core::resource_map::ResourceMap;
use async_trait::async_trait;
use common::errors::SvcError;
use common_lib::{
    mbus_api::ResourceKind,
    types::v0::store::{
        OperationGuard, OperationMode, OperationSequence, OperationSequencer, SpecStatus,
    },
};
use serde::de::DeserializeOwned;
use snafu::{ResultExt, Snafu};
use std::fmt::Debug;

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
pub trait SpecOperations: Clone + Debug + Sized + StorableObject + OperationSequencer {
    type Create: Debug + PartialEq + Sync + Send;
    type Owners: Default + Sync + Send;
    type Status: PartialEq;
    type State: PartialEq + Sync + Send;
    type UpdateOp: Sync + Send;

    /// Start a create operation and attempt to log the transaction to the store.
    /// In case of error, the log is undone and an error is returned.
    async fn start_create<O>(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
        request: &Self::Create,
        mode: OperationMode,
    ) -> Result<(Self, OperationGuard<Self>), SvcError>
    where
        Self: PartialEq<Self::Create>,
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        let guard = locked_spec.operation_guard(mode)?;
        let spec_clone = {
            let mut spec = locked_spec.lock();
            spec.start_create_inner(request)?;
            spec.clone()
        };
        Self::store_operation_log(registry, locked_spec, &spec_clone).await?;
        Ok((spec_clone, guard))
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
        if self.status().creating() {
            if self != request {
                Err(SvcError::ReCreateMismatch {
                    id: self.uuid(),
                    kind: self.kind(),
                    resource: format!("{:?}", self),
                    request: format!("{:?}", request),
                })
            } else {
                self.start_create_op();
                Ok(())
            }
        } else if self.status().created() {
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
                let mut spec_clone = locked_spec.lock().clone();
                spec_clone.commit_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock();
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
                let mut spec_clone = locked_spec.lock().clone();
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock();
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
    /// If the del_owned flag is set, then we skip the check for owners.
    /// Otherwise, if the spec is still owned then we cannot proceed with deletion.
    async fn start_destroy<O>(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
        del_owned: bool,
        mode: OperationMode,
    ) -> Result<OperationGuard<Self>, SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        Self::start_destroy_by(
            locked_spec,
            registry,
            &Self::Owners::default(),
            del_owned,
            mode,
        )
        .await
    }

    /// Start a destroy operation by spec owners and attempt to log the transaction to the store.
    /// In case of error, the log is undone and an error is returned.
    /// If the del_owned flag is set, then we skip the check for owners.
    /// The del_by parameter specifies who is trying to delete the resource. If the resource has any
    /// other owners then we cannot proceed with deletion but we disown the resource from del_by.
    async fn start_destroy_by<O>(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
        owners: &Self::Owners,
        ignore_owners: bool,
        mode: OperationMode,
    ) -> Result<OperationGuard<Self>, SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        let guard = locked_spec.operation_guard_wait(mode).await?;
        {
            let mut spec = locked_spec.lock();
            let _ = spec.busy()?;
            if spec.status().deleted() {
                return Ok(guard);
            } else if !ignore_owners {
                spec.disown(owners);
                if spec.owned() {
                    tracing::error!(
                        "{:?} id '{:?}' cannot be deleted because it's owned by: '{:?}'",
                        spec.kind(),
                        spec.uuid(),
                        spec.owners()
                    );
                    return Err(SvcError::InUse {
                        kind: spec.kind(),
                        id: spec.uuid(),
                    });
                }
            }
        }

        // resource specific validation rules
        if let Err(error) = Self::validate_destroy(locked_spec, registry) {
            return Err(error);
        }

        let spec_clone = {
            let mut spec = locked_spec.lock();

            // once we've started, there's no going back...
            spec.set_status(SpecStatus::Deleting);

            spec.start_destroy_op();
            spec.clone()
        };

        Self::store_operation_log(registry, locked_spec, &spec_clone).await?;
        Ok(guard)
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
        let key = locked_spec.lock().key();
        match result {
            Ok(val) => {
                let mut spec_clone = locked_spec.lock().clone();
                spec_clone.commit_op();
                let deleted = registry.delete_kv(&key.key()).await;
                match deleted {
                    Ok(_) => {
                        Self::remove_spec(locked_spec, registry);
                        let mut spec = locked_spec.lock();
                        spec.commit_op();
                        Ok(val)
                    }
                    Err(error) => {
                        let mut spec = locked_spec.lock();
                        spec.set_op_result(true);
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let mut spec_clone = locked_spec.lock().clone();
                spec_clone.clear_op();
                let stored = registry.store_obj(&spec_clone).await;
                let mut spec = locked_spec.lock();
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

    /// Start an update operation and attempt to log the transaction to the store.
    /// In case of error, the log is undone and an error is returned.
    async fn start_update(
        registry: &Registry,
        locked_spec: &Arc<Mutex<Self>>,
        state: &Self::State,
        update_operation: Self::UpdateOp,
        mode: OperationMode,
    ) -> Result<(Self, OperationGuard<Self>), SvcError>
    where
        Self: PartialEq<Self::State>,
        Self: SpecTransaction<Self::UpdateOp>,
        Self: StorableObject,
    {
        let guard = locked_spec.operation_guard_wait(mode).await?;
        let spec_clone = {
            let mut spec = locked_spec.lock().clone();
            spec.start_update_inner(registry, state, update_operation)?;
            *locked_spec.lock() = spec.clone();
            spec
        };

        Self::store_operation_log(registry, locked_spec, &spec_clone).await?;
        Ok((spec_clone, guard))
    }

    /// Checks that the object ready to accept a new update operation
    fn start_update_inner(
        &mut self,
        registry: &Registry,
        state: &Self::State,
        operation: Self::UpdateOp,
    ) -> Result<(), SvcError>
    where
        Self: PartialEq<Self::State>,
    {
        // we're busy right now, try again later
        let _ = self.busy()?;

        match self.status() {
            SpecStatus::Creating => Err(SvcError::PendingCreation {
                id: self.uuid(),
                kind: self.kind(),
            }),
            SpecStatus::Deleted | SpecStatus::Deleting => Err(SvcError::PendingDeletion {
                id: self.uuid(),
                kind: self.kind(),
            }),
            SpecStatus::Created(_) => {
                // start the requested operation (which also checks if it's a valid transition)
                self.start_update_op(registry, state, operation)?;
                Ok(())
            }
        }
    }

    /// Completes an update operation by trying to update the spec in the persistent store.
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
                let mut spec = locked_spec.lock();
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
                let mut spec = locked_spec.lock();
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
                let mut spec = locked_spec.lock();
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
        if self.dirty() {
            return Err(SvcError::StoreSave {
                kind: self.kind(),
                id: self.uuid(),
            });
        }
        Ok(())
    }
    fn operation_lock(&self) -> &OperationSequence {
        self.as_ref()
    }
    fn operation_lock_mut(&mut self) -> &mut OperationSequence {
        self.as_mut()
    }
    /// Attempt to store a spec object with a logged SpecOperation to the persistent store
    /// In case of failure the operation cannot proceed so clear it and return an error
    async fn store_operation_log<O>(
        registry: &Registry,
        locked_spec: &Arc<Mutex<Self>>,
        spec_clone: &Self,
    ) -> Result<(), SvcError>
    where
        Self: SpecTransaction<O>,
        Self: StorableObject,
    {
        if let Err(error) = registry.store_obj(spec_clone).await {
            let mut spec = locked_spec.lock();
            spec.clear_op();
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Start an update operation (not all resources support this currently)
    fn start_update_op(
        &mut self,
        _registry: &Registry,
        _state: &Self::State,
        _operation: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        unimplemented!();
    }
    /// Used for resource specific validation rules
    fn validate_destroy(
        _locked_spec: &Arc<Mutex<Self>>,
        _registry: &Registry,
    ) -> Result<(), SvcError> {
        Ok(())
    }
    /// Check if the state is in sync with the spec
    fn state_synced(&self, state: &Self::State) -> bool
    where
        Self: PartialEq<Self::State>,
    {
        // todo: do the check explicitly on each specialization rather than using PartialEq
        self == state
    }
    /// Start a create transaction
    fn start_create_op(&mut self);
    /// Start a destroy transaction
    fn start_destroy_op(&mut self);
    /// Remove the object from the global Spec List
    fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry);
    /// Check if the object is dirty -> needs to be flushed to the persistent store
    fn dirty(&self) -> bool;
    /// Get the kind (for log messages)
    fn kind(&self) -> ResourceKind;
    /// Get the UUID as a string (for log messages)
    fn uuid(&self) -> String;
    /// Get the state of the object
    fn status(&self) -> SpecStatus<Self::Status>;
    /// Set the state of the object
    fn set_status(&mut self, state: SpecStatus<Self::Status>);
    /// Check if the object is owned by another
    fn owned(&self) -> bool {
        false
    }
    /// Get a human readable list of owners
    fn owners(&self) -> Option<String> {
        None
    }
    /// Disown resource by owners
    fn disown(&mut self, _owner: &Self::Owners) {}
}

/// Operations are locked
#[async_trait::async_trait]
pub trait OperationSequenceGuard<T: OperationSequencer + SpecOperations> {
    /// Attempt to obtain a guard for the specified operation mode
    fn operation_guard(&self, mode: OperationMode) -> Result<OperationGuard<T>, SvcError>;
    /// Attempt to obtain a guard for the specified operation mode
    /// A few attempts are made with an async sleep in case something else is already running
    async fn operation_guard_wait(
        &self,
        mode: OperationMode,
    ) -> Result<OperationGuard<T>, SvcError>;
}

#[async_trait::async_trait]
impl<T: OperationSequencer + SpecOperations> OperationSequenceGuard<T> for Arc<Mutex<T>> {
    fn operation_guard(&self, mode: OperationMode) -> Result<OperationGuard<T>, SvcError> {
        match OperationGuard::try_sequence(self, mode) {
            Ok(guard) => Ok(guard),
            Err(error) => {
                tracing::trace!("Resource '{}' is busy: {}", self.lock().uuid(), error);
                Err(SvcError::Conflict {})
            }
        }
    }
    async fn operation_guard_wait(
        &self,
        mode: OperationMode,
    ) -> Result<OperationGuard<T>, SvcError> {
        let mut tries = 10;
        loop {
            match self.operation_guard(mode) {
                Ok(guard) => return Ok(guard),
                Err(error) if tries == 0 => {
                    return Err(error);
                }
                Err(_) => tries -= 1,
            };

            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
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
    pub(crate) volumes: ResourceMap<VolumeId, VolumeSpec>,
    pub(crate) nodes: ResourceMap<NodeId, NodeSpec>,
    pub(crate) nexuses: ResourceMap<NexusId, NexusSpec>,
    pub(crate) pools: ResourceMap<PoolId, PoolSpec>,
    pub(crate) replicas: ResourceMap<ReplicaId, ReplicaSpec>,
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

    /// Populate the resource specs with data from the persistent store.
    async fn populate_specs<S: Store>(
        &self,
        store: &mut S,
        spec_type: StorableObjectType,
    ) -> Result<(), SpecError> {
        let prefix = key_prefix(spec_type);
        let store_entries = store
            .get_values_prefix(&prefix)
            .await
            .context(StoreGet {})?;
        let store_values = store_entries.iter().map(|e| e.1.clone()).collect();

        let mut resource_specs = self.0.write();
        match spec_type {
            StorableObjectType::VolumeSpec => {
                let specs =
                    Self::deserialise_specs::<VolumeSpec>(store_values).context(Deserialise {
                        obj_type: StorableObjectType::VolumeSpec,
                    })?;
                resource_specs.volumes.populate(specs);
            }
            StorableObjectType::NodeSpec => {
                let specs =
                    Self::deserialise_specs::<NodeSpec>(store_values).context(Deserialise {
                        obj_type: StorableObjectType::NodeSpec,
                    })?;
                resource_specs.nodes.populate(specs);
            }
            StorableObjectType::NexusSpec => {
                let specs =
                    Self::deserialise_specs::<NexusSpec>(store_values).context(Deserialise {
                        obj_type: StorableObjectType::NexusSpec,
                    })?;
                resource_specs.nexuses.populate(specs);
            }
            StorableObjectType::PoolSpec => {
                let specs =
                    Self::deserialise_specs::<PoolSpec>(store_values).context(Deserialise {
                        obj_type: StorableObjectType::PoolSpec,
                    })?;
                resource_specs.pools.populate(specs);
            }
            StorableObjectType::ReplicaSpec => {
                let specs =
                    Self::deserialise_specs::<ReplicaSpec>(store_values).context(Deserialise {
                        obj_type: StorableObjectType::ReplicaSpec,
                    })?;
                resource_specs.replicas.populate(specs);
            }
            _ => {
                // Not all spec types are persisted in the store.
                unimplemented!("{} not persisted in store", spec_type);
            }
        };
        Ok(())
    }
}
