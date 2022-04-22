use crate::{
    core::{
        specs::{ResourceSpecs, ResourceSpecsLocked, SpecOperations},
        wrapper::ClientOps,
    },
    registry::Registry,
};
use common::errors::{SvcError, SvcError::PoolNotFound};
use common_lib::{
    mbus_api::ResourceKind,
    types::v0::{
        message_bus::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, Pool, PoolId, PoolState,
            PoolStatus, Replica, ReplicaId, ReplicaOwners, ReplicaStatus, ShareReplica,
            UnshareReplica,
        },
        store::{
            pool::{PoolOperation, PoolSpec},
            replica::{ReplicaOperation, ReplicaSpec},
            OperationMode, SpecStatus, SpecTransaction,
        },
    },
};
use parking_lot::Mutex;
use std::sync::Arc;

#[async_trait::async_trait]
impl SpecOperations for PoolSpec {
    type Create = CreatePool;
    type Owners = ();
    type Status = PoolStatus;
    type State = PoolState;
    type UpdateOp = ();

    fn validate_destroy(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
    ) -> Result<(), SvcError> {
        let id = locked_spec.lock().id.clone();
        let pool_in_use = registry.specs().pool_has_replicas(&id);
        if pool_in_use {
            Err(SvcError::InUse {
                kind: ResourceKind::Pool,
                id: id.to_string(),
            })
        } else {
            Ok(())
        }
    }
    fn start_create_op(&mut self) {
        self.start_op(PoolOperation::Create);
    }
    fn start_destroy_op(&mut self) {
        self.start_op(PoolOperation::Destroy);
    }
    fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry) {
        let id = locked_spec.lock().id.clone();
        registry.specs().remove_pool(&id);
    }
    fn dirty(&self) -> bool {
        // pools are not updatable currently, so the spec is never dirty (not written to etcd)
        // because it can never change after creation
        false
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Pool
    }
    fn uuid(&self) -> String {
        self.id.to_string()
    }
    fn status(&self) -> SpecStatus<Self::Status> {
        self.status.clone()
    }
    fn set_status(&mut self, status: SpecStatus<Self::Status>) {
        self.status = status;
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}

#[async_trait::async_trait]
impl SpecOperations for ReplicaSpec {
    type Create = CreateReplica;
    type Owners = ReplicaOwners;
    type Status = ReplicaStatus;
    type State = Replica;
    type UpdateOp = ReplicaOperation;

    async fn start_update_op(
        &mut self,
        _: &Registry,
        state: &Self::State,
        op: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        match op {
            ReplicaOperation::Share(_) if self.share.shared() && state.share.shared() => {
                Err(SvcError::AlreadyShared {
                    kind: self.kind(),
                    id: self.uuid(),
                    share: state.share.to_string(),
                })
            }
            ReplicaOperation::Share(_) => Ok(()),
            ReplicaOperation::Unshare if !self.share.shared() && !state.share.shared() => {
                Err(SvcError::NotShared {
                    kind: self.kind(),
                    id: self.uuid(),
                })
            }
            ReplicaOperation::Unshare => Ok(()),
            _ => unreachable!(),
        }?;
        self.start_op(op);
        Ok(())
    }
    fn start_create_op(&mut self) {
        self.start_op(ReplicaOperation::Create);
    }
    fn start_destroy_op(&mut self) {
        self.start_op(ReplicaOperation::Destroy);
    }
    fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry) {
        let uuid = locked_spec.lock().uuid.clone();
        registry.specs().remove_replica(&uuid);
    }
    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Replica
    }
    fn uuid(&self) -> String {
        self.uuid.to_string()
    }
    fn status(&self) -> SpecStatus<Self::Status> {
        self.status.clone()
    }
    fn set_status(&mut self, status: SpecStatus<Self::Status>) {
        self.status = status;
    }
    fn owned(&self) -> bool {
        self.owners.is_owned()
    }
    fn owners(&self) -> Option<String> {
        Some(format!("{:?}", self.owners))
    }
    fn disown(&mut self, owner: &Self::Owners) {
        self.owners.disown(owner)
    }
    fn disown_all(&mut self) {
        self.owners.disown_all();
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Gets list of protected ReplicaSpec's for a given pool `id`
    fn get_pool_replicas(&self, id: &PoolId) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        let mut replicas = vec![];
        for replica in self.replicas.to_vec() {
            if id == &replica.lock().pool {
                replicas.push(replica.clone())
            }
        }
        replicas
    }
    /// Gets all ReplicaSpec's
    pub(crate) fn get_replicas(&self) -> Vec<ReplicaSpec> {
        let mut vector = vec![];
        for object in self.replicas.to_vec() {
            let object = object.lock();
            vector.push(object.clone());
        }
        vector
    }

    /// Get all PoolSpecs
    pub(crate) fn get_pools(&self) -> Vec<PoolSpec> {
        let mut specs = vec![];
        for pool_spec in self.pools.to_vec() {
            specs.push(pool_spec.lock().clone());
        }
        specs
    }
}

impl ResourceSpecsLocked {
    pub(crate) async fn create_pool(
        &self,
        registry: &Registry,
        request: &CreatePool,
        mode: OperationMode,
    ) -> Result<Pool, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        let pool_spec = self.get_or_create_pool(request);
        let (_, _g) = SpecOperations::start_create(&pool_spec, registry, request, mode).await?;

        let result = node.create_pool(request).await;

        let pool_state = SpecOperations::complete_create(result, &pool_spec, registry).await?;
        let pool_spec = pool_spec.lock().clone();
        Ok(Pool::new(pool_spec, pool_state))
    }

    pub(crate) async fn destroy_pool(
        &self,
        registry: &Registry,
        request: &DestroyPool,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        // what if the node is never coming back?
        // do we need a way to forcefully "delete" things?
        let node = registry.get_node_wrapper(&request.node).await?;

        let pool_spec = self.get_locked_pool(&request.id);
        if let Some(pool_spec) = &pool_spec {
            let _guard = SpecOperations::start_destroy(pool_spec, registry, false, mode).await?;

            let result = node.destroy_pool(request).await;
            SpecOperations::complete_destroy(result, pool_spec, registry).await
        } else {
            node.destroy_pool(request).await
        }
    }

    pub(crate) async fn create_replica(
        &self,
        registry: &Registry,
        request: &CreateReplica,
        mode: OperationMode,
    ) -> Result<Replica, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        let replica_spec = self.get_or_create_replica(request);
        let (_, _guard) =
            SpecOperations::start_create(&replica_spec, registry, request, mode).await?;

        let result = node.create_replica(request).await;
        SpecOperations::complete_create(result, &replica_spec, registry).await
    }

    pub(crate) async fn destroy_replica_spec(
        &self,
        registry: &Registry,
        replica: &ReplicaSpec,
        destroy_by: ReplicaOwners,
        delete_owned: bool,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        match Self::get_replica_node(registry, replica).await {
            // Should never happen, but just in case...
            None => Err(SvcError::Internal {
                details: "Failed to find the node where a replica lives".to_string(),
            }),
            Some(node) => {
                self.destroy_replica(
                    registry,
                    &Self::destroy_replica_request(replica.clone(), destroy_by, &node),
                    delete_owned,
                    mode,
                )
                .await
            }
        }
    }

    pub(crate) async fn destroy_replica(
        &self,
        registry: &Registry,
        request: &DestroyReplica,
        delete_owned: bool,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        let replica = self.get_replica(&request.uuid);
        if let Some(replica) = &replica {
            SpecOperations::start_destroy_by(
                replica,
                registry,
                &request.disowners,
                delete_owned,
                mode,
            )
            .await?;

            let result = node.destroy_replica(request).await;
            SpecOperations::complete_destroy(result, replica, registry).await
        } else {
            node.destroy_replica(request).await
        }
    }
    pub(crate) async fn share_replica(
        &self,
        registry: &Registry,
        request: &ShareReplica,
        mode: OperationMode,
    ) -> Result<String, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(replica_spec) = self.get_replica(&request.uuid) {
            let status = registry.get_replica(&request.uuid).await?;
            let (spec_clone, _guard) = SpecOperations::start_update(
                registry,
                &replica_spec,
                &status,
                ReplicaOperation::Share(request.protocol),
                mode,
            )
            .await?;

            let result = node.share_replica(request).await;
            SpecOperations::complete_update(registry, result, replica_spec, spec_clone).await
        } else {
            node.share_replica(request).await
        }
    }
    pub(crate) async fn unshare_replica(
        &self,
        registry: &Registry,
        request: &UnshareReplica,
        mode: OperationMode,
    ) -> Result<String, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await?;

        if let Some(replica_spec) = self.get_replica(&request.uuid) {
            let status = registry.get_replica(&request.uuid).await?;
            let (spec_clone, _guard) = SpecOperations::start_update(
                registry,
                &replica_spec,
                &status,
                ReplicaOperation::Unshare,
                mode,
            )
            .await?;

            let result = node.unshare_replica(request).await;
            SpecOperations::complete_update(registry, result, replica_spec, spec_clone).await
        } else {
            node.unshare_replica(request).await
        }
    }

    /// Get or Create the protected ReplicaSpec for the given request
    fn get_or_create_replica(&self, request: &CreateReplica) -> Arc<Mutex<ReplicaSpec>> {
        let mut specs = self.write();
        if let Some(replica) = specs.replicas.get(&request.uuid) {
            replica.clone()
        } else {
            specs.replicas.insert(ReplicaSpec::from(request))
        }
    }
    /// Get a protected ReplicaSpec for the given replica `id`, if it exists
    pub(crate) fn get_replica(&self, id: &ReplicaId) -> Option<Arc<Mutex<ReplicaSpec>>> {
        let specs = self.read();
        specs.replicas.get(id).cloned()
    }

    /// Get or Create the protected PoolSpec for the given request
    fn get_or_create_pool(&self, request: &CreatePool) -> Arc<Mutex<PoolSpec>> {
        let mut specs = self.write();
        if let Some(pool) = specs.pools.get(&request.id) {
            pool.clone()
        } else {
            specs.pools.insert(PoolSpec::from(request))
        }
    }
    /// Get a protected PoolSpec for the given pool `id`, if it exists
    pub(crate) fn get_locked_pool(&self, id: &PoolId) -> Option<Arc<Mutex<PoolSpec>>> {
        let specs = self.read();
        specs.pools.get(id).cloned()
    }
    /// Get a PoolSpec for the given pool `id`, if it exists
    pub(crate) fn get_pool(&self, id: &PoolId) -> Result<PoolSpec, SvcError> {
        let specs = self.read();
        specs
            .pools
            .get(id)
            .map(|p| p.lock().clone())
            .ok_or(PoolNotFound {
                pool_id: id.to_owned(),
            })
    }
    /// Get a vector of protected PoolSpec's
    pub(crate) fn get_locked_pools(&self) -> Vec<Arc<Mutex<PoolSpec>>> {
        let specs = self.read();
        specs.pools.to_vec()
    }
    /// Get a vector of PoolSpec's
    pub(crate) fn get_pools(&self) -> Vec<PoolSpec> {
        let pools = self.get_locked_pools();
        pools.into_iter().map(|p| p.lock().clone()).collect()
    }
    /// Check if the given pool `id` has any replicas
    fn pool_has_replicas(&self, id: &PoolId) -> bool {
        let specs = self.read();
        !specs.get_pool_replicas(id).is_empty()
    }
    /// Remove the replica `id` from the spec list
    fn remove_replica(&self, id: &ReplicaId) {
        let mut specs = self.write();
        specs.replicas.remove(id);
    }
    /// Remove the Pool `id` from the spec list
    fn remove_pool(&self, id: &PoolId) {
        let mut specs = self.write();
        specs.pools.remove(id);
    }

    /// Get a vector of protected ReplicaSpec's
    pub(crate) fn get_replicas(&self) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        let specs = self.read();
        specs.replicas.to_vec()
    }

    /// Get a vector of protected ReplicaSpec's
    pub(crate) fn get_cloned_replicas(&self) -> Vec<ReplicaSpec> {
        let specs = self.read();
        specs
            .replicas
            .to_vec()
            .into_iter()
            .map(|r| r.lock().clone())
            .collect::<Vec<_>>()
    }

    /// Worker that reconciles dirty PoolSpec's with the persistent store.
    /// This is useful when pool operations are performed but we fail to
    /// update the spec with the persistent store.
    pub(crate) async fn reconcile_dirty_pools(&self, registry: &Registry) -> bool {
        let mut pending_ops = false;

        let pools = self.get_locked_pools();
        for pool in pools {
            if !SpecOperations::handle_incomplete_ops(&pool, registry).await {
                // Not all pending operations could be handled.
                pending_ops = true;
            }
        }
        pending_ops
    }

    /// Worker that reconciles dirty ReplicaSpec's with the persistent store.
    /// This is useful when replica operations are performed but we fail to
    /// update the spec with the persistent store.
    pub(crate) async fn reconcile_dirty_replicas(&self, registry: &Registry) -> bool {
        let mut pending_ops = false;

        let replicas = self.get_replicas();
        for replica in replicas {
            if !SpecOperations::handle_incomplete_ops(&replica, registry).await {
                // Not all pending operations could be handled.
                pending_ops = true;
            }
        }
        pending_ops
    }
}
