use crate::controller::{
    operations::ResourceLifecycle,
    registry::Registry,
    specs::{
        GuardedOperationsHelper, OperationSequenceGuard, ResourceSpecs, ResourceSpecsLocked,
        SpecOperationsHelper,
    },
};
use common::errors::{SvcError, SvcError::PoolNotFound};
use common_lib::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            pool::{PoolOperation, PoolSpec},
            replica::{ReplicaOperation, ReplicaSpec},
            OperationGuardArc, ResourceMutex, SpecStatus, SpecTransaction,
        },
        transport::{
            CreatePool, CreateReplica, PoolId, PoolState, PoolStatus, Replica, ReplicaId,
            ReplicaOwners, ReplicaStatus,
        },
    },
};

#[async_trait::async_trait]
impl GuardedOperationsHelper for OperationGuardArc<PoolSpec> {
    type Create = CreatePool;
    type Owners = ();
    type Status = PoolStatus;
    type State = PoolState;
    type UpdateOp = ();
    type Inner = PoolSpec;

    fn validate_destroy(&self, registry: &Registry) -> Result<(), SvcError> {
        let id = self.lock().id.clone();
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

    fn remove_spec(&self, registry: &Registry) {
        let id = self.lock().id.clone();
        registry.specs().remove_pool(&id);
    }
}

#[async_trait::async_trait]
impl SpecOperationsHelper for PoolSpec {
    type Create = CreatePool;
    type Owners = ();
    type Status = PoolStatus;
    type State = PoolState;
    type UpdateOp = ();

    fn start_create_op(&mut self) {
        self.start_op(PoolOperation::Create);
    }
    fn start_destroy_op(&mut self) {
        self.start_op(PoolOperation::Destroy);
    }

    fn dirty(&self) -> bool {
        // The pool spec can be dirty if a pool create operation fails to complete because it cannot
        // write to etcd.
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Pool
    }
    fn uuid_str(&self) -> String {
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
impl GuardedOperationsHelper for OperationGuardArc<ReplicaSpec> {
    type Create = CreateReplica;
    type Owners = ReplicaOwners;
    type Status = ReplicaStatus;
    type State = Replica;
    type UpdateOp = ReplicaOperation;
    type Inner = ReplicaSpec;

    fn remove_spec(&self, registry: &Registry) {
        let uuid = self.lock().uuid.clone();
        registry.specs().remove_replica(&uuid);
    }
}

#[async_trait::async_trait]
impl SpecOperationsHelper for ReplicaSpec {
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
                    id: self.uuid_str(),
                    share: state.share.to_string(),
                })
            }
            ReplicaOperation::Share(_) => Ok(()),
            ReplicaOperation::Unshare if !self.share.shared() && !state.share.shared() => {
                Err(SvcError::NotShared {
                    kind: self.kind(),
                    id: self.uuid_str(),
                })
            }
            ReplicaOperation::Unshare => Ok(()),
            ReplicaOperation::OwnerUpdate(_) => Ok(()),
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
    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Replica
    }
    fn uuid_str(&self) -> String {
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
    fn get_pool_replicas(&self, id: &PoolId) -> Vec<ResourceMutex<ReplicaSpec>> {
        let mut replicas = vec![];
        for replica in self.replicas.to_vec() {
            let pool_id = replica.lock().pool.pool_name().clone();
            if id == &pool_id {
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
    /// Get the guarded ReplicaSpec for the given replica `id`, if any exists
    pub(crate) async fn replica_opt(
        &self,
        replica: &ReplicaId,
    ) -> Result<Option<OperationGuardArc<ReplicaSpec>>, SvcError> {
        Ok(match self.get_replica(replica) {
            None => None,
            Some(replica) => Some(replica.operation_guard_wait().await?),
        })
    }
    /// Get the guarded ReplicaSpec for the given replica `id`, if any exists
    pub(crate) async fn replica(
        &self,
        replica: &ReplicaId,
    ) -> Result<OperationGuardArc<ReplicaSpec>, SvcError> {
        match self.get_replica(replica) {
            None => Err(SvcError::ReplicaNotFound {
                replica_id: replica.clone(),
            }),
            Some(replica) => Ok(replica.operation_guard_wait().await?),
        }
    }

    pub(crate) async fn destroy_replica_spec(
        &self,
        registry: &Registry,
        replica_spec: &ReplicaSpec,
        destroy_by: ReplicaOwners,
    ) -> Result<(), SvcError> {
        match Self::get_replica_node(registry, replica_spec).await {
            // Should never happen, but just in case...
            None => Err(SvcError::Internal {
                details: "Failed to find the node where a replica lives".to_string(),
            }),
            Some(node) => {
                let mut replica = self.replica(&replica_spec.uuid).await?;
                replica
                    .destroy(
                        registry,
                        &Self::destroy_replica_request(replica_spec.clone(), destroy_by, &node),
                    )
                    .await
            }
        }
    }

    /// Get or Create the protected ReplicaSpec for the given request
    pub(crate) fn get_or_create_replica(
        &self,
        request: &CreateReplica,
    ) -> ResourceMutex<ReplicaSpec> {
        let mut specs = self.write();
        if let Some(replica) = specs.replicas.get(&request.uuid) {
            replica.clone()
        } else {
            specs.replicas.insert(ReplicaSpec::from(request))
        }
    }
    /// Get a protected ReplicaSpec for the given replica `id`, if it exists
    pub(crate) fn get_replica(&self, id: &ReplicaId) -> Option<ResourceMutex<ReplicaSpec>> {
        let specs = self.read();
        specs.replicas.get(id).cloned()
    }

    /// Get or Create the protected PoolSpec for the given request
    pub(crate) fn get_or_create_pool(&self, request: &CreatePool) -> ResourceMutex<PoolSpec> {
        let mut specs = self.write();
        if let Some(pool) = specs.pools.get(&request.id) {
            pool.clone()
        } else {
            specs.pools.insert(PoolSpec::from(request))
        }
    }
    /// Get a protected PoolSpec for the given pool `id`, if it exists
    pub(crate) fn get_locked_pool(&self, id: &PoolId) -> Option<ResourceMutex<PoolSpec>> {
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
    pub(crate) fn get_locked_pools(&self) -> Vec<ResourceMutex<PoolSpec>> {
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
    pub(crate) fn get_replicas(&self) -> Vec<ResourceMutex<ReplicaSpec>> {
        let specs = self.read();
        specs.replicas.to_vec()
    }

    /// Get a vector of ReplicaSpec's
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
            if let Ok(mut guard) = pool.operation_guard() {
                if !guard.handle_incomplete_ops(registry).await {
                    // Not all pending operations could be handled.
                    pending_ops = true;
                }
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
            if let Ok(mut guard) = replica.operation_guard() {
                if !guard.handle_incomplete_ops(registry).await {
                    // Not all pending operations could be handled.
                    pending_ops = true;
                }
            }
        }
        pending_ops
    }
}
