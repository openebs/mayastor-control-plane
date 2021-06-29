use snafu::OptionExt;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    core::{
        specs::{ResourceSpecs, ResourceSpecsLocked, SpecOperations},
        wrapper::ClientOps,
    },
    registry::Registry,
};
use common::errors::{NodeNotFound, SvcError};
use common_lib::{
    mbus_api::ResourceKind,
    types::v0::{
        message_bus::mbus::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, Pool, PoolId, PoolState,
            Replica, ReplicaId, ReplicaState, ShareReplica, UnshareReplica,
        },
        store::{
            pool::{PoolOperation, PoolSpec},
            replica::{ReplicaOperation, ReplicaSpec},
            SpecState, SpecTransaction,
        },
    },
};

#[async_trait::async_trait]
impl SpecOperations for PoolSpec {
    type Create = CreatePool;
    type State = PoolState;
    type Status = Pool;
    type UpdateOp = ();

    async fn validate_destroy(
        locked_spec: &Arc<Mutex<Self>>,
        registry: &Registry,
    ) -> Result<(), SvcError> {
        let id = locked_spec.lock().await.id.clone();
        let pool_in_use = registry.specs.pool_has_replicas(&id).await;
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
    async fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry) {
        let id = locked_spec.lock().await.id.clone();
        registry.specs.remove_pool(&id).await;
    }
    fn set_updating(&mut self, updating: bool) {
        self.updating = updating;
    }
    fn updating(&self) -> bool {
        self.updating
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
    fn state(&self) -> SpecState<Self::State> {
        self.state.clone()
    }
    fn set_state(&mut self, state: SpecState<Self::State>) {
        self.state = state;
    }
}

#[async_trait::async_trait]
impl SpecOperations for ReplicaSpec {
    type Create = CreateReplica;
    type State = ReplicaState;
    type Status = Replica;
    type UpdateOp = ReplicaOperation;

    fn start_update_op(
        &mut self,
        status: &Self::Status,
        op: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        match op {
            ReplicaOperation::Share(_) if status.share.shared() => Err(SvcError::AlreadyShared {
                kind: self.kind(),
                id: self.uuid(),
                share: status.share.to_string(),
            }),
            ReplicaOperation::Share(_) => Ok(()),
            ReplicaOperation::Unshare if !status.share.shared() => Err(SvcError::NotShared {
                kind: self.kind(),
                id: self.uuid(),
            }),
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
    async fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry) {
        let uuid = locked_spec.lock().await.uuid.clone();
        registry.specs.remove_replica(&uuid).await;
    }
    fn set_updating(&mut self, updating: bool) {
        self.updating = updating;
    }
    fn updating(&self) -> bool {
        self.updating
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
    fn state(&self) -> SpecState<Self::State> {
        self.state.clone()
    }
    fn set_state(&mut self, state: SpecState<Self::State>) {
        self.state = state;
    }
    fn owned(&self) -> bool {
        self.owners.is_owned()
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Gets list of protected ReplicaSpec's for a given pool `id`
    async fn get_pool_replicas(&self, id: &PoolId) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        let mut replicas = vec![];
        for replica in self.replicas.values() {
            if id == &replica.lock().await.pool {
                replicas.push(replica.clone())
            }
        }
        replicas
    }
    /// Gets all ReplicaSpec's
    pub(crate) async fn get_replicas(&self) -> Vec<ReplicaSpec> {
        let mut vector = vec![];
        for object in self.replicas.values() {
            let object = object.lock().await;
            vector.push(object.clone());
        }
        vector
    }
}

impl ResourceSpecsLocked {
    pub(crate) async fn create_pool(
        &self,
        registry: &Registry,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let pool_spec = self.get_or_create_pool(&request).await;
        SpecOperations::start_create(&pool_spec, registry, request).await?;

        let result = node.create_pool(request).await;
        SpecOperations::complete_create(result, &pool_spec, registry).await
    }

    pub(crate) async fn destroy_pool(
        &self,
        registry: &Registry,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        // what if the node is never coming back?
        // do we need a way to forcefully "delete" things?
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let pool_spec = self.get_pool(&request.id).await;
        if let Some(pool_spec) = &pool_spec {
            SpecOperations::start_destroy(&pool_spec, registry, false).await?;

            let result = node.destroy_pool(request).await;
            SpecOperations::complete_destroy(result, &pool_spec, registry).await
        } else {
            node.destroy_pool(request).await
        }
    }

    pub(crate) async fn create_replica(
        &self,
        registry: &Registry,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let replica_spec = self.get_or_create_replica(&request).await;
        SpecOperations::start_create(&replica_spec, registry, request).await?;

        let result = node.create_replica(request).await;
        SpecOperations::complete_create(result, &replica_spec, registry).await
    }

    pub(crate) async fn destroy_replica(
        &self,
        registry: &Registry,
        request: &DestroyReplica,
        delete_owned: bool,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let replica = self.get_replica(&request.uuid).await;
        if let Some(replica) = &replica {
            SpecOperations::start_destroy(&replica, registry, delete_owned).await?;

            let result = node.destroy_replica(request).await;
            SpecOperations::complete_destroy(result, &replica, registry).await
        } else {
            node.destroy_replica(request).await
        }
    }
    pub(crate) async fn share_replica(
        &self,
        registry: &Registry,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(replica_spec) = self.get_replica(&request.uuid).await {
            let status = registry.get_replica(&request.uuid).await?;
            let spec_clone = SpecOperations::start_update(
                registry,
                &replica_spec,
                &status,
                ReplicaOperation::Share(request.protocol),
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
    ) -> Result<String, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(replica_spec) = self.get_replica(&request.uuid).await {
            let status = registry.get_replica(&request.uuid).await?;
            let spec_clone = SpecOperations::start_update(
                registry,
                &replica_spec,
                &status,
                ReplicaOperation::Unshare,
            )
            .await?;

            let result = node.unshare_replica(request).await;
            SpecOperations::complete_update(registry, result, replica_spec, spec_clone).await
        } else {
            node.unshare_replica(request).await
        }
    }

    /// Get or Create the protected ReplicaSpec for the given request
    async fn get_or_create_replica(&self, request: &CreateReplica) -> Arc<Mutex<ReplicaSpec>> {
        let mut specs = self.write().await;
        if let Some(replica) = specs.replicas.get(&request.uuid) {
            replica.clone()
        } else {
            let spec = ReplicaSpec::from(request);
            let locked_spec = Arc::new(Mutex::new(spec));
            specs
                .replicas
                .insert(request.uuid.clone(), locked_spec.clone());
            locked_spec
        }
    }
    /// Get a protected ReplicaSpec for the given replica `id`, if it exists
    async fn get_replica(&self, id: &ReplicaId) -> Option<Arc<Mutex<ReplicaSpec>>> {
        let specs = self.read().await;
        specs.replicas.get(id).cloned()
    }

    /// Get or Create the protected PoolSpec for the given request
    async fn get_or_create_pool(&self, request: &CreatePool) -> Arc<Mutex<PoolSpec>> {
        let mut specs = self.write().await;
        if let Some(pool) = specs.pools.get(&request.id) {
            pool.clone()
        } else {
            let spec = PoolSpec::from(request);
            let locked_spec = Arc::new(Mutex::new(spec));
            specs.pools.insert(request.id.clone(), locked_spec.clone());
            locked_spec
        }
    }
    /// Get a protected PoolSpec for the given pool `id`, if it exists
    async fn get_pool(&self, id: &PoolId) -> Option<Arc<Mutex<PoolSpec>>> {
        let specs = self.read().await;
        specs.pools.get(id).cloned()
    }
    /// Check if the given pool `id` has any replicas
    async fn pool_has_replicas(&self, id: &PoolId) -> bool {
        let specs = self.read().await;
        !specs.get_pool_replicas(id).await.is_empty()
    }
    /// Remove the replica `id` from the spec list
    async fn remove_replica(&self, id: &ReplicaId) {
        let mut specs = self.write().await;
        specs.replicas.remove(id);
    }
    /// Remove the Pool `id` from the spec list
    async fn remove_pool(&self, id: &PoolId) {
        let mut specs = self.write().await;
        specs.pools.remove(id);
    }

    /// Get a vector of protected ReplicaSpec's
    pub(crate) async fn get_replicas(&self) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        let specs = self.read().await;
        specs.replicas.values().cloned().collect()
    }

    /// Worker that reconciles dirty ReplicaSpec's with the persistent store.
    /// This is useful when replica operations are performed but we fail to
    /// update the spec with the persistent store.
    pub(crate) async fn reconcile_dirty_replicas(&self, registry: &Registry) -> bool {
        if registry.store_online().await {
            let mut pending_count = 0;

            let replicas = self.get_replicas().await;
            for replica_spec in replicas {
                let mut replica = replica_spec.lock().await;
                if replica.updating || !replica.state.created() {
                    continue;
                }
                if let Some(op) = replica.operation.clone() {
                    let mut replica_clone = replica.clone();

                    let fail = !match op.result {
                        Some(true) => {
                            replica_clone.commit_op();
                            let result = registry.store_obj(&replica_clone).await;
                            if result.is_ok() {
                                replica.commit_op();
                            }
                            result.is_ok()
                        }
                        Some(false) => {
                            replica_clone.clear_op();
                            let result = registry.store_obj(&replica_clone).await;
                            if result.is_ok() {
                                replica.clear_op();
                            }
                            result.is_ok()
                        }
                        None => {
                            // we must have crashed... we could check the node to see what the
                            // current state is but for now assume failure
                            replica_clone.clear_op();
                            let result = registry.store_obj(&replica_clone).await;
                            if result.is_ok() {
                                replica.clear_op();
                            }
                            result.is_ok()
                        }
                    };
                    if fail {
                        pending_count += 1;
                    }
                }
            }
            pending_count > 0
        } else {
            true
        }
    }
}
