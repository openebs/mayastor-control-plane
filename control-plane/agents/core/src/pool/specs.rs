use std::{ops::Deref, sync::Arc};

use snafu::OptionExt;
use tokio::sync::Mutex;

use common::errors::{NodeNotFound, SvcError};
use mbus_api::{
    v0::{
        CreatePool,
        CreateReplica,
        DestroyPool,
        DestroyReplica,
        Pool,
        PoolId,
        PoolState,
        Protocol,
        Replica,
        ReplicaId,
        ReplicaState,
        ShareReplica,
        UnshareReplica,
    },
    ResourceKind,
};
use store::{
    store::{ObjectKey, Store, StoreError},
    types::v0::{
        pool::{PoolSpec, PoolSpecKey, PoolSpecState},
        replica::{ReplicaSpec, ReplicaSpecKey, ReplicaSpecState},
    },
};

use crate::{
    core::{
        specs::{ResourceSpecs, ResourceSpecsLocked},
        wrapper::ClientOps,
    },
    registry::Registry,
};

impl ResourceSpecs {
    fn get_replica(&self, id: &ReplicaId) -> Option<Arc<Mutex<ReplicaSpec>>> {
        self.replicas.get(id).cloned()
    }
    fn add_replica(&mut self, replica: ReplicaSpec) -> Arc<Mutex<ReplicaSpec>> {
        let spec = Arc::new(Mutex::new(replica.clone()));
        self.replicas.insert(replica.uuid, spec.clone());
        spec
    }
    async fn get_pool_replicas(
        &self,
        id: &PoolId,
    ) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        let mut replicas = vec![];
        for replica in self.replicas.values() {
            if id == &replica.lock().await.pool {
                replicas.push(replica.clone())
            }
        }
        replicas
    }

    fn get_pool(&self, id: &PoolId) -> Option<Arc<Mutex<PoolSpec>>> {
        self.pools.get(id).cloned()
    }
    fn del_pool(&mut self, id: &PoolId) {
        let _ = self.pools.remove(id);
    }
}

impl ResourceSpecsLocked {
    /// Create Pool
    pub(crate) async fn create_pool(
        &self,
        registry: &Registry,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await.context(
            NodeNotFound {
                node_id: request.node.clone(),
            },
        )?;
        let pool_spec = {
            let mut specs = self.write().await;

            if let Some(spec) = specs.get_pool(&request.id) {
                {
                    let mut pool_spec = spec.lock().await;
                    if pool_spec.updating {
                        // it's already being created
                        return Err(SvcError::Conflict {});
                    } else if pool_spec.state.creating() {
                        // this might be a retry, check if the params are the
                        // same if so, let's retry!
                        if pool_spec.ne(request) {
                            // if not then we can't proceed, so signal a
                            // conflict
                            return Err(SvcError::Conflict {});
                        }
                    } else {
                        return Err(SvcError::AlreadyExists {
                            kind: ResourceKind::Pool,
                            id: request.id.to_string(),
                        });
                    }
                    pool_spec.updating = true;
                }
                spec
            } else {
                let spec = PoolSpec::from(request);
                // write the spec to the persistent store
                {
                    let mut store = registry.store.lock().await;
                    store.put_obj(&spec).await?;
                }
                // add spec to the internal spec registry
                let spec = Arc::new(Mutex::new(spec));
                specs.pools.insert(request.id.clone(), spec.clone());
                spec
            }
        };

        let result = node.create_pool(request).await;
        let mut pool_spec = pool_spec.lock().await;
        pool_spec.updating = false;
        if result.is_ok() {
            let mut pool = pool_spec.clone();
            pool.state = PoolSpecState::Created(PoolState::Online);
            let mut store = registry.store.lock().await;
            store.put_obj(&pool).await?;
            pool_spec.state = PoolSpecState::Created(PoolState::Online);
        } else {
            drop(pool_spec);
            self.del_pool(&request.id).await;
            let mut store = registry.store.lock().await;
            let _ =
                store.delete_kv(&PoolSpecKey::from(&request.id).key()).await;
        }

        result
    }

    pub(crate) async fn destroy_pool(
        &self,
        registry: &Registry,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        // what if the node is never coming back?
        // do we need a way to forcefully "delete" things?
        let node = registry.get_node_wrapper(&request.node).await.context(
            NodeNotFound {
                node_id: request.node.clone(),
            },
        )?;

        let pool_spec = self.get_pool(&request.id).await;
        if let Some(pool_spec) = &pool_spec {
            let mut pool_spec = pool_spec.lock().await;
            if pool_spec.updating {
                return Err(SvcError::Conflict {});
            } else if pool_spec.state.deleted() {
                return Ok(());
            }
            pool_spec.updating = true;
        }

        let pool_in_use = self.pool_has_replicas(&request.id).await;
        if let Some(pool_spec) = &pool_spec {
            let mut pool_spec = pool_spec.lock().await;
            if pool_in_use {
                pool_spec.updating = false;
                // pool is currently in use so we shouldn't delete it
                return Err(SvcError::InUse {
                    kind: ResourceKind::Pool,
                    id: request.id.to_string(),
                });
            }
            if !pool_spec.state.deleting() {
                pool_spec.state = PoolSpecState::Deleting;
                // write it to the store
                let mut store = registry.store.lock().await;
                store.put_obj(pool_spec.deref()).await?;
            }
        }

        if let Some(pool_spec) = pool_spec {
            let mut pool_spec = pool_spec.lock().await;
            let result = node.destroy_pool(&request).await;
            {
                // remove the spec from the persistent store
                // if it fails, then fail the request and let the op retry
                let mut store = registry.store.lock().await;
                if let Err(error) =
                    store.delete_kv(&PoolSpecKey::from(&request.id).key()).await
                {
                    if !matches!(error, StoreError::MissingEntry { .. }) {
                        return Err(error.into());
                    }
                }
            }
            pool_spec.updating = false;
            pool_spec.state = PoolSpecState::Deleted;
            drop(pool_spec);
            // now remove the spec from our list
            let mut spec = self.write().await;
            spec.del_pool(&request.id);
            result
        } else {
            node.destroy_pool(&request).await
        }
    }

    pub(crate) async fn create_replica(
        &self,
        registry: &Registry,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await.context(
            NodeNotFound {
                node_id: request.node.clone(),
            },
        )?;

        let replica_spec = {
            let mut specs = self.write().await;

            if let Some(spec) = specs.get_replica(&request.uuid) {
                {
                    let mut replica_spec = spec.lock().await;
                    if replica_spec.updating {
                        // already being created
                        return Err(SvcError::Conflict {});
                    } else if replica_spec.state.creating() {
                        // this might be a retry, check if the params are the
                        // same if so, let's retry!
                        if replica_spec.ne(request) {
                            // if not then we can't proceed, so signal a
                            // conflict
                            return Err(SvcError::Conflict {});
                        }
                    } else {
                        return Err(SvcError::AlreadyExists {
                            kind: ResourceKind::Replica,
                            id: request.uuid.to_string(),
                        });
                    }
                    replica_spec.updating = true;
                }
                spec
            } else {
                let spec = ReplicaSpec::from(request);
                // write the spec to the persistent store
                {
                    let mut store = registry.store.lock().await;
                    store.put_obj(&spec).await?;
                }
                // add spec to the internal spec registry
                specs.add_replica(spec)
            }
        };

        let result = node.create_replica(request).await;
        let mut replica_spec = replica_spec.lock().await;
        replica_spec.updating = false;
        if result.is_ok() {
            let mut replica = replica_spec.clone();
            replica.state = ReplicaSpecState::Created(ReplicaState::Online);
            let mut store = registry.store.lock().await;
            store.put_obj(&replica).await?;
            replica_spec.state =
                ReplicaSpecState::Created(ReplicaState::Online);
        } else {
            // todo: check if this was a mayastor or a transport error
            drop(replica_spec);
            self.del_replica(&request.uuid).await;
            let mut store = registry.store.lock().await;
            let _ = store
                .delete_kv(&ReplicaSpecKey::from(&request.uuid).key())
                .await;
        }

        result
    }

    pub(crate) async fn destroy_replica(
        &self,
        registry: &Registry,
        request: &DestroyReplica,
        force: bool,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await.context(
            NodeNotFound {
                node_id: request.node.clone(),
            },
        )?;

        let replica = self.get_replica(&request.uuid).await;
        if let Some(replica) = &replica {
            let mut replica = replica.lock().await;
            let destroy_replica = force || !replica.owners.is_owned();

            if !destroy_replica {
                return Err(SvcError::InUse {
                    kind: ResourceKind::Replica,
                    id: request.uuid.to_string(),
                });
            } else if replica.updating {
                return Err(SvcError::Conflict {});
            } else if replica.state.deleted() {
                return Ok(());
            }

            if !replica.state.deleting() {
                replica.state = ReplicaSpecState::Deleting;
                // write it to the store
                let mut store = registry.store.lock().await;
                store.put_obj(replica.deref()).await?;
            }
            replica.updating = true;
        }

        if let Some(replica) = replica {
            let result = node.destroy_replica(request).await;
            match &result {
                Ok(_) => {
                    let mut replica = replica.lock().await;
                    replica.updating = false;
                    {
                        // remove the spec from the persistent store
                        // if it fails, then fail the request and let the op
                        // retry
                        let mut store = registry.store.lock().await;
                        if let Err(error) = store
                            .delete_kv(
                                &ReplicaSpecKey::from(&request.uuid).key(),
                            )
                            .await
                        {
                            if !matches!(error, StoreError::MissingEntry { .. })
                            {
                                return Err(error.into());
                            }
                        }
                    }
                    replica.state = ReplicaSpecState::Deleted;
                    drop(replica);
                    // now remove the spec from our list
                    self.del_replica(&request.uuid).await;
                }
                Err(_error) => {
                    let mut replica = replica.lock().await;
                    replica.updating = false;
                }
            }
            result
        } else {
            node.destroy_replica(&request).await
        }
    }
    pub(super) async fn share_replica(
        &self,
        registry: &Registry,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        let node = registry.get_node_wrapper(&request.node).await.context(
            NodeNotFound {
                node_id: request.node.clone(),
            },
        )?;

        if let Some(replica_spec) = self.get_replica(&request.uuid).await {
            let mut spec = replica_spec.lock().await;
            if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::ReplicaNotFound {
                    replica_id: request.uuid.clone(),
                });
            } else if spec.share != Protocol::Off {
                return Err(SvcError::AlreadyShared {
                    kind: ResourceKind::Replica,
                    id: request.uuid.to_string(),
                    share: spec.share.to_string(),
                });
            }

            spec.updating = true;
            let mut spec_clone = spec.clone();
            drop(spec);

            match node.share_replica(request).await {
                Ok(share) => {
                    spec_clone.share = request.protocol.into();
                    let result = {
                        let mut store = registry.store.lock().await;
                        store.put_obj(&spec_clone).await
                    };
                    if let Err(error) = result {
                        let _ =
                            node.unshare_replica(&request.clone().into()).await;
                        let mut spec = replica_spec.lock().await;
                        spec.updating = false;
                        return Err(error.into());
                    }
                    let mut spec = replica_spec.lock().await;
                    spec.share = request.protocol.into();
                    spec.updating = false;
                    Ok(share)
                }
                Err(error) => {
                    let mut spec = replica_spec.lock().await;
                    spec.updating = false;
                    Err(error)
                }
            }
        } else {
            node.share_replica(request).await
        }
    }
    pub(super) async fn unshare_replica(
        &self,
        registry: &Registry,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        let node = registry.get_node_wrapper(&request.node).await.context(
            NodeNotFound {
                node_id: request.node.clone(),
            },
        )?;

        if let Some(replica_spec) = self.get_replica(&request.uuid).await {
            let mut spec = replica_spec.lock().await;
            if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::ReplicaNotFound {
                    replica_id: request.uuid.clone(),
                });
            } else if spec.share == Protocol::Off {
                return Err(SvcError::NotShared {
                    kind: ResourceKind::Replica,
                    id: request.uuid.to_string(),
                });
            }

            spec.updating = true;
            let mut spec_clone = spec.clone();
            drop(spec);

            match node.unshare_replica(request).await {
                Ok(_) => {
                    spec_clone.share = Protocol::Off;
                    let result = {
                        let mut store = registry.store.lock().await;
                        store.put_obj(&spec_clone).await
                    };
                    if let Err(error) = result {
                        let _ =
                            node.share_replica(&request.clone().into()).await;
                        let mut spec = replica_spec.lock().await;
                        spec.updating = false;
                        return Err(error.into());
                    }
                    let mut spec = replica_spec.lock().await;
                    spec.share = Protocol::Off;
                    spec.updating = false;
                    Ok(())
                }
                Err(error) => {
                    let mut spec = replica_spec.lock().await;
                    spec.updating = false;
                    Err(error)
                }
            }
        } else {
            node.unshare_replica(request).await
        }
    }

    async fn get_replica(
        &self,
        id: &ReplicaId,
    ) -> Option<Arc<Mutex<ReplicaSpec>>> {
        let specs = self.read().await;
        specs.replicas.get(id).cloned()
    }
    async fn get_pool(&self, id: &PoolId) -> Option<Arc<Mutex<PoolSpec>>> {
        let specs = self.read().await;
        specs.pools.get(id).cloned()
    }
    async fn pool_has_replicas(&self, id: &PoolId) -> bool {
        let specs = self.read().await;
        !specs.get_pool_replicas(id).await.is_empty()
    }
    async fn del_replica(&self, id: &ReplicaId) {
        let mut specs = self.write().await;
        specs.replicas.remove(id);
    }
    async fn del_pool(&self, id: &PoolId) {
        let mut specs = self.write().await;
        specs.pools.remove(id);
    }
}
