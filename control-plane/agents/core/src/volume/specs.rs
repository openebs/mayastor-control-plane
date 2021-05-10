use std::{ops::Deref, sync::Arc};

use tokio::sync::Mutex;

use common::errors::{NotEnough, SvcError};
use mbus_api::{
    v0::{
        ChildUri, CreateNexus, CreateReplica, CreateVolume, DestroyNexus, DestroyReplica,
        DestroyVolume, NexusId, NodeId, PoolState, Protocol, ReplicaId, ReplicaOwners, Volume,
        VolumeId, VolumeState,
    },
    ResourceKind,
};
use store::{
    store::{ObjectKey, Store, StoreError},
    types::v0::{
        nexus::NexusSpec,
        replica::ReplicaSpec,
        volume::{VolumeSpec, VolumeSpecKey, VolumeSpecState},
    },
};

use crate::{
    core::{
        specs::{ResourceSpecs, ResourceSpecsLocked},
        wrapper::PoolWrapper,
    },
    registry::Registry,
};

impl ResourceSpecs {
    fn get_volume(&self, id: &VolumeId) -> Option<Arc<Mutex<VolumeSpec>>> {
        self.volumes.get(id).cloned()
    }
}

async fn get_node_pools(
    registry: &Registry,
    request: &CreateVolume,
) -> Result<Vec<Vec<PoolWrapper>>, SvcError> {
    let node_pools = registry.get_node_pools_wrapper().await?;

    let size = request.size;
    let replicas = request.replicas;
    let allowed_nodes = request.allowed_nodes();

    if !allowed_nodes.is_empty() && replicas > allowed_nodes.len() as u64 {
        // oops, how would this even work mr requester?
        return Err(SvcError::InvalidArguments {});
    }

    // filter pools according to the following criteria (any order):
    // 1. if allowed_nodes were specified then only pools from those nodes
    // can be used.
    // 2. pools should have enough free space for the
    // volume (do we need to take into account metadata?)
    // 3. ideally use only healthy(online) pools with degraded pools as a
    // fallback
    let mut node_pools_sorted = vec![];
    for pools in node_pools {
        let mut pools = pools
            .iter()
            .filter(|&p| {
                // required nodes, if any
                allowed_nodes.is_empty() || allowed_nodes.contains(&p.node)
            })
            .filter(|&p| {
                // enough free space
                p.free_space() >= size
            })
            .filter(|&p| {
                // but preferably (the sort will sort this out for us)
                p.state != PoolState::Faulted && p.state != PoolState::Unknown
            })
            .cloned()
            .collect::<Vec<_>>();

        // sort pools from least to most suitable
        // state, then number of replicas and then free space
        pools.sort();

        node_pools_sorted.push(pools);
    }

    // we could not satisfy the request, no point in continuing any further
    if replicas > node_pools_sorted.len() as u64 {
        return Err(NotEnough::OfPools {
            have: node_pools_sorted.len() as u64,
            need: replicas,
        }
        .into());
    }
    if replicas == 0 {
        // not valid, unless we want to create volumes in a failed state...
        return Err(SvcError::InvalidArguments {});
    }

    Ok(node_pools_sorted)
}

async fn get_node_replicas(
    registry: &Registry,
    request: &CreateVolume,
) -> Result<Vec<Vec<CreateReplica>>, SvcError> {
    let pools = get_node_pools(registry, request).await?;
    let node_replicas = pools
        .iter()
        .map(|p| {
            p.iter()
                .map(|p| CreateReplica {
                    node: p.node.clone(),
                    uuid: ReplicaId::new(),
                    pool: p.id.clone(),
                    size: request.size,
                    thin: false,
                    share: Protocol::Nvmf,
                    managed: true,
                    owners: ReplicaOwners::new(&request.uuid),
                })
                .collect()
        })
        .collect::<Vec<_>>();
    if node_replicas.len() < request.replicas as usize {
        Err(NotEnough::OfReplicas {
            have: node_replicas.len() as u64,
            need: request.replicas,
        }
        .into())
    } else {
        Ok(node_replicas)
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Get a list of all protected VolumeSpec's
    async fn create_volume_spec(
        &mut self,
        registry: &Registry,
        request: &CreateVolume,
    ) -> Result<Arc<Mutex<VolumeSpec>>, SvcError> {
        let volume = if let Some(volume) = self.get_volume(&request.uuid) {
            let mut volume_spec = volume.lock().await;
            if volume_spec.updating {
                // already being created
                return Err(SvcError::Conflict {});
            } else if volume_spec.state.creating() {
                // this might be a retry, check if the params are the
                // same and if so, let's retry!
                if volume_spec.ne(request) {
                    // if not then we can't proceed
                    return Err(SvcError::Conflict {});
                }
            } else {
                return Err(SvcError::AlreadyExists {
                    kind: ResourceKind::Volume,
                    id: request.uuid.to_string(),
                });
            }

            volume_spec.updating = true;
            drop(volume_spec);
            volume
        } else {
            let volume_spec = VolumeSpec::from(request);
            // write the spec to the persistent store
            {
                let mut store = registry.store.lock().await;
                store.put_obj(&volume_spec).await?;
            }
            // add spec to the internal spec registry
            let spec = Arc::new(Mutex::new(volume_spec));
            self.volumes.insert(request.uuid.clone(), spec.clone());
            spec
        };
        Ok(volume)
    }
}
impl ResourceSpecsLocked {
    /// Get the protected VolumeSpec for the given volume `id`, if any exists
    async fn get_volume(&self, id: &VolumeId) -> Option<Arc<Mutex<VolumeSpec>>> {
        let specs = self.read().await;
        specs.volumes.get(id).cloned()
    }

    /// Get a list of protected ReplicaSpec's for the given `id`
    /// todo: we could also get the replicas from the volume nexuses?
    async fn get_volume_replicas(&self, id: &VolumeId) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        let mut replicas = vec![];
        let specs = self.read().await;
        for replica in specs.replicas.values() {
            let spec = replica.lock().await;
            if spec.owners.owned_by(id) {
                replicas.push(replica.clone());
            }
        }
        replicas
    }
    /// Get the `NodeId` where `replica` lives
    async fn get_replica_node(registry: &Registry, replica: &ReplicaSpec) -> Option<NodeId> {
        let pools = registry.get_pools_inner().await.unwrap();
        pools.iter().find_map(|p| {
            if p.id == replica.pool {
                Some(p.node.clone())
            } else {
                None
            }
        })
    }
    /// Get a list of protected NexusSpecs's for the given volume `id`
    async fn get_volume_nexuses(&self, id: &VolumeId) -> Vec<Arc<Mutex<NexusSpec>>> {
        let mut nexuses = vec![];
        let specs = self.read().await;
        for nexus in specs.nexuses.values() {
            let spec = nexus.lock().await;
            if spec.owner.as_ref() == Some(id) {
                nexuses.push(nexus.clone());
            }
        }
        nexuses
    }

    fn destroy_replica_request(spec: ReplicaSpec, node: &NodeId) -> DestroyReplica {
        DestroyReplica {
            node: node.clone(),
            pool: spec.pool,
            uuid: spec.uuid,
        }
    }

    pub(crate) async fn create_volume(
        &self,
        registry: &Registry,
        request: &CreateVolume,
    ) -> Result<Volume, SvcError> {
        // hold the specs lock while we determine the nodes/pools/replicas
        let mut specs = self.write().await;
        // todo: pick nodes and pools using the Node&Pool Topology
        let create_replicas = get_node_replicas(&registry, request).await?;
        // create the volume spec
        let volume = specs.create_volume_spec(registry, request).await?;

        // ok the selection of potential replicas has been made, now we can let
        // go of the specs and allow others to proceed
        drop(specs);

        let mut replicas = vec![];
        for node_replica in &create_replicas {
            if replicas.len() >= request.replicas as usize {
                break;
            }
            for pool_replica in node_replica {
                let replica = if replicas.is_empty() {
                    let mut replica = pool_replica.clone();
                    // the local replica needs to be connected via "bdev:///"
                    replica.share = Protocol::Off;
                    replica
                } else {
                    pool_replica.clone()
                };
                match self.create_replica(registry, &replica).await {
                    Ok(replica) => {
                        replicas.push(replica);
                        // one replica per node, though this may change when the
                        // topology lands
                        break;
                    }
                    Err(error) => {
                        tracing::error!(
                            "Failed to create replica {:?} for volume {}, error: {}",
                            replica,
                            request.uuid,
                            error
                        );
                        // continue trying...
                    }
                };
            }
        }
        // we can't fulfil the required replication factor, so let the caller
        // decide what to do next
        if replicas.len() < request.replicas as usize {
            {
                let mut volume_spec = volume.lock().await;
                volume_spec.state = VolumeSpecState::Deleting;
            }
            for replica in &replicas {
                if let Err(error) = self
                    .destroy_replica(registry, &replica.clone().into(), true)
                    .await
                {
                    tracing::error!(
                        "Failed to delete replica {:?} for volume {}, error: {}",
                        replica,
                        request.uuid,
                        error
                    );
                }
            }
            let mut specs = self.write().await;
            specs.volumes.remove(&request.uuid);
            let mut volume_spec = volume.lock().await;
            volume_spec.updating = false;
            volume_spec.state = VolumeSpecState::Deleted;
            return Err(NotEnough::OfReplicas {
                have: replicas.len() as u64,
                need: request.replicas,
            }
            .into());
        }

        // todo: we won't even need to create a nexus until it's published
        let nexus = match self
            .create_nexus(
                registry,
                &CreateNexus {
                    node: replicas[0].node.clone(),
                    uuid: NexusId::new(),
                    size: request.size,
                    children: replicas.iter().map(|r| ChildUri::from(&r.uri)).collect(),
                    managed: true,
                    owner: Some(request.uuid.clone()),
                },
            )
            .await
        {
            Ok(nexus) => nexus,
            Err(error) => {
                let mut volume_spec = volume.lock().await;
                volume_spec.state = VolumeSpecState::Deleting;
                drop(volume_spec);
                for replica in &replicas {
                    if let Err(error) = self
                        .destroy_replica(registry, &replica.clone().into(), true)
                        .await
                    {
                        tracing::error!(
                            "Failed to delete replica {:?} for volume {}, error: {}",
                            replica,
                            request.uuid,
                            error
                        );
                    }
                }
                let mut specs = self.write().await;
                specs.volumes.remove(&request.uuid);
                let mut volume_spec = volume.lock().await;
                volume_spec.updating = false;
                volume_spec.state = VolumeSpecState::Deleted;
                // todo: how to determine if this was a mayastor error or a
                // transport error? If it was a transport error
                // it's possible that the nexus has been created successfully
                // or is still being created.
                // Note: It's still safe to recreate the nexus somewhere else if
                // use a different set of replicas
                return Err(error);
            }
        };

        let mut volume_spec = volume.lock().await;
        volume_spec.updating = false;
        let mut store = registry.store.lock().await;
        store.put_obj(volume_spec.deref()).await?;
        volume_spec.state = VolumeSpecState::Created(VolumeState::Online);

        // todo: the volume should live in the store, and maybe in the registry
        // as well
        Ok(Volume {
            uuid: request.uuid.clone(),
            size: request.size,
            state: VolumeState::Online,
            children: vec![nexus],
        })
    }

    pub(crate) async fn destroy_volume(
        &self,
        registry: &Registry,
        request: &DestroyVolume,
    ) -> Result<(), SvcError> {
        let volume = self.get_volume(&request.uuid).await;
        if let Some(volume) = &volume {
            let mut volume = volume.lock().await;
            if volume.updating {
                return Err(SvcError::Conflict {});
            } else if volume.state.deleted() {
                return Ok(());
            }
            if !volume.state.deleting() {
                volume.state = VolumeSpecState::Deleting;
                // write it to the store
                let mut store = registry.store.lock().await;
                store.put_obj(volume.deref()).await?;
            }
            volume.updating = true;
        }
        let mut first_error = None;

        if let Some(volume) = volume {
            let nexuses = self.get_volume_nexuses(&request.uuid).await;
            for nexus in nexuses {
                let nexus = nexus.lock().await.deref().clone();
                if let Err(error) = self
                    .destroy_nexus(registry, &DestroyNexus::from(nexus), true)
                    .await
                {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
            let replicas = self.get_volume_replicas(&request.uuid).await;
            for replica in replicas {
                let spec = replica.lock().await.deref().clone();
                if let Some(node) = Self::get_replica_node(registry, &spec).await {
                    if let Err(error) = self
                        .destroy_replica(
                            registry,
                            &Self::destroy_replica_request(spec, &node),
                            true,
                        )
                        .await
                    {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                } else {
                    // the above is able to handle when a pool is moved to a
                    // different node but if a pool is
                    // unplugged, what do we do? Fake an error ReplicaNotFound?
                }
            }
            match first_error {
                None => {
                    let mut volume = volume.lock().await;
                    volume.updating = false;
                    {
                        let mut store = registry.store.lock().await;
                        if let Err(error) = store
                            .delete_kv(&VolumeSpecKey::from(&request.uuid).key())
                            .await
                        {
                            if !matches!(error, StoreError::MissingEntry { .. }) {
                                return Err(error.into());
                            }
                        }
                    }
                    volume.state = VolumeSpecState::Deleted;
                    drop(volume);
                    self.del_volume(&request.uuid).await;
                    Ok(())
                }
                Some(error) => {
                    let mut volume = volume.lock().await;
                    volume.updating = false;
                    Err(error)
                }
            }
        } else {
            Err(SvcError::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })
        }
    }
    /// Delete volume by its `id`
    async fn del_volume(&self, id: &VolumeId) {
        let mut specs = self.write().await;
        specs.volumes.remove(id);
    }
}
