use std::{convert::From, ops::Deref, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    core::{
        specs::{ResourceSpecs, ResourceSpecsLocked},
        wrapper::PoolWrapper,
    },
    registry::Registry,
};
use common::{
    errors,
    errors::{NodeNotFound, NotEnough, SvcError},
};
use mbus_api::ResourceKind;
use snafu::OptionExt;
use types::v0::{
    message_bus::mbus::{
        ChildUri, CreateNexus, CreateReplica, CreateVolume, DestroyNexus, DestroyReplica,
        DestroyVolume, Nexus, NexusId, NodeId, PoolState, Protocol, PublishVolume, ReplicaId,
        ReplicaOwners, ShareNexus, ShareVolume, UnpublishVolume, UnshareNexus, UnshareVolume,
        Volume, VolumeId, VolumeState,
    },
    store::{
        definitions::{ObjectKey, Store, StoreError},
        nexus::NexusSpec,
        replica::ReplicaSpec,
        volume::{VolumeOperation, VolumeSpec, VolumeSpecKey, VolumeSpecState},
        SpecTransaction,
    },
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
    /// Return a new blank VolumeSpec for the given request which has been committed to the
    /// persistent store or returns an existing one if the create operation has not
    /// yet succeeded but is able to be retried.
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
            registry.store_obj(&volume_spec).await?;
            // add spec to the internal spec registry
            let spec = Arc::new(Mutex::new(volume_spec));
            self.volumes.insert(request.uuid.clone(), spec.clone());
            spec
        };
        Ok(volume)
    }

    /// Gets all VolumeSpec's
    pub(crate) async fn get_volumes(&self) -> Vec<VolumeSpec> {
        let mut vector = vec![];
        for object in self.volumes.values() {
            let object = object.lock().await;
            vector.push(object.clone());
        }
        vector
    }
}
impl ResourceSpecsLocked {
    /// Get the protected VolumeSpec for the given volume `id`, if any exists
    pub(crate) async fn get_volume(&self, id: &VolumeId) -> Option<Arc<Mutex<VolumeSpec>>> {
        let specs = self.read().await;
        specs.volumes.get(id).cloned()
    }

    /// Gets all VolumeSpec's
    pub(crate) async fn get_volumes(&self) -> Vec<VolumeSpec> {
        let specs = self.read().await;
        specs.get_volumes().await
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
        let create_replicas = get_node_replicas(registry, request).await?;
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

        let mut volume_spec = volume.lock().await;
        volume_spec.updating = false;
        volume_spec.state = VolumeSpecState::Created(VolumeState::Online);
        let mut store = registry.store.lock().await;
        store.put_obj(volume_spec.deref()).await?;

        Ok(Volume {
            uuid: request.uuid.clone(),
            size: request.size,
            state: VolumeState::Online,
            protocol: Protocol::Off,
            children: vec![],
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

    pub(crate) async fn share_volume(
        &self,
        registry: &Registry,
        request: &ShareVolume,
    ) -> Result<String, SvcError> {
        let volume_spec = self
            .get_volume(&request.uuid)
            .await
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let spec_clone = {
            let mut spec = volume_spec.lock().await;
            if spec.pending_op() {
                return Err(SvcError::StoreSave {
                    kind: ResourceKind::Volume,
                    id: request.uuid.to_string(),
                });
            } else if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::VolumeNotFound {
                    vol_id: request.uuid.to_string(),
                });
            } else {
                // validate the operation itself against the spec and status
                if spec.protocol != status.protocol {
                    return Err(SvcError::NotReady {
                        kind: ResourceKind::Volume,
                        id: request.uuid.to_string(),
                    });
                } else if request.protocol == spec.protocol || spec.protocol.shared() {
                    return Err(SvcError::AlreadyShared {
                        kind: ResourceKind::Volume,
                        id: spec.uuid.to_string(),
                        share: spec.protocol.to_string(),
                    });
                }
                if status.children.len() != 1 {
                    return Err(SvcError::NotReady {
                        kind: ResourceKind::Volume,
                        id: request.uuid.to_string(),
                    });
                }
            }

            spec.updating = true;
            spec.start_op(VolumeOperation::Share(request.protocol));
            spec.clone()
        };

        if let Err(error) = registry.store_obj(&spec_clone).await {
            let mut spec = volume_spec.lock().await;
            spec.updating = false;
            spec.clear_op();
            return Err(error);
        }

        // Share the first child nexus (no ANA)
        assert_eq!(status.children.len(), 1);
        let nexus = status.children.get(0).unwrap();
        let result = self
            .share_nexus(registry, &ShareNexus::from((nexus, None, request.protocol)))
            .await;

        Self::spec_complete_op(registry, result, volume_spec, spec_clone).await
    }

    pub(crate) async fn unshare_volume(
        &self,
        registry: &Registry,
        request: &UnshareVolume,
    ) -> Result<(), SvcError> {
        let volume_spec = self
            .get_volume(&request.uuid)
            .await
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let spec_clone = {
            let mut spec = volume_spec.lock().await;
            if spec.pending_op() {
                return Err(SvcError::StoreSave {
                    kind: ResourceKind::Volume,
                    id: request.uuid.to_string(),
                });
            } else if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::VolumeNotFound {
                    vol_id: request.uuid.to_string(),
                });
            } else {
                // validate the operation itself against the spec and status
                if spec.protocol != status.protocol {
                    return Err(SvcError::NotReady {
                        kind: ResourceKind::Volume,
                        id: request.uuid.to_string(),
                    });
                } else if !spec.protocol.shared() {
                    return Err(SvcError::NotShared {
                        kind: ResourceKind::Volume,
                        id: spec.uuid.to_string(),
                    });
                }
                if status.children.len() != 1 {
                    return Err(SvcError::NotReady {
                        kind: ResourceKind::Volume,
                        id: request.uuid.to_string(),
                    });
                }
            }

            spec.updating = true;
            spec.start_op(VolumeOperation::Unshare);
            spec.clone()
        };

        if let Err(error) = registry.store_obj(&spec_clone).await {
            let mut spec = volume_spec.lock().await;
            spec.updating = false;
            spec.clear_op();
            return Err(error);
        }

        // Unshare the first child nexus (no ANA)
        assert_eq!(status.children.len(), 1);
        let nexus = status.children.get(0).unwrap();
        let result = self
            .unshare_nexus(registry, &UnshareNexus::from(nexus))
            .await;

        Self::spec_complete_op(registry, result, volume_spec, spec_clone).await
    }

    pub(crate) async fn publish_volume(
        &self,
        registry: &Registry,
        request: &PublishVolume,
    ) -> Result<String, SvcError> {
        let spec = self
            .get_volume(&request.uuid)
            .await
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;
        let nexus_node = get_volume_target_node(registry, &status, request).await?;

        let spec_clone = {
            let mut spec = spec.lock().await;
            if spec.pending_op() {
                return Err(SvcError::StoreSave {
                    kind: ResourceKind::Volume,
                    id: request.uuid.to_string(),
                });
            } else if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::VolumeNotFound {
                    vol_id: request.uuid.to_string(),
                });
            } else {
                // validate the operation itself against the spec and status
                if spec.protocol != status.protocol {
                    return Err(SvcError::NotReady {
                        kind: ResourceKind::Volume,
                        id: request.uuid.to_string(),
                    });
                } else if (request.target_node.is_some() && spec.target_node.is_some())
                    || (request.share.is_some() && spec.protocol.shared())
                {
                    return Err(SvcError::VolumeAlreadyPublished {
                        vol_id: request.uuid.to_string(),
                        node: spec.target_node.clone().unwrap().to_string(),
                        protocol: spec.protocol.to_string(),
                    });
                }
            }

            spec.start_op(VolumeOperation::Publish((
                nexus_node.clone(),
                request.share,
            )));
            spec.clone()
        };

        // Log the tentative spec update against the persistent store
        if let Err(error) = registry.store_obj(&spec_clone).await {
            let mut spec = spec.lock().await;
            spec.clear_op();
            return Err(error);
        }

        // Create a Nexus on the requested or auto-selected node
        let result = self
            .volume_create_nexus(registry, &nexus_node, &spec_clone)
            .await;
        let nexus = Self::spec_step_op(registry, result, spec.clone(), spec_clone.clone()).await?;

        // Share the Nexus if it was requested
        let mut result = Ok(nexus.device_uri.clone());
        if let Some(share) = request.share {
            result = self
                .share_nexus(registry, &ShareNexus::from((&nexus, None, share)))
                .await;
        }
        Self::spec_complete_op(registry, result, spec, spec_clone).await
    }

    pub(crate) async fn unpublish_volume(
        &self,
        registry: &Registry,
        request: &UnpublishVolume,
    ) -> Result<(), SvcError> {
        let spec = self
            .get_volume(&request.uuid)
            .await
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let (nexus, spec_clone) = {
            let mut spec = spec.lock().await;
            if spec.pending_op() {
                return Err(SvcError::StoreSave {
                    kind: ResourceKind::Volume,
                    id: request.uuid.to_string(),
                });
            } else if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::VolumeNotFound {
                    vol_id: request.uuid.to_string(),
                });
            } else {
                // validate the operation itself against the spec and status
                if spec.protocol != status.protocol
                    || spec.target_node != status.target_node().flatten()
                {
                    return Err(SvcError::NotReady {
                        kind: ResourceKind::Volume,
                        id: request.uuid.to_string(),
                    });
                }
            }
            let nexus = get_volume_nexus(&status)?;

            spec.start_op(VolumeOperation::Unpublish);
            (nexus, spec.clone())
        };

        // Log the tentative spec update against the persistent store
        if let Err(error) = registry.store_obj(&spec_clone).await {
            let mut spec = spec.lock().await;
            spec.clear_op();
            return Err(error);
        }

        // Destroy the Nexus
        let result = self.destroy_nexus(registry, &nexus.into(), true).await;
        Self::spec_complete_op(registry, result, spec, spec_clone).await
    }

    async fn volume_create_nexus(
        &self,
        registry: &Registry,
        target_node: &NodeId,
        vol_spec: &VolumeSpec,
    ) -> Result<Nexus, SvcError> {
        // find all replica status
        let status_replicas = registry.get_replicas().await.unwrap();
        // find all replica specs for this volume
        let spec_replicas = self.get_volume_replicas(&vol_spec.uuid).await;

        let mut spec_status_pair = vec![];
        for status_replica in status_replicas.iter() {
            for locked_replica in spec_replicas.iter() {
                let mut spec_replica = locked_replica.lock().await;
                if spec_replica.uuid == status_replica.uuid {
                    // todo: also check the health from etcd
                    // and that we don't have multiple replicas on the same node?
                    if spec_replica.size >= vol_spec.size && spec_replica.managed {
                        spec_status_pair.push((locked_replica.clone(), status_replica.clone()));
                        break;
                    } else {
                        // this replica is no longer valid
                        // todo: do it now, or let the reconcile do it?
                        spec_replica.owners.disowned_by_volume();
                    }
                }
            }
        }
        let mut nexus_replicas = vec![];
        // now reduce the replicas
        // one replica per node, with the right share protocol
        for (spec, status) in spec_status_pair.iter() {
            if nexus_replicas.len() > vol_spec.num_replicas as usize {
                // we have enough replicas as per the volume spec
                break;
            }
            let (share, unshare) = {
                let spec = spec.lock().await;
                let local = &status.node == target_node;
                (
                    local && (spec.share.shared() | status.share.shared()),
                    !local && (!spec.share.shared() | !status.share.shared()),
                )
            };
            if share {
                // unshare the replica
                if let Ok(uri) = self.unshare_replica(registry, &status.into()).await {
                    nexus_replicas.push(ChildUri::from(uri));
                }
            } else if unshare {
                // share the replica
                if let Ok(uri) = self.share_replica(registry, &status.into()).await {
                    nexus_replicas.push(ChildUri::from(uri));
                }
            } else {
                nexus_replicas.push(ChildUri::from(&status.uri));
            }
        }

        // Create the nexus on the request.node
        self.create_nexus(
            registry,
            &CreateNexus {
                node: target_node.clone(),
                uuid: NexusId::new(),
                size: vol_spec.size,
                children: nexus_replicas,
                managed: true,
                owner: Some(vol_spec.uuid.clone()),
            },
        )
        .await
    }

    /// Delete volume by its `id`
    async fn del_volume(&self, id: &VolumeId) {
        let mut specs = self.write().await;
        specs.volumes.remove(id);
    }
}

fn get_volume_nexus(volume_status: &Volume) -> Result<Nexus, SvcError> {
    match volume_status.children.len() {
        0 => Err(SvcError::VolumeNotPublished {
            vol_id: volume_status.uuid.to_string(),
        }),
        1 => Ok(volume_status.children[0].clone()),
        _ => Err(SvcError::NotReady {
            kind: ResourceKind::Volume,
            id: volume_status.uuid.to_string(),
        }),
    }
}

async fn get_volume_target_node(
    registry: &Registry,
    status: &Volume,
    request: &PublishVolume,
) -> Result<NodeId, SvcError> {
    // We can't configure a new target_node if the volume is currently published
    if let Some(current_node) = status.children.get(0) {
        return Err(SvcError::VolumeAlreadyPublished {
            vol_id: status.uuid.to_string(),
            node: current_node.node.to_string(),
            protocol: current_node.share.to_string(),
        });
    }

    match request.target_node.as_ref() {
        None => {
            // auto select a node
            let nodes = registry.get_nodes_wrapper().await;
            for locked_node in nodes {
                let node = locked_node.lock().await;
                // todo: use other metrics in order to make the "best" choice
                if node.is_online() {
                    return Ok(node.id.clone());
                }
            }
            Err(SvcError::NoNodes {})
        }
        Some(node) => {
            // make sure the requested node is available
            // todo: check the max number of nexuses per node is respected
            let node = registry
                .get_node_wrapper(node)
                .await
                .context(NodeNotFound {
                    node_id: node.clone(),
                })?;
            let node = node.lock().await;
            if node.is_online() {
                Ok(node.id.clone())
            } else {
                Err(SvcError::NodeNotOnline {
                    node: node.id.clone(),
                })
            }
        }
    }
}
