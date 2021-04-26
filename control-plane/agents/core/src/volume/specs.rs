use std::{ops::Deref, sync::Arc};

use snafu::OptionExt;
use tokio::sync::Mutex;

use common::errors::{NodeNotFound, NotEnough, SvcError};
use mbus_api::{
    v0::{
        AddNexusChild,
        Child,
        ChildUri,
        CreateNexus,
        CreateReplica,
        CreateVolume,
        DestroyNexus,
        DestroyReplica,
        DestroyVolume,
        Nexus,
        NexusId,
        NexusShareProtocol,
        NexusState,
        NodeId,
        PoolState,
        Protocol,
        RemoveNexusChild,
        ReplicaId,
        ReplicaOwners,
        ShareNexus,
        UnshareNexus,
        Volume,
        VolumeId,
        VolumeState,
    },
    ResourceKind,
};
use store::{
    store::{ObjectKey, Store, StoreError},
    types::v0::{
        nexus::{NexusSpec, NexusSpecKey, NexusSpecState},
        replica::ReplicaSpec,
        volume::{VolumeSpec, VolumeSpecKey, VolumeSpecState},
    },
};

use crate::{
    core::{
        specs::{ResourceSpecs, ResourceSpecsLocked},
        wrapper::{ClientOps, PoolWrapper},
    },
    registry::Registry,
};

impl ResourceSpecs {
    fn get_nexus(&self, id: &NexusId) -> Option<Arc<Mutex<NexusSpec>>> {
        self.nexuses.get(id).cloned()
    }
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
    let allowed_nodes = request.allowed_nodes.clone();

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

impl ResourceSpecs {
    pub(crate) async fn get_nexuses(&self) -> Vec<NexusSpec> {
        let mut vector = vec![];
        for object in self.nexuses.values() {
            let object = object.lock().await;
            vector.push(object.clone());
        }
        vector
    }
    pub(crate) async fn get_created_nexuses(&self) -> Vec<NexusSpec> {
        let mut nexuses = vec![];
        for nexus in self.nexuses.values() {
            let nexus = nexus.lock().await;
            if nexus.state.created() || nexus.state.deleting() {
                nexuses.push(nexus.clone());
            }
        }
        nexuses
    }

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
    pub(crate) async fn get_created_nexus_specs(&self) -> Vec<NexusSpec> {
        let specs = self.read().await;
        specs.get_created_nexuses().await
    }
    async fn get_nexus(&self, id: &NexusId) -> Option<Arc<Mutex<NexusSpec>>> {
        let specs = self.read().await;
        specs.nexuses.get(id).cloned()
    }
    async fn get_volume(&self, id: &VolumeId) -> Option<Arc<Mutex<VolumeSpec>>> {
        let specs = self.read().await;
        specs.volumes.get(id).cloned()
    }
    // we could also get the replicas from the volume nexuses
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
    // we could also tag the volume with the "latest" nexuses
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

    pub(super) async fn create_nexus(
        &self,
        registry: &Registry,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let nexus_spec = {
            let mut specs = self.write().await;
            if let Some(spec) = specs.get_nexus(&request.uuid) {
                {
                    let mut nexus_spec = spec.lock().await;
                    if nexus_spec.updating {
                        // already being created
                        return Err(SvcError::Conflict {});
                    } else if nexus_spec.state.creating() {
                        // this might be a retry, check if the params are the
                        // same and if so, let's retry!
                        if nexus_spec.ne(request) {
                            // if not then we can't proceed
                            return Err(SvcError::Conflict {});
                        }
                    } else {
                        return Err(SvcError::AlreadyExists {
                            kind: ResourceKind::Nexus,
                            id: request.uuid.to_string(),
                        });
                    }

                    nexus_spec.updating = true;
                }
                spec
            } else {
                let spec = NexusSpec::from(request);
                // write the spec to the persistent store
                {
                    let mut store = registry.store.lock().await;
                    store.put_obj(&spec).await?;
                }
                // add spec to the internal spec registry
                let spec = Arc::new(Mutex::new(spec));
                specs.nexuses.insert(request.uuid.clone(), spec.clone());
                spec
            }
        };

        let result = node.create_nexus(request).await;
        if result.is_ok() {
            let mut nexus_spec = nexus_spec.lock().await;
            nexus_spec.state = NexusSpecState::Created(NexusState::Online);
            nexus_spec.updating = false;
            let mut store = registry.store.lock().await;
            store.put_obj(nexus_spec.deref()).await?;
        }

        result
    }

    pub(super) async fn destroy_nexus(
        &self,
        registry: &Registry,
        request: &DestroyNexus,
        force: bool,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let nexus = self.get_nexus(&request.uuid).await;
        if let Some(nexus) = &nexus {
            let mut nexus = nexus.lock().await;
            let destroy_nexus = force || nexus.owner.is_none();

            if nexus.updating {
                return Err(SvcError::Conflict {});
            } else if nexus.state.deleted() {
                return Ok(());
            }
            if !destroy_nexus {
                return Err(SvcError::Conflict {});
            }
            if !nexus.state.deleting() {
                nexus.state = NexusSpecState::Deleting;
                // write it to the store
                let mut store = registry.store.lock().await;
                store.put_obj(nexus.deref()).await?;
            }
            nexus.updating = true;
        }

        if let Some(nexus) = nexus {
            let result = node.destroy_nexus(request).await;
            match &result {
                Ok(_) => {
                    let mut nexus = nexus.lock().await;
                    nexus.updating = false;
                    {
                        // remove the spec from the persistent store
                        // if it fails, then fail the request and let the op
                        // retry
                        let mut store = registry.store.lock().await;
                        if let Err(error) = store
                            .delete_kv(&NexusSpecKey::from(&request.uuid).key())
                            .await
                        {
                            if !matches!(error, StoreError::MissingEntry { .. }) {
                                return Err(error.into());
                            }
                        }
                    }
                    nexus.state = NexusSpecState::Deleted;
                    drop(nexus);
                    // now remove the spec from our list
                    self.del_nexus(&request.uuid).await;
                }
                Err(_error) => {
                    let mut nexus = nexus.lock().await;
                    nexus.updating = false;
                }
            }
            result
        } else {
            node.destroy_nexus(request).await
        }
    }

    pub(super) async fn share_nexus(
        &self,
        registry: &Registry,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.uuid).await {
            let mut spec = nexus_spec.lock().await;
            if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::NexusNotFound {
                    nexus_id: request.uuid.to_string(),
                });
            } else if spec.share != Protocol::Off {
                return Err(SvcError::AlreadyShared {
                    kind: ResourceKind::Nexus,
                    id: request.uuid.to_string(),
                    share: spec.share.to_string(),
                });
            }

            spec.updating = true;
            let mut spec_clone = spec.clone();
            drop(spec);

            match node.share_nexus(request).await {
                Ok(share) => {
                    spec_clone.share = request.protocol.into();
                    let result = {
                        let mut store = registry.store.lock().await;
                        store.put_obj(&spec_clone).await
                    };
                    if let Err(error) = result {
                        let _ = node.unshare_nexus(&request.clone().into()).await;
                        let mut spec = nexus_spec.lock().await;
                        spec.updating = false;
                        return Err(error.into());
                    }
                    let mut spec = nexus_spec.lock().await;
                    spec.share = request.protocol.into();
                    spec.updating = false;
                    Ok(share)
                }
                Err(error) => {
                    let mut spec = nexus_spec.lock().await;
                    spec.updating = false;
                    Err(error)
                }
            }
        } else {
            node.share_nexus(request).await
        }
    }
    pub(super) async fn unshare_nexus(
        &self,
        registry: &Registry,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let specs = self.read().await;
        if let Some(nexus_spec) = specs.get_nexus(&request.uuid) {
            let mut spec = nexus_spec.lock().await;
            if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::NexusNotFound {
                    nexus_id: request.uuid.to_string(),
                });
            } else if spec.share == Protocol::Off {
                return Err(SvcError::NotShared {
                    kind: ResourceKind::Nexus,
                    id: request.uuid.to_string(),
                });
            }

            spec.updating = true;
            let mut spec_clone = spec.clone();
            drop(spec);

            match node.unshare_nexus(request).await {
                Ok(_) => {
                    let previous_share = spec_clone.share;
                    spec_clone.share = Protocol::Off;
                    let result = {
                        let mut store = registry.store.lock().await;
                        store.put_obj(&spec_clone).await
                    };
                    if let Err(error) = result {
                        let share = ShareNexus {
                            node: request.node.clone(),
                            uuid: request.uuid.clone(),
                            key: None,
                            protocol: match previous_share {
                                Protocol::Off => unreachable!(),
                                Protocol::Nvmf => NexusShareProtocol::Nvmf,
                                Protocol::Iscsi => NexusShareProtocol::Iscsi,
                                Protocol::Nbd => unreachable!(),
                            },
                        };
                        let _ = node.share_nexus(&share).await;
                        let mut spec = nexus_spec.lock().await;
                        spec.updating = false;
                        return Err(error.into());
                    }
                    let mut spec = nexus_spec.lock().await;
                    spec.share = Protocol::Off;
                    spec.updating = false;
                    Ok(())
                }
                Err(error) => {
                    let mut spec = nexus_spec.lock().await;
                    spec.updating = false;
                    Err(error)
                }
            }
        } else {
            node.unshare_nexus(request).await
        }
    }

    pub(super) async fn add_nexus_child(
        &self,
        registry: &Registry,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.nexus).await {
            let mut spec = nexus_spec.lock().await;
            if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::NexusNotFound {
                    nexus_id: request.nexus.to_string(),
                });
            } else if spec.children.contains(&request.uri) {
                return Err(SvcError::ChildAlreadyExists {
                    nexus: request.nexus.to_string(),
                    child: request.uri.to_string(),
                });
            }

            spec.updating = true;
            let mut spec_clone = spec.clone();
            drop(spec);

            match node.add_child(request).await {
                Ok(share) => {
                    spec_clone.children.push(request.uri.clone());
                    let result = {
                        let mut store = registry.store.lock().await;
                        store.put_obj(&spec_clone).await
                    };
                    if let Err(error) = result {
                        let _ = node.remove_child(&request.clone().into()).await;
                        let mut spec = nexus_spec.lock().await;
                        spec.updating = false;
                        return Err(error.into());
                    }
                    let mut spec = nexus_spec.lock().await;
                    spec.children.push(request.uri.clone());
                    spec.updating = false;
                    Ok(share)
                }
                Err(error) => {
                    let mut spec = nexus_spec.lock().await;
                    spec.updating = false;
                    Err(error)
                }
            }
        } else {
            node.add_child(request).await
        }
    }

    pub(super) async fn remove_nexus_child(
        &self,
        registry: &Registry,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let node = registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        if let Some(nexus_spec) = self.get_nexus(&request.nexus).await {
            let mut spec = nexus_spec.lock().await;
            if spec.updating {
                return Err(SvcError::Conflict {});
            } else if !spec.state.created() {
                return Err(SvcError::NexusNotFound {
                    nexus_id: request.nexus.to_string(),
                });
            } else if !spec.children.contains(&request.uri) {
                return Err(SvcError::ChildNotFound {
                    nexus: request.nexus.to_string(),
                    child: request.uri.to_string(),
                });
            }

            spec.updating = true;
            let mut spec_clone = spec.clone();
            drop(spec);

            match node.remove_child(request).await {
                Ok(_) => {
                    spec_clone.children.retain(|c| c != &request.uri);
                    let result = {
                        let mut store = registry.store.lock().await;
                        store.put_obj(&spec_clone).await
                    };
                    if let Err(error) = result {
                        let mut spec = nexus_spec.lock().await;
                        spec.updating = false;
                        return Err(error.into());
                    }
                    let mut spec = nexus_spec.lock().await;
                    spec.children.retain(|c| c != &request.uri);
                    spec.updating = false;
                    Ok(())
                }
                Err(error) => {
                    let mut spec = nexus_spec.lock().await;
                    spec.updating = false;
                    Err(error)
                }
            }
        } else {
            node.remove_child(request).await
        }
    }

    fn destroy_replica_request(spec: ReplicaSpec, node: &NodeId) -> DestroyReplica {
        DestroyReplica {
            node: node.clone(),
            pool: spec.pool,
            uuid: spec.uuid,
        }
    }

    pub(super) async fn create_volume(
        &self,
        registry: &Registry,
        request: &CreateVolume,
    ) -> Result<Volume, SvcError> {
        if request.nexuses > 1 {
            tracing::error!("ANA volumes are not currently supported");
            return Err(SvcError::MultipleNexuses {});
        }

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

    pub(super) async fn destroy_volume(
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
    async fn del_volume(&self, id: &VolumeId) {
        let mut specs = self.write().await;
        specs.volumes.remove(id);
    }
    async fn del_nexus(&self, id: &NexusId) {
        let mut specs = self.write().await;
        specs.nexuses.remove(id);
    }
}
