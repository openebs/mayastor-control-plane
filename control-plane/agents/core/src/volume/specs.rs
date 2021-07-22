use crate::{
    core::{
        scheduling::{
            resources::ReplicaItem,
            volume::{GetChildForRemoval, GetSuitablePools},
        },
        specs::{ResourceSpecs, ResourceSpecsLocked, SpecOperations},
    },
    registry::Registry,
    volume::scheduling,
};
use common::{
    errors,
    errors::{NodeNotFound, NotEnough, SvcError},
};
use common_lib::{
    mbus_api::ResourceKind,
    types::v0::{
        message_bus::{
            AddNexusReplica, ChildUri, CreateNexus, CreateReplica, CreateVolume, DestroyNexus,
            DestroyReplica, DestroyVolume, Nexus, NexusId, NodeId, Protocol, PublishVolume,
            RemoveNexusReplica, Replica, ReplicaId, ReplicaOwners, SetVolumeReplica, ShareNexus,
            ShareVolume, UnpublishVolume, UnshareNexus, UnshareVolume, Volume, VolumeId,
            VolumeState,
        },
        store::{
            nexus::{NexusSpec, ReplicaUri},
            nexus_child::NexusChild,
            replica::ReplicaSpec,
            volume::{VolumeOperation, VolumeSpec},
            SpecState, SpecTransaction,
        },
    },
};
use parking_lot::Mutex;
use snafu::OptionExt;
use std::{convert::From, ops::Deref, sync::Arc};

/// Select a nexus child to be removed from a nexus
pub(crate) async fn get_nexus_child_remove_candidate(
    spec: &VolumeSpec,
    status: &Volume,
    registry: &Registry,
) -> Result<ReplicaItem, SvcError> {
    let candidates = scheduling::get_nexus_child_remove_candidate(
        &GetChildForRemoval::new(spec, status),
        registry,
    )
    .await;
    tracing::trace!(
        "Removal candidates for volume '{}': {:?}",
        spec.uuid,
        candidates
    );

    if candidates.len() <= 1 {
        Err(SvcError::ReplicaRemovalNoCandidates { id: spec.uuid() })
    } else {
        Ok(candidates.first().unwrap().clone())
    }
}

/// Return a list of appropriate CreateReplica requests per node
async fn get_volume_replica_candidates(
    registry: &Registry,
    request: impl Into<GetSuitablePools>,
) -> Result<Vec<CreateReplica>, SvcError> {
    let request = request.into();
    let pools = scheduling::get_volume_pool_candidates(request.clone(), registry).await;

    if pools.is_empty() {
        return Err(SvcError::NotEnoughResources {
            source: NotEnough::OfPools { have: 0, need: 1 },
        });
    }

    tracing::trace!(
        "Creation pool candidates for volume '{}': {:?}",
        request.uuid,
        pools
    );

    Ok(pools
        .iter()
        .map(|p| CreateReplica {
            node: p.node.clone(),
            uuid: ReplicaId::new(),
            pool: p.id.clone(),
            size: request.size,
            thin: false,
            share: Protocol::Nvmf,
            managed: true,
            owners: ReplicaOwners::from_volume(&request.uuid),
        })
        .collect::<Vec<_>>())
}

/// Return a list of appropriate CreateReplica requests per node
async fn get_create_volume_replicas(
    registry: &Registry,
    request: &CreateVolume,
) -> Result<Vec<CreateReplica>, SvcError> {
    if !request.allowed_nodes().is_empty()
        && request.replicas > request.allowed_nodes().len() as u64
    {
        // oops, how would this even work mr requester?
        return Err(SvcError::InvalidArguments {});
    }

    let node_replicas = get_volume_replica_candidates(registry, request).await?;

    if request.replicas > node_replicas.len() as u64 {
        Err(SvcError::from(NotEnough::OfPools {
            have: node_replicas.len() as u64,
            need: request.replicas,
        }))
    } else {
        Ok(node_replicas)
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Gets all VolumeSpec's
    pub(crate) fn get_volumes(&self) -> Vec<VolumeSpec> {
        self.volumes.values().map(|v| v.lock().clone()).collect()
    }
}
impl ResourceSpecsLocked {
    /// Get the protected VolumeSpec for the given volume `id`, if any exists
    pub(crate) fn get_volume(&self, id: &VolumeId) -> Option<Arc<Mutex<VolumeSpec>>> {
        let specs = self.read();
        specs.volumes.get(id).cloned()
    }

    /// Gets all VolumeSpec's
    pub(crate) fn get_volumes(&self) -> Vec<VolumeSpec> {
        let specs = self.read();
        specs.get_volumes()
    }

    /// Get a list of nodes currently used as replicas
    pub(crate) fn get_volume_data_nodes(&self, id: &VolumeId) -> Vec<NodeId> {
        let used_pools = self
            .read()
            .replicas
            .values()
            .filter(|r| r.lock().owners.owned_by(id))
            .map(|r| r.lock().pool.clone())
            .collect::<Vec<_>>();
        self.read()
            .get_pools()
            .iter()
            .filter(|p| used_pools.iter().any(|up| up == &p.id))
            .map(|p| p.node.clone())
            .collect::<Vec<_>>()
    }

    /// Get a list of protected ReplicaSpec's for the given `id`
    /// todo: we could also get the replicas from the volume nexuses?
    pub(crate) fn get_volume_replicas(&self, id: &VolumeId) -> Vec<Arc<Mutex<ReplicaSpec>>> {
        self.read()
            .replicas
            .values()
            .filter(|r| r.lock().owners.owned_by(id))
            .cloned()
            .collect()
    }
    /// Get the `NodeId` where `replica` lives
    pub(crate) async fn get_replica_node(
        registry: &Registry,
        replica: &ReplicaSpec,
    ) -> Option<NodeId> {
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
    pub(crate) fn get_volume_nexuses(&self, id: &VolumeId) -> Vec<Arc<Mutex<NexusSpec>>> {
        self.read()
            .nexuses
            .values()
            .filter(|n| n.lock().owner.as_ref() == Some(id))
            .cloned()
            .collect()
    }

    /// Return a `DestroyReplica` request based on the provided arguments
    pub(crate) fn destroy_replica_request(
        spec: ReplicaSpec,
        by: ReplicaOwners,
        node: &NodeId,
    ) -> DestroyReplica {
        DestroyReplica {
            node: node.clone(),
            pool: spec.pool,
            uuid: spec.uuid,
            disowners: by,
        }
    }

    /// Create a new volume for the given `CreateVolume` request
    pub(crate) async fn create_volume(
        &self,
        registry: &Registry,
        request: &CreateVolume,
    ) -> Result<Volume, SvcError> {
        // todo: pick nodes and pools using the Node&Pool Topology
        // todo: virtually increase the pool usage to avoid a race for space with concurrent calls
        let create_replicas = get_create_volume_replicas(registry, request).await?;

        let volume = self.get_or_create_volume(request);
        SpecOperations::start_create(&volume, registry, request).await?;

        let mut replicas = Vec::<Replica>::new();
        for replica in &create_replicas {
            if replicas.len() >= request.replicas as usize {
                break;
            } else if replicas.iter().any(|r| r.node == replica.node) {
                // don't reuse the same node
                continue;
            }
            let replica = if replicas.is_empty() {
                let mut replica = replica.clone();
                // the local replica needs to be connected via "bdev:///"
                replica.share = Protocol::None;
                replica
            } else {
                replica.clone()
            };
            match self.create_replica(registry, &replica).await {
                Ok(replica) => {
                    replicas.push(replica);
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

        // we can't fulfil the required replication factor, so let the caller
        // decide what to do next
        let result = if replicas.len() < request.replicas as usize {
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
            Err(SvcError::from(NotEnough::OfReplicas {
                have: replicas.len() as u64,
                need: request.replicas,
            }))
        } else {
            Ok(Volume {
                uuid: request.uuid.clone(),
                size: request.size,
                state: VolumeState::Online,
                protocol: Protocol::None,
                children: vec![],
            })
        };

        SpecOperations::complete_create(result, &volume, registry).await
    }

    /// Destroy a volume based on the given `DestroyVolume` request
    pub(crate) async fn destroy_volume(
        &self,
        registry: &Registry,
        request: &DestroyVolume,
    ) -> Result<(), SvcError> {
        let volume = self.get_volume(&request.uuid);
        if let Some(volume) = &volume {
            SpecOperations::start_destroy(volume, registry, false).await?;

            let mut first_error = Ok(());
            let nexuses = self.get_volume_nexuses(&request.uuid);
            for nexus in nexuses {
                let nexus = nexus.lock().deref().clone();
                if let Err(error) = self
                    .destroy_nexus(registry, &DestroyNexus::from(nexus), true)
                    .await
                {
                    if first_error.is_ok() {
                        first_error = Err(error);
                    }
                }
            }

            let replicas = self.get_volume_replicas(&request.uuid);
            for replica in replicas {
                let spec = replica.lock().deref().clone();
                if let Some(node) = Self::get_replica_node(registry, &spec).await {
                    if let Err(error) = self
                        .destroy_replica(
                            registry,
                            &Self::destroy_replica_request(spec, Default::default(), &node),
                            true,
                        )
                        .await
                    {
                        if first_error.is_ok() {
                            first_error = Err(error);
                        }
                    }
                } else {
                    // the above is able to handle when a pool is moved to a
                    // different node but if a pool is
                    // unplugged, what do we do? Fake an error ReplicaNotFound?
                }
            }

            SpecOperations::complete_destroy(first_error, volume, registry).await
        } else {
            Err(SvcError::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })
        }
    }

    /// Share a volume based on the given `ShareVolume` request
    pub(crate) async fn share_volume(
        &self,
        registry: &Registry,
        request: &ShareVolume,
    ) -> Result<String, SvcError> {
        let volume_spec = self
            .get_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let spec_clone = SpecOperations::start_update(
            registry,
            &volume_spec,
            &status,
            VolumeOperation::Share(request.protocol),
        )
        .await?;

        // Share the first child nexus (no ANA)
        assert_eq!(status.children.len(), 1);
        let nexus = status.children.get(0).unwrap();
        let result = self
            .share_nexus(registry, &ShareNexus::from((nexus, None, request.protocol)))
            .await;

        SpecOperations::complete_update(registry, result, volume_spec, spec_clone).await
    }

    /// Unshare a volume based on the given `UnshareVolume` request
    pub(crate) async fn unshare_volume(
        &self,
        registry: &Registry,
        request: &UnshareVolume,
    ) -> Result<(), SvcError> {
        let volume_spec = self
            .get_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let spec_clone =
            SpecOperations::start_update(registry, &volume_spec, &status, VolumeOperation::Unshare)
                .await?;

        // Unshare the first child nexus (no ANA)
        assert_eq!(status.children.len(), 1);
        let nexus = status.children.get(0).unwrap();
        let result = self
            .unshare_nexus(registry, &UnshareNexus::from(nexus))
            .await;

        SpecOperations::complete_update(registry, result, volume_spec, spec_clone).await
    }

    /// Publish a volume based on the given `PublishVolume` request
    pub(crate) async fn publish_volume(
        &self,
        registry: &Registry,
        request: &PublishVolume,
    ) -> Result<Volume, SvcError> {
        let spec = self
            .get_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;

        let status = registry.get_volume_status(&request.uuid).await?;
        let nexus_node = get_volume_target_node(registry, &status, request).await?;

        let spec_clone = SpecOperations::start_update(
            registry,
            &spec,
            &status,
            VolumeOperation::Publish((nexus_node.clone(), request.share)),
        )
        .await?;

        // Create a Nexus on the requested or auto-selected node
        let result = self
            .volume_create_nexus(registry, &nexus_node, &spec_clone)
            .await;

        let nexus =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Share the Nexus if it was requested
        let mut result = Ok(nexus.device_uri.clone());
        if let Some(share) = request.share {
            result = self
                .share_nexus(registry, &ShareNexus::from((&nexus, None, share)))
                .await;
        }
        SpecOperations::complete_update(registry, result, spec, spec_clone).await?;
        registry.get_volume_status(&status.uuid).await
    }

    /// Unpublish a volume based on the given `UnpublishVolume` request
    pub(crate) async fn unpublish_volume(
        &self,
        registry: &Registry,
        request: &UnpublishVolume,
    ) -> Result<Volume, SvcError> {
        let spec = self
            .get_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let spec_clone =
            SpecOperations::start_update(registry, &spec, &status, VolumeOperation::Unpublish)
                .await?;
        let nexus = get_volume_nexus(&status).expect("Already validated");

        // Destroy the Nexus
        let result = self.destroy_nexus(registry, &nexus.into(), true).await;
        SpecOperations::complete_update(registry, result, spec.clone(), spec_clone).await?;
        registry.get_volume_status(&status.uuid).await
    }

    /// Create a replica for the given volume using the provided list of candidates in order
    pub(crate) async fn create_volume_replica(
        &self,
        registry: &Registry,
        status: &Volume,
        candidates: &[CreateReplica],
    ) -> Result<Replica, SvcError> {
        let mut result = Err(SvcError::NotEnoughResources {
            source: NotEnough::OfReplicas { have: 0, need: 1 },
        });
        for attempt in candidates.iter() {
            let mut attempt = attempt.clone();

            if status.children.len() == 1 && status.children[0].node == attempt.node {
                attempt.share = Protocol::None;
            }

            result = self.create_replica(registry, &attempt).await;
            if result.is_ok() {
                break;
            }
        }
        result
    }

    /// Add the given replica to the nexuses of the given volume
    /// Only volumes with 1 nexus are currently supported
    /// todo: support N Nexuses per volume for ANA
    pub(crate) async fn add_volume_nexus_replica(
        &self,
        registry: &Registry,
        status: &Volume,
        replica: Replica,
    ) -> Result<(), SvcError> {
        let children = status.children.len();
        // status object already validated
        assert!(children == 0 || children == 1);

        if children == 1 {
            let nexus = &status.children[0];
            match self
                .add_nexus_replica(
                    registry,
                    &AddNexusReplica {
                        node: nexus.node.clone(),
                        nexus: nexus.uuid.clone(),
                        replica: ReplicaUri::new(&replica.uuid, &ChildUri::from(replica.uri)),
                        auto_rebuild: true,
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => {
                    if let Some(replica) = self.get_replica(&replica.uuid) {
                        let mut replica = replica.lock();
                        replica.disown(&ReplicaOwners::from_volume(&status.uuid));
                    }
                    Err(error)
                }
            }
        } else {
            Ok(())
        }
    }

    /// Increase the replica count of the given volume by 1
    /// Creates a new data replica from a list of candidates
    /// Adds the replica to the volume nexuses (if any)
    pub(crate) async fn increase_volume_replica(
        &self,
        registry: &Registry,
        spec: Arc<Mutex<VolumeSpec>>,
        status: Volume,
        spec_clone: VolumeSpec,
    ) -> Result<Volume, SvcError> {
        // Prepare a list of candidates (based on some criteria)
        let result = get_volume_replica_candidates(registry, &spec_clone).await;
        let candidates =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Create the data replica from the pool candidates
        let result = self
            .create_volume_replica(registry, &status, &candidates)
            .await;
        let replica =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Add the newly created replica to the nexus, if it's up
        let result = self
            .add_volume_nexus_replica(registry, &status, replica)
            .await;
        SpecOperations::complete_update(registry, result, spec, spec_clone).await?;

        registry.get_volume_status(&status.uuid).await
    }

    /// Remove a replica from all nexuses for the given volume
    /// Only volumes with 1 nexus are currently supported
    pub(crate) async fn remove_nexus_child_candidate(
        &self,
        spec_clone: &VolumeSpec,
        registry: &Registry,
        remove: &ReplicaItem,
    ) -> Result<(), SvcError> {
        if let Some(child_uri) = remove.uri() {
            // if the nexus is up, first remove the child from the nexus before deleting the replica
            let nexuses = self
                .get_volume_nexuses(&spec_clone.uuid)
                .iter()
                // todo: remove from multiple nexuses for ANA
                .find(|n| n.lock().children.iter().any(|c| &c.uri() == child_uri))
                .cloned();
            match nexuses {
                None => Ok(()),
                Some(nexus) => {
                    let nexus = nexus.lock().clone();
                    self.remove_nexus_replica(
                        registry,
                        &RemoveNexusReplica {
                            node: nexus.node,
                            nexus: nexus.uuid,
                            replica: ReplicaUri::new(&remove.spec().uuid, child_uri),
                        },
                    )
                    .await
                }
            }
        } else {
            Ok(())
        }
    }

    /// Decrement the replica count of the given volume by 1
    /// Removes the replica from all volume nexuses
    pub(crate) async fn decrease_volume_replica(
        &self,
        registry: &Registry,
        spec: Arc<Mutex<VolumeSpec>>,
        status: Volume,
        spec_clone: VolumeSpec,
    ) -> Result<Volume, SvcError> {
        // Determine which replica is most suitable to be removed
        let result = get_nexus_child_remove_candidate(&spec_clone, &status, registry).await;
        // Can fail if meanwhile the state of a replica/nexus/child changes, so fail gracefully
        let remove =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Remove the replica from its nexus (where it exists as a child)
        let result = self
            .remove_nexus_child_candidate(&spec_clone, registry, &remove)
            .await;
        SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // now remove the replica from the pool
        let result = self
            .destroy_replica_spec(
                registry,
                remove.spec(),
                ReplicaOwners::from_volume(&status.uuid),
                false,
            )
            .await;

        SpecOperations::complete_update(registry, result, spec, spec_clone).await?;
        registry.get_volume_status(&status.uuid).await
    }

    /// Sets a volume's replica count on the given `SetVolumeReplica` request
    pub(crate) async fn set_volume_replica(
        &self,
        registry: &Registry,
        request: &SetVolumeReplica,
    ) -> Result<Volume, SvcError> {
        let spec = self
            .get_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let status = registry.get_volume_status(&request.uuid).await?;

        let operation = VolumeOperation::SetReplica(request.replicas);
        let spec_clone = SpecOperations::start_update(registry, &spec, &status, operation).await?;

        assert_ne!(request.replicas, spec_clone.num_replicas);
        if request.replicas > spec_clone.num_replicas {
            self.increase_volume_replica(registry, spec, status, spec_clone)
                .await
        } else {
            self.decrease_volume_replica(registry, spec, status, spec_clone)
                .await
        }
    }

    /// Create a nexus for the given volume on the specified target_node
    /// Existing replicas may be shared/unshared so we can connect to them
    async fn volume_create_nexus(
        &self,
        registry: &Registry,
        target_node: &NodeId,
        vol_spec: &VolumeSpec,
    ) -> Result<Nexus, SvcError> {
        // find all replica status
        let status_replicas = registry.get_replicas().await.unwrap();
        // find all replica specs for this volume
        let spec_replicas = self.get_volume_replicas(&vol_spec.uuid);

        let mut spec_status_pair = vec![];
        for status_replica in status_replicas.iter() {
            for locked_replica in spec_replicas.iter() {
                let mut spec_replica = locked_replica.lock();
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
                let spec = spec.lock();
                let local = &status.node == target_node;
                (
                    local && (spec.share.shared() | status.share.shared()),
                    !local && (!spec.share.shared() | !status.share.shared()),
                )
            };
            if share {
                // unshare the replica
                if let Ok(uri) = self.unshare_replica(registry, &status.into()).await {
                    nexus_replicas.push((spec, ChildUri::from(uri)));
                }
            } else if unshare {
                // share the replica
                if let Ok(uri) = self.share_replica(registry, &status.into()).await {
                    nexus_replicas.push((spec, ChildUri::from(uri)));
                }
            } else {
                nexus_replicas.push((spec, ChildUri::from(&status.uri)));
            }
        }

        // Create the nexus on the request.node
        self.create_nexus(
            registry,
            &CreateNexus {
                node: target_node.clone(),
                uuid: NexusId::new(),
                size: vol_spec.size,
                children: nexus_replicas
                    .iter()
                    .map(|(spec, uri)| NexusChild::from(&ReplicaUri::new(&spec.lock().uuid, uri)))
                    .collect(),
                managed: true,
                owner: Some(vol_spec.uuid.clone()),
            },
        )
        .await
    }

    /// Remove volume by its `id`
    pub(super) fn remove_volume(&self, id: &VolumeId) {
        let mut specs = self.write();
        specs.volumes.remove(id);
    }
    /// Get or Create the protected VolumeSpec for the given request
    fn get_or_create_volume(&self, request: &CreateVolume) -> Arc<Mutex<VolumeSpec>> {
        let mut specs = self.write();
        if let Some(volume) = specs.volumes.get(&request.uuid) {
            volume.clone()
        } else {
            specs.volumes.insert(VolumeSpec::from(request))
        }
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

#[async_trait::async_trait]
impl SpecOperations for VolumeSpec {
    type Create = CreateVolume;
    type Owners = ();
    type State = VolumeState;
    type Status = Volume;
    type UpdateOp = VolumeOperation;

    fn start_update_op(
        &mut self,
        status: &Self::Status,
        operation: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        // No ANA support, there can only be more than 1 nexus if we've recreated the nexus
        // on another node and original nexus reappears.
        // In this case, the reconciler will destroy one of them.
        if (self.target_node.is_some() && status.children.len() != 1)
            || self.target_node.is_none() && !status.children.is_empty()
        {
            return Err(SvcError::NotReady {
                kind: self.kind(),
                id: self.uuid(),
            });
        }

        match &operation {
            VolumeOperation::Share(_) if self.protocol.shared() => Err(SvcError::AlreadyShared {
                kind: self.kind(),
                id: self.uuid(),
                share: status.protocol.to_string(),
            }),
            VolumeOperation::Share(_) => Ok(()),
            VolumeOperation::Unshare if !self.protocol.shared() => Err(SvcError::NotShared {
                kind: self.kind(),
                id: self.uuid(),
            }),
            VolumeOperation::Unshare => Ok(()),
            VolumeOperation::Publish((_, share_option))
                if self.target_node.is_some()
                    || (share_option.is_some() && self.protocol.shared()) =>
            {
                let target_node = self.target_node.as_ref();
                Err(SvcError::VolumeAlreadyPublished {
                    vol_id: self.uuid(),
                    node: target_node.map_or("".into(), ToString::to_string),
                    protocol: self.protocol.to_string(),
                })
            }

            VolumeOperation::Publish(_) => Ok(()),
            VolumeOperation::Unpublish => Ok(()),

            VolumeOperation::SetReplica(replica_count) => {
                if *replica_count == self.num_replicas {
                    Err(SvcError::ReplicaCountAchieved {
                        id: self.uuid(),
                        count: self.num_replicas,
                    })
                } else if *replica_count < 1 {
                    Err(SvcError::LastReplica {
                        id: self.uuid.to_string(),
                    })
                } else if (*replica_count as i16 - self.num_replicas as i16).abs() > 1 {
                    Err(SvcError::ReplicaChangeCount {})
                } else if status.state != VolumeState::Online
                    && (*replica_count > self.num_replicas)
                {
                    Err(SvcError::ReplicaIncrease {
                        volume_id: self.uuid(),
                        volume_state: status.state.to_string(),
                    })
                } else {
                    Ok(())
                }
            }

            VolumeOperation::Create => unreachable!(),
            VolumeOperation::Destroy => unreachable!(),
        }?;
        self.start_op(operation);
        Ok(())
    }
    fn start_create_op(&mut self) {
        self.start_op(VolumeOperation::Create);
    }
    fn start_destroy_op(&mut self) {
        self.start_op(VolumeOperation::Destroy);
    }
    fn remove_spec(locked_spec: &Arc<Mutex<Self>>, registry: &Registry) {
        let uuid = locked_spec.lock().uuid.clone();
        registry.specs.remove_volume(&uuid);
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
        ResourceKind::Volume
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
}
