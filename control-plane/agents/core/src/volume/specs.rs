use crate::{
    core::{
        scheduling::{
            nexus::GetPersistedNexusChildren,
            resources::{ChildItem, HealthyChildItems, NexusChildItem, ReplicaItem},
            volume::{AddVolumeNexusReplicas, GetChildForRemoval, GetSuitablePools},
            ResourceFilter,
        },
        specs::{OperationSequenceGuard, ResourceSpecs, ResourceSpecsLocked, SpecOperations},
    },
    registry::Registry,
    volume::scheduling,
};
use common::{
    errors,
    errors::{NotEnough, SvcError, SvcError::VolumeNotFound},
};
use common_lib::{
    mbus_api::{ErrorChain, ResourceKind},
    types::v0::{
        message_bus::{
            AddNexusReplica, ChildUri, CreateNexus, CreateReplica, CreateVolume, DestroyNexus,
            DestroyReplica, DestroyVolume, Nexus, NexusId, NodeId, Protocol, PublishVolume,
            RemoveNexusReplica, Replica, ReplicaId, ReplicaOwners, SetVolumeReplica, ShareNexus,
            ShareVolume, UnpublishVolume, UnshareNexus, UnshareVolume, Volume, VolumeId,
            VolumeState, VolumeStatus,
        },
        store::{
            nexus::{NexusSpec, ReplicaUri},
            nexus_child::NexusChild,
            replica::ReplicaSpec,
            volume::{VolumeOperation, VolumeSpec},
            OperationMode, SpecStatus, SpecTransaction,
        },
    },
};
use parking_lot::Mutex;
use snafu::OptionExt;
use std::{convert::From, ops::Deref, sync::Arc};

/// Select a replica to be removed from the volume
pub(crate) async fn get_volume_replica_remove_candidate(
    spec: &VolumeSpec,
    state: &VolumeState,
    registry: &Registry,
) -> Result<ReplicaItem, SvcError> {
    let candidates = scheduling::get_volume_replica_remove_candidates(
        &GetChildForRemoval::new(spec, state, false),
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

/// Select a replica to be removed from the volume
pub(crate) async fn get_volume_unused_replica_remove_candidates(
    spec: &VolumeSpec,
    state: &VolumeState,
    registry: &Registry,
) -> Vec<ReplicaItem> {
    let candidates = scheduling::get_volume_replica_remove_candidates(
        &GetChildForRemoval::new(spec, state, true),
        registry,
    )
    .await;
    tracing::trace!(
        "Unused Replica removal candidates for volume '{}': {:?}",
        spec.uuid,
        candidates
    );

    candidates
}

/// Get a list of nexus children to be removed from a nexus
pub(crate) async fn get_nexus_child_remove_candidates(
    vol_spec: &VolumeSpec,
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<Vec<NexusChildItem>, SvcError> {
    let candidates =
        scheduling::get_nexus_child_remove_candidates(vol_spec, nexus_spec, registry).await;
    tracing::trace!(
        "Nexus Child removal candidates for nexus '{}' of volume '{}': {:?}",
        nexus_spec.uuid,
        vol_spec.uuid,
        candidates
    );

    if candidates.len() <= 1 {
        Err(SvcError::ReplicaRemovalNoCandidates {
            id: vol_spec.uuid(),
        })
    } else {
        Ok(candidates)
    }
}

/// Get a list of existing candidate volume replicas to attach to a given nexus
/// Useful to attach replicas to a nexus when the number of nexus children does not match
/// the volume's replica count
pub(crate) async fn get_nexus_attach_candidates(
    vol_spec: &VolumeSpec,
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<Vec<ChildItem>, SvcError> {
    let candidates = AddVolumeNexusReplicas::builder_with_defaults(vol_spec, nexus_spec, registry)
        .await?
        .collect();
    tracing::debug!(
        "Nexus Replica Attach candidates for nexus '{}' of volume '{}': {:?}",
        nexus_spec.uuid,
        vol_spec.uuid,
        candidates
    );

    Ok(candidates)
}

/// Return a list of appropriate requests which can be used to create a a replica on a pool
/// This can be used when the volume's current replica count is smaller than the desired volume's
/// replica count
pub(crate) async fn get_volume_replica_candidates(
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
            share: Protocol::None,
            managed: true,
            owners: ReplicaOwners::from_volume(&request.uuid),
        })
        .collect::<Vec<_>>())
}

/// Return a list of appropriate requests which can be used to create a a replica on a pool
/// This can be used when creating a volume
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

/// Get all usable healthy replicas for volume nexus creation
/// If no usable replica is available, return an error
pub(crate) async fn get_healthy_volume_replicas(
    spec: &VolumeSpec,
    target_node: &NodeId,
    registry: &Registry,
) -> Result<HealthyChildItems, SvcError> {
    let children = scheduling::get_healthy_volume_replicas(
        &GetPersistedNexusChildren::new(spec, target_node),
        registry,
    )
    .await?;

    tracing::trace!(
        "Healthy volume nexus replicas for volume '{}': {:?}",
        spec.uuid,
        children
    );

    if children.is_empty() {
        Err(SvcError::NoOnlineReplicas { id: spec.uuid() })
    } else {
        Ok(children)
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
    pub(crate) fn get_locked_volume(&self, id: &VolumeId) -> Option<Arc<Mutex<VolumeSpec>>> {
        let specs = self.read();
        specs.volumes.get(id).cloned()
    }

    /// Get a copy of the VolumeSpec for the volume with the given ID.
    pub(crate) fn get_volume(&self, id: &VolumeId) -> Result<VolumeSpec, SvcError> {
        match self.get_locked_volume(id) {
            Some(locked_spec) => {
                let spec = locked_spec.lock();
                Ok(spec.clone())
            }
            None => Err(VolumeNotFound {
                vol_id: id.to_string(),
            }),
        }
    }

    /// Gets a copy of all VolumeSpec's
    pub(crate) fn get_volumes(&self) -> Vec<VolumeSpec> {
        let specs = self.read();
        specs.get_volumes()
    }
    /// Gets a copy of all locked VolumeSpec's
    pub(crate) fn get_locked_volumes(&self) -> Vec<Arc<Mutex<VolumeSpec>>> {
        let specs = self.read();
        specs.volumes.to_vec()
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

    /// Get a list of protected ReplicaSpec's for the given volume `id`
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
        mode: OperationMode,
    ) -> Result<Volume, SvcError> {
        let volume = self.get_or_create_volume(request);
        let (volume_clone, _guard) =
            SpecOperations::start_create(&volume, registry, request, mode).await?;

        // todo: pick nodes and pools using the Node&Pool Topology
        // todo: virtually increase the pool usage to avoid a race for space with concurrent calls
        let result = get_create_volume_replicas(registry, request).await;
        let create_replicas =
            SpecOperations::validate_update_step(registry, result, &volume, &volume_clone).await?;

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
            match self.create_replica(registry, &replica, mode).await {
                Ok(replica) => {
                    replicas.push(replica);
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to create replica {:?} for volume {}, error: {}",
                        replica,
                        request.uuid,
                        error.full_string()
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
                    .destroy_replica(registry, &replica.clone().into(), true, mode)
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
            Ok(())
        };

        SpecOperations::complete_create(result, &volume, registry).await?;
        registry.get_volume(&request.uuid).await
    }

    /// Destroy a volume based on the given `DestroyVolume` request
    pub(crate) async fn destroy_volume(
        &self,
        registry: &Registry,
        request: &DestroyVolume,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let volume = self.get_locked_volume(&request.uuid);
        if let Some(volume) = &volume {
            SpecOperations::start_destroy(volume, registry, false, mode).await?;

            let mut first_error = Ok(());
            let nexuses = self.get_volume_nexuses(&request.uuid);
            for nexus in nexuses {
                let nexus = nexus.lock().deref().clone();
                if let Err(error) = self
                    .destroy_nexus(registry, &DestroyNexus::from(nexus), true, mode)
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
                            mode,
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
        mode: OperationMode,
    ) -> Result<String, SvcError> {
        let volume_spec =
            self.get_locked_volume(&request.uuid)
                .context(errors::VolumeNotFound {
                    vol_id: request.uuid.to_string(),
                })?;

        let state = registry.get_volume_state(&request.uuid).await?;

        let (spec_clone, _guard) = SpecOperations::start_update(
            registry,
            &volume_spec,
            &state,
            VolumeOperation::Share(request.protocol),
            mode,
        )
        .await?;

        // Share the first child nexus (no ANA)
        assert_eq!(state.children.len(), 1);
        let nexus = state.children.get(0).unwrap();
        let result = self
            .share_nexus(
                registry,
                &ShareNexus::from((nexus, None, request.protocol)),
                mode,
            )
            .await;

        SpecOperations::complete_update(registry, result, volume_spec, spec_clone).await
    }

    /// Unshare a volume based on the given `UnshareVolume` request
    pub(crate) async fn unshare_volume(
        &self,
        registry: &Registry,
        request: &UnshareVolume,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let volume_spec =
            self.get_locked_volume(&request.uuid)
                .context(errors::VolumeNotFound {
                    vol_id: request.uuid.to_string(),
                })?;
        let state = registry.get_volume_state(&request.uuid).await?;

        let (spec_clone, _guard) = SpecOperations::start_update(
            registry,
            &volume_spec,
            &state,
            VolumeOperation::Unshare,
            mode,
        )
        .await?;

        // Unshare the first child nexus (no ANA)
        assert_eq!(state.children.len(), 1);
        let nexus = state.children.get(0).unwrap();
        let result = self
            .unshare_nexus(registry, &UnshareNexus::from(nexus), mode)
            .await;

        SpecOperations::complete_update(registry, result, volume_spec, spec_clone).await
    }

    /// Publish a volume based on the given `PublishVolume` request
    pub(crate) async fn publish_volume(
        &self,
        registry: &Registry,
        request: &PublishVolume,
        mode: OperationMode,
    ) -> Result<Volume, SvcError> {
        let spec = self
            .get_locked_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;

        let state = registry.get_volume_state(&request.uuid).await?;
        let nexus_node = get_volume_target_node(registry, &state, request).await?;
        let nexus_id = NexusId::new();

        let operation =
            VolumeOperation::Publish((nexus_node.clone(), nexus_id.clone(), request.share));
        let (spec_clone, _guard) =
            SpecOperations::start_update(registry, &spec, &state, operation, mode).await?;

        // Create a Nexus on the requested or auto-selected node
        let result = self
            .volume_create_nexus(registry, &nexus_node, &nexus_id, &spec_clone, mode)
            .await;

        let nexus =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Share the Nexus if it was requested
        let mut result = Ok(nexus.clone());
        if let Some(share) = request.share {
            result = self
                .share_nexus(registry, &ShareNexus::from((&nexus, None, share)), mode)
                .await
                .map(|_| nexus);
        }

        SpecOperations::complete_update(registry, result, spec, spec_clone.clone()).await?;
        registry.get_volume(&request.uuid).await
    }

    /// Unpublish a volume based on the given `UnpublishVolume` request
    pub(crate) async fn unpublish_volume(
        &self,
        registry: &Registry,
        request: &UnpublishVolume,
        mode: OperationMode,
    ) -> Result<Volume, SvcError> {
        let spec = self
            .get_locked_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let state = registry.get_volume_state(&request.uuid).await?;

        let (spec_clone, _guard) =
            SpecOperations::start_update(registry, &spec, &state, VolumeOperation::Unpublish, mode)
                .await?;
        let nexus = get_volume_nexus(&state).expect("Already validated");

        // Destroy the Nexus
        let result = self
            .destroy_nexus(registry, &nexus.into(), true, mode)
            .await;
        SpecOperations::complete_update(registry, result, spec.clone(), spec_clone.clone()).await?;
        registry.get_volume(&request.uuid).await
    }

    /// Create a replica for the given volume using the provided list of candidates in order
    pub(crate) async fn create_volume_replica(
        &self,
        registry: &Registry,
        state: &VolumeState,
        candidates: &[CreateReplica],
        mode: OperationMode,
    ) -> Result<Replica, SvcError> {
        let mut result = Err(SvcError::NotEnoughResources {
            source: NotEnough::OfReplicas { have: 0, need: 1 },
        });
        for attempt in candidates.iter() {
            let mut attempt = attempt.clone();

            if state.children.len() == 1 && state.children[0].node == attempt.node {
                attempt.share = Protocol::None;
            }

            result = self.create_replica(registry, &attempt, mode).await;
            if result.is_ok() {
                break;
            }
        }
        result
    }

    /// Create `count` replicas for the given volume using the provided list of candidates, in order
    pub(crate) async fn create_volume_replicas(
        &self,
        registry: &Registry,
        volume_uuid: &VolumeId,
        candidates: Vec<CreateReplica>,
        count: usize,
        mode: OperationMode,
    ) -> usize {
        let mut created = 0;
        for attempt in candidates.into_iter() {
            if created >= count {
                break;
            }

            match self.create_replica(registry, &attempt, mode).await {
                Ok(replica) => {
                    tracing::debug!(
                        "Successfully created replica '{}' for volume '{}'",
                        replica.uuid,
                        volume_uuid
                    );

                    created += 1;
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to created replica '{:?}' for volume '{}', error: '{}'",
                        attempt,
                        volume_uuid,
                        error.full_string(),
                    );
                }
            }
        }
        created
    }

    /// Add the given replica to the nexuses of the given volume
    /// Only volumes with 1 nexus are currently supported
    /// todo: support N Nexuses per volume for ANA
    async fn add_replica_to_volume(
        &self,
        registry: &Registry,
        status: &VolumeState,
        replica: Replica,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let children = status.children.len();
        // status object already validated
        assert!(children == 0 || children == 1);

        if children == 1 {
            let nexus = &status.children[0];
            self.attach_replica_to_nexus(registry, &status.uuid, nexus, &replica, mode)
                .await
        } else {
            Ok(())
        }
    }

    /// Increase the replica count of the given volume by 1
    /// Creates a new data replica from a list of candidates
    /// Adds the replica to the volume nexuses (if any)
    async fn increase_volume_replica(
        &self,
        registry: &Registry,
        spec: Arc<Mutex<VolumeSpec>>,
        state: VolumeState,
        spec_clone: VolumeSpec,
        mode: OperationMode,
    ) -> Result<Volume, SvcError> {
        // Prepare a list of candidates (based on some criteria)
        let result = get_volume_replica_candidates(registry, &spec_clone).await;
        let candidates =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Create the data replica from the pool candidates
        let result = self
            .create_volume_replica(registry, &state, &candidates, mode)
            .await;
        let replica =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Add the newly created replica to the nexus, if it's up
        let result = self
            .add_replica_to_volume(registry, &state, replica, mode)
            .await;
        SpecOperations::complete_update(registry, result, spec, spec_clone).await?;

        registry.get_volume(&state.uuid).await
    }

    /// Remove a replica from all nexuses for the given volume
    /// Only volumes with 1 nexus are currently supported
    pub(crate) async fn remove_volume_child_candidate(
        &self,
        spec_clone: &VolumeSpec,
        registry: &Registry,
        remove: &ReplicaItem,
        mode: OperationMode,
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
                        mode,
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
    async fn decrease_volume_replica(
        &self,
        registry: &Registry,
        spec: Arc<Mutex<VolumeSpec>>,
        state: VolumeState,
        spec_clone: VolumeSpec,
        mode: OperationMode,
    ) -> Result<Volume, SvcError> {
        // Determine which replica is most suitable to be removed
        let result = get_volume_replica_remove_candidate(&spec_clone, &state, registry).await;
        // Can fail if meanwhile the state of a replica/nexus/child changes, so fail gracefully
        let remove =
            SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // Remove the replica from its nexus (where it exists as a child)
        let result = self
            .remove_volume_child_candidate(&spec_clone, registry, &remove, mode)
            .await;
        SpecOperations::validate_update_step(registry, result, &spec, &spec_clone).await?;

        // now remove the replica from the pool
        let result = self
            .destroy_replica_spec(
                registry,
                remove.spec(),
                ReplicaOwners::from_volume(&state.uuid),
                false,
                mode,
            )
            .await;

        SpecOperations::complete_update(registry, result, spec, spec_clone).await?;
        registry.get_volume(&state.uuid).await
    }

    /// Sets a volume's replica count on the given `SetVolumeReplica` request
    pub(crate) async fn set_volume_replica(
        &self,
        registry: &Registry,
        request: &SetVolumeReplica,
        mode: OperationMode,
    ) -> Result<Volume, SvcError> {
        let spec = self
            .get_locked_volume(&request.uuid)
            .context(errors::VolumeNotFound {
                vol_id: request.uuid.to_string(),
            })?;
        let state = registry.get_volume_state(&request.uuid).await?;

        let operation = VolumeOperation::SetReplica(request.replicas);
        let (spec_clone, _guard) =
            SpecOperations::start_update(registry, &spec, &state, operation, mode).await?;

        assert_ne!(request.replicas, spec_clone.num_replicas);
        if request.replicas > spec_clone.num_replicas {
            self.increase_volume_replica(registry, spec, state, spec_clone.clone(), mode)
                .await?
        } else {
            self.decrease_volume_replica(registry, spec, state, spec_clone.clone(), mode)
                .await?
        };

        registry.get_volume(&request.uuid).await
    }

    /// Make the replica accessible on the specified `NodeId`
    /// This means the replica might have to be shared/unshared so it can be open through
    /// the correct protocol (loopback locally, and nvmf remotely)
    async fn make_replica_accessible(
        &self,
        registry: &Registry,
        replica_state: &Replica,
        nexus_node: &NodeId,
        mode: OperationMode,
    ) -> Result<ChildUri, SvcError> {
        if nexus_node == &replica_state.node {
            // on the same node, so connect via the loopback bdev
            match self
                .unshare_replica(registry, &replica_state.into(), mode)
                .await
            {
                Ok(uri) => Ok(uri.into()),
                Err(SvcError::NotShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        } else {
            // on a different node, so connect via an nvmf target
            match self
                .share_replica(registry, &replica_state.into(), mode)
                .await
            {
                Ok(uri) => Ok(uri.into()),
                Err(SvcError::AlreadyShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        }
    }

    /// Create a nexus for the given volume on the specified target_node
    /// Existing replicas may be shared/unshared so we can connect to them
    async fn volume_create_nexus(
        &self,
        registry: &Registry,
        target_node: &NodeId,
        nexus_id: &NexusId,
        vol_spec: &VolumeSpec,
        mode: OperationMode,
    ) -> Result<Nexus, SvcError> {
        let children = get_healthy_volume_replicas(vol_spec, target_node, registry).await?;
        let (count, items) = match children {
            HealthyChildItems::One(candidates) => (1, candidates),
            HealthyChildItems::All(candidates) => (candidates.len(), candidates),
        };

        let mut nexus_replicas = vec![];
        let mut nodes = vec![];
        for item in items {
            if nexus_replicas.len() >= count {
                break;
            } else if nodes.contains(&item.state().node) {
                // spread the children across the nodes
                continue;
            }

            if let Ok(uri) = self
                .make_replica_accessible(registry, item.state(), target_node, mode)
                .await
            {
                nexus_replicas.push(NexusChild::Replica(ReplicaUri::new(
                    &item.spec().uuid,
                    &uri,
                )));
                nodes.push(item.state().node.clone());
            }
        }

        // Create the nexus on the requested node
        self.create_nexus(
            registry,
            &CreateNexus {
                node: target_node.clone(),
                uuid: nexus_id.clone(),
                size: vol_spec.size,
                children: nexus_replicas,
                managed: true,
                owner: Some(vol_spec.uuid.clone()),
            },
            mode,
        )
        .await
    }

    /// Attach the specified replica to the volume nexus
    /// The replica might need to be shared/unshared so it can be opened by the nexus
    pub(crate) async fn attach_replica_to_nexus(
        &self,
        registry: &Registry,
        volume_uuid: &VolumeId,
        nexus: &Nexus,
        replica: &Replica,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let uri = self
            .make_replica_accessible(registry, replica, &nexus.node, mode)
            .await?;
        match self
            .add_nexus_replica(
                registry,
                &AddNexusReplica {
                    node: nexus.node.clone(),
                    nexus: nexus.uuid.clone(),
                    replica: ReplicaUri::new(&replica.uuid, &uri),
                    auto_rebuild: true,
                },
                mode,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                if let Some(replica) = self.get_replica(&replica.uuid) {
                    let mut replica = replica.lock();
                    replica.disown(&ReplicaOwners::from_volume(volume_uuid));
                }
                Err(error)
            }
        }
    }

    /// Remove unused replicas from the volume
    /// (that is, replicas which are not used by a nexus and are in excess to the
    /// volume's replica count).
    pub(crate) async fn remove_unused_volume_replicas(
        &self,
        registry: &Registry,
        volume_spec: &Arc<Mutex<VolumeSpec>>,
        mut count: usize,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let spec_clone = volume_spec.lock().clone();
        let state = registry.get_volume_state(&spec_clone.uuid).await?;

        let candidates =
            get_volume_unused_replica_remove_candidates(&spec_clone, &state, registry).await;

        let mut result = Ok(());
        for replica in candidates {
            if count == 0 {
                break;
            }
            match self
                .remove_unused_volume_replica(registry, volume_spec, &replica.spec().uuid, mode)
                .await
            {
                Ok(_) => {
                    tracing::info!("Successfully removed replica '{}'", replica.spec().uuid);
                    count -= 1;
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to remove unused replicas xx: {}",
                        error.full_string()
                    );
                    result = Err(error);
                }
            }
        }
        result
    }

    /// Remove unused replica from the volume
    /// (that is, replicas which are not used by a nexus).
    /// It must not be the last replica of the volume
    /// (an error will be returned in such case).
    pub(crate) async fn remove_unused_volume_replica(
        &self,
        registry: &Registry,
        volume_spec: &Arc<Mutex<VolumeSpec>>,
        replica_id: &ReplicaId,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let volume_uuid = volume_spec.lock().uuid.clone();
        let (spec_clone, _guard) = SpecOperations::start_update(
            registry,
            volume_spec,
            &registry.get_volume_state(&volume_uuid).await?,
            VolumeOperation::RemoveUnusedReplica(replica_id.clone()),
            mode,
        )
        .await?;

        // The replica is unused, so we can disown it...
        let replica = self
            .get_replica(replica_id)
            .context(errors::ReplicaNotFound {
                replica_id: replica_id.to_owned(),
            });
        let replica =
            SpecOperations::validate_update_step(registry, replica, volume_spec, &spec_clone)
                .await?;

        // disown it from the volume first, so at the very least it can be garbage collected
        // at a later point if the node is not accessible
        let result = self.disown_volume_replica(registry, &replica).await;
        SpecOperations::validate_update_step(registry, result, volume_spec, &spec_clone).await?;

        // the garbage collector will destroy it at a later time
        let _ = self.destroy_volume_replica(registry, None, &replica).await;
        SpecOperations::complete_update(registry, Ok(()), volume_spec.clone(), spec_clone.clone())
            .await?;
        Ok(())
    }

    /// Attach existing replicas to the given volume nexus until it reaches the required number of
    /// data replicas.
    /// Returns the first encountered error, but tries to add as many as it can until it does so.
    pub(crate) async fn attach_replicas_to_nexus(
        &self,
        registry: &Registry,
        volume_spec: &Arc<Mutex<VolumeSpec>>,
        nexus_spec: &Arc<Mutex<NexusSpec>>,
        nexus_state: &Nexus,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let vol_spec_clone = volume_spec.lock().clone();
        let vol_uuid = vol_spec_clone.uuid.clone();
        let nexus_spec_clone = nexus_spec.lock().clone();
        let volume_children = self
            .get_volume_replicas(&vol_spec_clone.uuid)
            .len()
            .min(vol_spec_clone.num_replicas as usize);
        let mut nexus_children = nexus_spec_clone.children.len();

        let replicas =
            get_nexus_attach_candidates(&vol_spec_clone, &nexus_spec_clone, registry).await?;

        let mut result = Ok(());
        for replica in replicas {
            if nexus_children >= volume_children {
                break;
            }
            match self
                .attach_replica_to_nexus(registry, &vol_uuid, nexus_state, replica.state(), mode)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        "Successfully attached replica '{}' to nexus '{}'",
                        replica.state().uuid,
                        nexus_state.uuid
                    );
                    nexus_children += 1;
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to attach replica '{}' to nexus '{}' part of volume '{}': '{}'",
                        replica.state().uuid,
                        nexus_state.uuid,
                        vol_spec_clone.uuid,
                        error.full_string()
                    );
                    result = Err(error);
                }
            };
        }
        result
    }

    /// Remove excessive replicas from the given volume nexus.
    /// It should not have more replicas then the volume's required replica count.
    /// Returns the first encountered error, but tries to remove as many as it can until it does so.
    pub(crate) async fn remove_excess_replicas_from_nexus(
        &self,
        registry: &Registry,
        volume_spec: &Arc<Mutex<VolumeSpec>>,
        nexus_spec: &Arc<Mutex<NexusSpec>>,
        nexus_state: &Nexus,
        mode: OperationMode,
    ) -> Result<(), SvcError> {
        let vol_spec_clone = volume_spec.lock().clone();
        let nexus_spec_clone = nexus_spec.lock().clone();
        let volume_children = vol_spec_clone.num_replicas as usize;
        let mut nexus_replica_children =
            nexus_spec_clone
                .children
                .iter()
                .fold(0usize, |mut c, child| {
                    if let Some(replica) = child.as_replica() {
                        if registry.specs.get_replica(replica.uuid()).is_some() {
                            c += 1;
                        }
                    }
                    c
                });

        let candidates =
            get_nexus_child_remove_candidates(&vol_spec_clone, &nexus_spec_clone, registry).await?;

        let mut result = Ok(());
        for candidate in candidates {
            if nexus_replica_children <= volume_children {
                break;
            }
            match self
                .remove_nexus_child_by_uri(registry, nexus_state, candidate.child_uri(), true, mode)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        "Successfully removed child '{}' from nexus '{}' part of volume '{}'",
                        candidate.child_uri(),
                        nexus_state.uuid,
                        vol_spec_clone.uuid
                    );
                    nexus_replica_children -= 1;
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to remove child '{}' from nexus '{}' part of volume '{}': '{}'",
                        candidate.child_uri(),
                        nexus_state.uuid,
                        vol_spec_clone.uuid,
                        error.full_string()
                    );
                    result = Err(error);
                }
            }
        }
        result
    }

    /// Disown replica from its volume
    async fn disown_volume_replica(
        &self,
        registry: &Registry,
        replica: &Arc<Mutex<ReplicaSpec>>,
    ) -> Result<(), SvcError> {
        replica.lock().owners.disowned_by_volume();
        let clone = replica.lock().clone();
        registry.store_obj(&clone).await
    }

    /// Destroy the replica from its volume
    pub(crate) async fn destroy_volume_replica(
        &self,
        registry: &Registry,
        node_id: Option<&NodeId>,
        replica: &Arc<Mutex<ReplicaSpec>>,
    ) -> Result<(), SvcError> {
        let node_id = match node_id {
            Some(node_id) => node_id.clone(),
            None => {
                let replica_uuid = replica.lock().uuid.clone();
                match registry.get_replica(&replica_uuid).await {
                    Ok(state) => state.node.clone(),
                    Err(_) => {
                        let pool_id = replica.lock().pool.clone();
                        let pool_spec = self.get_pool(&pool_id).context(errors::PoolNotFound {
                            pool_id: pool_id.to_string(),
                        })?;
                        let node_id = pool_spec.lock().node.clone();
                        node_id
                    }
                }
            }
        };

        let spec = replica.lock().clone();
        self.destroy_replica(
            registry,
            &Self::destroy_replica_request(spec, Default::default(), &node_id),
            true,
            OperationMode::ReconcileStep,
        )
        .await
    }

    /// Disown and destroy the replica from its volume
    pub(crate) async fn disown_and_destroy_replica(
        &self,
        registry: &Registry,
        node: &NodeId,
        replica_uuid: &ReplicaId,
    ) -> Result<(), SvcError> {
        if let Some(replica) = self.get_replica(replica_uuid) {
            // disown it from the volume first, so at the very least it can be garbage collected
            // at a later point if the node is not accessible
            self.disown_volume_replica(registry, &replica).await?;
            self.destroy_volume_replica(registry, Some(node), &replica)
                .await
        } else {
            Err(SvcError::ReplicaNotFound {
                replica_id: replica_uuid.to_owned(),
            })
        }
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

    /// Worker that reconciles dirty VolumeSpecs's with the persistent store.
    /// This is useful when nexus operations are performed but we fail to
    /// update the spec with the persistent store.
    pub async fn reconcile_dirty_volumes(&self, registry: &Registry) -> bool {
        if registry.store_online().await {
            let mut pending_count = 0;

            let volumes = self.get_locked_volumes();
            for volume_spec in volumes {
                let (mut volume_clone, _guard) = {
                    if let Ok(guard) = volume_spec.operation_guard(OperationMode::ReconcileStart) {
                        let volume = volume_spec.lock();
                        if !volume.status.created() {
                            continue;
                        }
                        (volume.clone(), guard)
                    } else {
                        continue;
                    }
                };

                if let Some(op) = volume_clone.operation.clone() {
                    let fail = !match op.result {
                        Some(true) => {
                            volume_clone.commit_op();
                            let result = registry.store_obj(&volume_clone).await;
                            if result.is_ok() {
                                let mut volume = volume_spec.lock();
                                volume.commit_op();
                            }
                            result.is_ok()
                        }
                        Some(false) => {
                            volume_clone.clear_op();
                            let result = registry.store_obj(&volume_clone).await;
                            if result.is_ok() {
                                let mut volume = volume_spec.lock();
                                volume.clear_op();
                            }
                            result.is_ok()
                        }
                        None => {
                            // we must have crashed... we could check the node to see what the
                            // current state is but for now assume failure
                            volume_clone.clear_op();
                            let result = registry.store_obj(&volume_clone).await;
                            if result.is_ok() {
                                let mut volume = volume_spec.lock();
                                volume.clear_op();
                            }
                            result.is_ok()
                        }
                    };
                    if fail {
                        pending_count += 1;
                    }
                } else {
                    // No operation to reconcile.
                }
            }
            pending_count > 0
        } else {
            true
        }
    }
}

fn get_volume_nexus(volume_state: &VolumeState) -> Result<Nexus, SvcError> {
    match volume_state.children.len() {
        0 => Err(SvcError::VolumeNotPublished {
            vol_id: volume_state.uuid.to_string(),
        }),
        1 => Ok(volume_state.children[0].clone()),
        _ => Err(SvcError::NotReady {
            kind: ResourceKind::Volume,
            id: volume_state.uuid.to_string(),
        }),
    }
}

async fn get_volume_target_node(
    registry: &Registry,
    status: &VolumeState,
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
            let nodes = registry.get_node_wrappers().await;
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
            let node = registry.get_node_wrapper(node).await?;
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
    type Status = VolumeStatus;
    type State = VolumeState;
    type UpdateOp = VolumeOperation;

    fn start_update_op(
        &mut self,
        registry: &Registry,
        state: &Self::State,
        operation: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        // No ANA support, there can only be more than 1 nexus if we've recreated the nexus
        // on another node and original nexus reappears.
        // In this case, the reconciler will destroy one of them.
        if (self.target_node.is_some() && state.children.len() != 1)
            || self.target_node.is_none() && !state.children.is_empty()
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
                share: state.protocol.to_string(),
            }),
            VolumeOperation::Share(_) if self.target_node.is_none() => {
                Err(SvcError::VolumeNotPublished {
                    vol_id: self.uuid(),
                })
            }
            VolumeOperation::Share(_) => Ok(()),
            VolumeOperation::Unshare if !self.protocol.shared() => Err(SvcError::NotShared {
                kind: self.kind(),
                id: self.uuid(),
            }),
            VolumeOperation::Unshare => Ok(()),
            VolumeOperation::Publish((_, _, share_option))
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
            VolumeOperation::Unpublish => get_volume_nexus(state).map(|_| ()),

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
                } else if state.status != VolumeStatus::Online
                    && (*replica_count > self.num_replicas)
                {
                    Err(SvcError::ReplicaIncrease {
                        volume_id: self.uuid(),
                        volume_state: state.status.to_string(),
                    })
                } else {
                    Ok(())
                }
            }

            VolumeOperation::RemoveUnusedReplica(uuid) => {
                let last_replica = !registry
                    .specs
                    .get_volume_replicas(&self.uuid)
                    .iter()
                    .any(|r| &r.lock().uuid != uuid);
                let nexuses = registry.specs.get_volume_nexuses(&self.uuid);
                let used = nexuses.iter().any(|n| n.lock().contains_replica(uuid));
                if !last_replica && !used {
                    Ok(())
                } else {
                    Err(SvcError::Conflict {})
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
    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Volume
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
}
