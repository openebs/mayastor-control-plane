use crate::{
    controller::{
        registry::Registry,
        resources::{
            operations::{ResourceLifecycle, ResourceSharing},
            operations_helper::{
                GuardedOperationsHelper, OperationSequenceGuard, ResourceSpecs,
                ResourceSpecsLocked, SpecOperationsHelper,
            },
            OperationGuardArc, ResourceMutex, TraceSpan, TraceStrLog,
        },
        scheduling::{
            nexus::GetPersistedNexusChildren,
            resources::{ChildItem, HealthyChildItems, ReplicaItem},
            volume::{
                AddVolumeNexusReplicas, GetChildForRemoval, GetSuitablePools,
                ReplicaRemovalCandidates,
            },
            ResourceFilter,
        },
    },
    nexus::scheduling::target_node_candidate,
    volume::scheduling,
};
use agents::{
    errors,
    errors::{
        NotEnough, SvcError,
        SvcError::{ReplicaRemovalNoCandidates, VolumeNotFound},
    },
};
use grpc::operations::{PaginatedResult, Pagination};
use stor_port::{
    transport_api::{ErrorChain, ResourceKind},
    types::v0::{
        store::{
            definitions::ObjectKey,
            nexus::{NexusSpec, ReplicaUri},
            nexus_child::NexusChild,
            nexus_persistence::NexusInfoKey,
            replica::ReplicaSpec,
            volume::{VolumeGroupSpec, VolumeOperation, VolumeSpec},
            SpecStatus, SpecTransaction,
        },
        transport::{
            AddNexusReplica, ChildUri, CreateNexus, CreateReplica, CreateVolume, DestroyReplica,
            Nexus, NodeId, PoolId, Protocol, RemoveNexusReplica, Replica, ReplicaId, ReplicaName,
            ReplicaOwners, Volume, VolumeId, VolumeShareProtocol, VolumeState, VolumeStatus,
        },
    },
};

use crate::controller::resources::operations::ResourceOwnerUpdate;
use grpc::operations::volume::traits::PublishVolumeInfo;
use snafu::OptionExt;
use std::convert::From;
use stor_port::types::v0::{
    store::{replica::PoolRef, volume::TargetConfig},
    transport::ShareReplica,
};

/// Select a replica to be removed from the volume
pub(crate) async fn volume_replica_remove_candidate(
    spec: &VolumeSpec,
    state: &VolumeState,
    registry: &Registry,
) -> Result<ReplicaItem, SvcError> {
    let mut candidates = scheduling::volume_replica_remove_candidates(
        &GetChildForRemoval::new(spec, state, false),
        registry,
    )
    .await?
    .candidates();

    spec.trace_span(|| tracing::trace!("Volume Replica removal candidates: {:?}", candidates));

    candidates
        .next()
        .context(errors::ReplicaRemovalNoCandidates {
            id: spec.uuid_str(),
        })
}

/// Get replica candidates to be removed from the volume
/// This list includes healthy and non_healthy candidates, so care must be taken to
/// make sure we don't remove "too many healthy" candidates
pub(crate) async fn volume_unused_replica_remove_candidates(
    spec: &VolumeSpec,
    state: &VolumeState,
    registry: &Registry,
) -> Result<ReplicaRemovalCandidates, SvcError> {
    let candidates = scheduling::volume_replica_remove_candidates(
        &GetChildForRemoval::new(spec, state, true),
        registry,
    )
    .await?
    .candidates();

    spec.trace(&format!(
        "Unused Replica removal candidates for volume: {candidates:?}"
    ));

    Ok(candidates)
}

/// Get a list of nexus children to be removed from a nexus
pub(crate) async fn nexus_child_remove_candidates(
    vol_spec: &VolumeSpec,
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<ReplicaRemovalCandidates, SvcError> {
    let candidates = scheduling::nexus_child_remove_candidates(vol_spec, nexus_spec, registry)
        .await?
        .candidates();

    nexus_spec.debug(&format!("Nexus Child removal candidates: {candidates:?}"));

    Ok(candidates)
}

/// Get a list of existing candidate volume replicas to attach to a given nexus
/// Useful to attach replicas to a nexus when the number of nexus children does not match
/// the volume's replica count
pub(crate) async fn nexus_attach_candidates(
    vol_spec: &VolumeSpec,
    nexus_spec: &NexusSpec,
    registry: &Registry,
) -> Result<Vec<ChildItem>, SvcError> {
    let candidates = AddVolumeNexusReplicas::builder_with_defaults(vol_spec, nexus_spec, registry)
        .await?
        .collect();

    nexus_spec.debug(&format!("Nexus replica attach candidates: {candidates:?}"));

    Ok(candidates)
}

/// Return a list of appropriate requests which can be used to create a a replica on a pool
/// This can be used when the volume's current replica count is smaller than the desired volume's
/// replica count
pub(crate) async fn volume_replica_candidates(
    registry: &Registry,
    volume_spec: &VolumeSpec,
) -> Result<Vec<CreateReplica>, SvcError> {
    let request = GetSuitablePools::new(volume_spec);
    let pools = scheduling::volume_pool_candidates(request.clone(), registry).await;

    if pools.is_empty() {
        return Err(SvcError::NotEnoughResources {
            source: NotEnough::OfPools { have: 0, need: 1 },
        });
    }

    volume_spec.trace(&format!(
        "Creation pool candidates for volume: {:?}",
        pools.iter().map(|p| p.state()).collect::<Vec<_>>()
    ));

    Ok(pools
        .iter()
        .map(|p| {
            let replica_uuid = ReplicaId::new();
            CreateReplica {
                node: p.node.clone(),
                name: Some(ReplicaName::new(&replica_uuid, Some(&request.uuid))),
                uuid: replica_uuid,
                pool_id: p.id.clone(),
                pool_uuid: None,
                size: request.size,
                thin: request.thin,
                share: Protocol::None,
                managed: true,
                owners: ReplicaOwners::from_volume(&request.uuid),
                allowed_hosts: vec![],
            }
        })
        .collect::<Vec<_>>())
}

/// Return a list of appropriate requests which can be used to create a a replica on a pool
/// This can be used when creating a volume
pub(crate) async fn create_volume_replicas(
    registry: &Registry,
    request: &CreateVolume,
    volume: &VolumeSpec,
) -> Result<Vec<CreateReplica>, SvcError> {
    if !request.allowed_nodes().is_empty()
        && request.replicas > request.allowed_nodes().len() as u64
    {
        // oops, how would this even work mr requester?
        return Err(SvcError::InvalidArguments {});
    }

    let node_replicas = volume_replica_candidates(registry, volume).await?;

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
pub(crate) async fn healthy_volume_replicas(
    spec: &VolumeSpec,
    target_node: &NodeId,
    registry: &Registry,
) -> Result<HealthyChildItems, SvcError> {
    let children = scheduling::healthy_volume_replicas(
        &GetPersistedNexusChildren::new_create(spec, target_node),
        registry,
    )
    .await?;

    spec.trace(&format!(
        "Healthy volume nexus replicas for volume: {children:?}"
    ));

    if children.is_empty() {
        Err(SvcError::NoOnlineReplicas {
            id: spec.uuid_str(),
        })
    } else {
        Ok(children)
    }
}

/// Implementation of the ResourceSpecs which is retrieved from the ResourceSpecsLocked
/// During these calls, no other thread can add/remove elements from the list
impl ResourceSpecs {
    /// Gets all VolumeSpec's
    pub(crate) fn volumes(&self) -> Vec<VolumeSpec> {
        self.volumes.values().map(|v| v.lock().clone()).collect()
    }

    /// Gets all VolumeGroupSpec's
    pub(crate) fn volume_groups(&self) -> Vec<VolumeGroupSpec> {
        self.volume_groups
            .values()
            .map(|v| v.lock().clone())
            .collect()
    }

    /// Get a subset of the volumes based on the pagination argument.
    pub(crate) fn paginated_volumes(&self, pagination: &Pagination) -> PaginatedResult<VolumeSpec> {
        let num_volumes = self.volumes.len() as u64;
        let max_entries = pagination.max_entries();
        let offset = std::cmp::min(pagination.starting_token(), num_volumes);
        let mut last_result = false;
        let length = match offset + max_entries >= num_volumes {
            true => {
                last_result = true;
                num_volumes - offset
            }
            false => pagination.max_entries(),
        };

        PaginatedResult::new(self.volumes.paginate(offset, length), last_result)
    }
}
impl ResourceSpecsLocked {
    /// Get the resourced VolumeSpec for the given volume `id`, if any exists
    pub(crate) fn volume_rsc(&self, id: &VolumeId) -> Option<ResourceMutex<VolumeSpec>> {
        let specs = self.read();
        specs.volumes.get(id).cloned()
    }

    /// Get a copy of the VolumeSpec for the volume with the given ID.
    pub(crate) fn volume_clone(&self, id: &VolumeId) -> Result<VolumeSpec, SvcError> {
        match self.volume_rsc(id) {
            Some(locked_spec) => {
                let spec = locked_spec.lock();
                Ok(spec.clone())
            }
            None => Err(VolumeNotFound {
                vol_id: id.to_string(),
            }),
        }
    }
    /// Get a guarded VolumeSpec for the volume with the given ID.
    pub(crate) async fn volume(
        &self,
        id: &VolumeId,
    ) -> Result<OperationGuardArc<VolumeSpec>, SvcError> {
        match self.volume_rsc(id) {
            Some(spec) => spec.operation_guard_wait().await,
            None => Err(VolumeNotFound {
                vol_id: id.to_string(),
            }),
        }
    }

    /// Gets a copy of all VolumeSpec's
    pub(crate) fn volumes(&self) -> Vec<VolumeSpec> {
        let specs = self.read();
        specs.volumes()
    }

    /// Get a subset of volumes based on the pagination argument.
    pub(crate) fn paginated_volumes(&self, pagination: &Pagination) -> PaginatedResult<VolumeSpec> {
        let specs = self.read();
        specs.paginated_volumes(pagination)
    }

    /// Gets a copy of all locked VolumeSpec's
    pub(crate) fn volumes_rsc(&self) -> Vec<ResourceMutex<VolumeSpec>> {
        let specs = self.read();
        specs.volumes.to_vec()
    }

    /// Get a list of nodes currently used as replicas
    pub(crate) fn volume_data_nodes(&self, id: &VolumeId) -> Vec<NodeId> {
        let used_pools = self
            .read()
            .replicas
            .values()
            .filter(|r| r.lock().owners.owned_by(id))
            .map(|r| r.lock().pool.pool_name().clone())
            .collect::<Vec<_>>();
        self.read()
            .pools()
            .iter()
            .filter(|p| used_pools.iter().any(|up| up == &p.id))
            .map(|p| p.node.clone())
            .collect::<Vec<_>>()
    }

    /// Get a list of resourced ReplicaSpec's for the given volume `id`
    /// todo: we could also get the replicas from the volume nexuses?
    pub(crate) fn volume_replicas(&self, id: &VolumeId) -> Vec<ResourceMutex<ReplicaSpec>> {
        self.read()
            .replicas
            .values()
            .filter(|r| r.lock().owners.owned_by(id))
            .cloned()
            .collect()
    }

    /// Get a list of cloned volume replicas owned by the given volume `id`.
    pub(crate) fn volume_replicas_cln(&self, id: &VolumeId) -> Vec<ReplicaSpec> {
        self.read()
            .replicas
            .values()
            .filter_map(|r| {
                let r = r.lock();
                if r.owners.owned_by(id) {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get the `NodeId` where `replica` lives
    pub(crate) async fn replica_node(registry: &Registry, replica: &ReplicaSpec) -> Option<NodeId> {
        let pools = registry.pool_states_inner().await;
        pools.iter().find_map(|p| {
            if p.id == replica.pool.pool_name().clone() {
                Some(p.node.clone())
            } else {
                None
            }
        })
    }

    /// Get the `NodeId` where `pool` lives.
    pub(crate) async fn pool_node(registry: &Registry, pool: &PoolId) -> Option<NodeId> {
        let pools = registry.pool_states_inner().await;
        pools.iter().find_map(|p| {
            if &p.id == pool {
                Some(p.node.clone())
            } else {
                None
            }
        })
    }

    /// Get a list of resourced NexusSpecs's which are owned by the given volume `id`
    /// but may not be active anymore.
    /// This may happen if the connection to the persistent store is lost and we fail to
    /// update/delete the nexus spec and the control plane restarts.
    /// To get the current active volume nexus target use `get_volume_target_nexus`.
    pub(crate) fn volume_nexuses(&self, id: &VolumeId) -> Vec<ResourceMutex<NexusSpec>> {
        self.read()
            .nexuses
            .values()
            .filter(|n| n.lock().owner.as_ref() == Some(id))
            .cloned()
            .collect()
    }

    /// Get a list of resourced NexusSpecs's which are associated with the given volume `id`
    /// and are currently in shutdown state.
    pub(crate) async fn volume_shutdown_nexuses(
        &self,
        id: &VolumeId,
    ) -> Vec<ResourceMutex<NexusSpec>> {
        self.read()
            .nexuses
            .values()
            .filter(|nexus| {
                let nexus_spec = nexus.lock();
                nexus_spec.name == id.as_str() && nexus_spec.is_shutdown()
            })
            .cloned()
            .collect()
    }

    /// Get a list of resourced NexusSpecs's which are associated with the given volume `id`
    /// which have shutdown failed.
    pub(crate) async fn volume_failed_shutdown_nexuses(
        &self,
        id: &VolumeId,
    ) -> Vec<ResourceMutex<NexusSpec>> {
        self.read()
            .nexuses
            .values()
            .filter(|nexus| {
                let nexus_spec = nexus.lock();
                nexus_spec.name == id.as_str() && nexus_spec.status_info().shutdown_failed()
            })
            .cloned()
            .collect()
    }

    /// Get the resourced volume nexus target for the given volume
    pub(crate) fn volume_target_nexus_rsc(
        &self,
        volume: &VolumeSpec,
    ) -> Option<ResourceMutex<NexusSpec>> {
        match volume.target() {
            None => None,
            Some(target) => self.nexus_rsc(target.nexus()),
        }
    }
    /// Get the resourced volume nexus target for the given volume
    pub(crate) async fn volume_target_nexus(
        &self,
        volume: &VolumeSpec,
    ) -> Result<Option<OperationGuardArc<NexusSpec>>, SvcError> {
        Ok(match volume.target() {
            None => None,
            Some(target) => self.nexus_opt(target.nexus()).await?,
        })
    }

    /// Return a `DestroyReplica` request based on the provided arguments
    pub(crate) fn destroy_replica_request(
        spec: ReplicaSpec,
        by: ReplicaOwners,
        node: &NodeId,
    ) -> DestroyReplica {
        let pool_id = match spec.pool.clone() {
            PoolRef::Named(id) => id,
            PoolRef::Uuid(id, _) => id,
        };
        let pool_uuid = match spec.pool {
            PoolRef::Named(_) => None,
            PoolRef::Uuid(_, uuid) => Some(uuid),
        };
        DestroyReplica {
            node: node.clone(),
            pool_id,
            pool_uuid,
            uuid: spec.uuid,
            name: spec.name.into(),
            disowners: by,
        }
    }

    /// Delete the NexusInfo key from the persistent store.
    /// If deletion fails we just log it and continue.
    pub(crate) async fn delete_nexus_info(key: &NexusInfoKey, registry: &Registry) {
        let vol_id = match key.volume_id() {
            Some(v) => v.as_str(),
            None => "",
        };
        match registry.delete_kv(&key.key()).await {
            Ok(_) => {
                tracing::trace!(
                    volume.uuid = %vol_id,
                    nexus.uuid = %key.nexus_id(),
                    "Deleted NexusInfo entry from persistent store",
                );
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    volume.uuid = %vol_id,
                    nexus.uuid = %key.nexus_id(),
                    "Failed to delete NexusInfo entry from persistent store",
                );
            }
        }
    }

    /// Create a replica for the given volume using the provided list of candidates in order
    pub(crate) async fn create_volume_replica(
        &self,
        registry: &Registry,
        state: &VolumeState,
        candidates: &[CreateReplica],
    ) -> Result<Replica, SvcError> {
        let mut result = Err(SvcError::NotEnoughResources {
            source: NotEnough::OfReplicas { have: 0, need: 1 },
        });
        for attempt in candidates.iter() {
            let mut attempt = attempt.clone();

            if let Some(nexus) = &state.target {
                if nexus.node == attempt.node {
                    attempt.share = Protocol::None;
                }
            }

            result = OperationGuardArc::<ReplicaSpec>::create(registry, &attempt).await;
            if result.is_ok() {
                break;
            }
        }
        result
    }

    /// Create `count` replicas for the given volume using the provided
    /// list of candidates, in order.
    pub(crate) async fn create_volume_replicas(
        &self,
        registry: &Registry,
        volume_spec: &VolumeSpec,
        count: usize,
    ) -> Result<Vec<ReplicaId>, SvcError> {
        let mut created_replicas = Vec::with_capacity(count);
        let mut candidate_error = None;

        for iter in 0 .. count {
            let candidates = match volume_replica_candidates(registry, volume_spec).await {
                Ok(candidates) => candidates,
                Err(error) => {
                    candidate_error = Some(error);
                    break;
                }
            };

            for attempt in candidates.into_iter() {
                match OperationGuardArc::<ReplicaSpec>::create(registry, &attempt).await {
                    Ok(replica) => {
                        volume_spec
                            .debug(&format!("Successfully created replica '{}'", replica.uuid));
                        created_replicas.push(replica.uuid);
                        break;
                    }
                    Err(error) => {
                        volume_spec.error(&format!(
                            "Failed to create replica '{:?}', error: '{}'",
                            attempt,
                            error.full_string(),
                        ));
                    }
                }
            }

            if created_replicas.len() <= iter {
                break;
            }
        }

        if created_replicas.is_empty() {
            if let Some(error) = candidate_error {
                return Err(error);
            }
        }
        Ok(created_replicas)
    }

    /// Add the given replica to the nexus of the given volume
    async fn add_replica_to_volume(
        &self,
        registry: &Registry,
        status: &VolumeState,
        replica: Replica,
    ) -> Result<(), SvcError> {
        if let Some(nexus) = &status.target {
            let mut nexus_guard = self.nexus(&nexus.uuid).await?;
            self.attach_replica_to_nexus(registry, &mut nexus_guard, &status.uuid, nexus, &replica)
                .await
        } else {
            Ok(())
        }
    }

    /// Increase the replica count of the given volume by 1
    /// Creates a new data replica from a list of candidates
    /// Adds the replica to the volume nexuses (if any)
    pub(crate) async fn increase_volume_replica(
        &self,
        volume: &mut OperationGuardArc<VolumeSpec>,
        registry: &Registry,
        state: VolumeState,
        spec_clone: VolumeSpec,
    ) -> Result<Volume, SvcError> {
        // Prepare a list of candidates (based on some criteria)
        let result = volume_replica_candidates(registry, &spec_clone).await;
        let candidates = volume
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        // Create the data replica from the pool candidates
        let result = self
            .create_volume_replica(registry, &state, &candidates)
            .await;
        let replica = volume
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        // Add the newly created replica to the nexus, if it's up
        let result = self.add_replica_to_volume(registry, &state, replica).await;
        volume.complete_update(registry, result, spec_clone).await?;

        registry.volume(&state.uuid).await
    }

    /// Remove a replica from all nexuses for the given volume
    /// Only volumes with 1 nexus are currently supported
    pub(crate) async fn remove_volume_child_candidate(
        &self,
        spec_clone: &VolumeSpec,
        registry: &Registry,
        remove: &ReplicaItem,
    ) -> Result<(), SvcError> {
        // if the nexus is up, first remove the child from the nexus before deleting the replica
        let mut nexus = self.volume_target_nexus(spec_clone).await?;
        let nexus = nexus.iter_mut().find_map(|n| {
            let found = n
                .lock()
                .children
                .iter()
                .flat_map(|c| c.as_replica())
                .find(|r| r.uuid() == &remove.spec().uuid);
            found.map(|r| (n, r))
        });
        match nexus {
            None => Ok(()),
            Some((nexus, replica)) => {
                let nexus_spec = nexus.lock().clone();
                self.remove_nexus_replica(
                    Some(nexus),
                    registry,
                    &RemoveNexusReplica {
                        node: nexus_spec.node,
                        nexus: nexus_spec.uuid,
                        replica: ReplicaUri::new(&remove.spec().uuid, replica.uri()),
                    },
                )
                .await
            }
        }
    }

    /// Decrement the replica count of the given volume by 1
    /// Removes the replica from all volume nexuses
    pub(crate) async fn decrease_volume_replica(
        &self,
        volume: &mut OperationGuardArc<VolumeSpec>,
        registry: &Registry,
        state: VolumeState,
        spec_clone: VolumeSpec,
    ) -> Result<Volume, SvcError> {
        // Determine which replica is most suitable to be removed
        let result = volume_replica_remove_candidate(&spec_clone, &state, registry).await;

        if let Err(ReplicaRemovalNoCandidates { .. }) = result {
            // The desired number of replicas is already met. This can occur if a replica has been
            // removed from the volume due to an error.
            volume.complete_update(registry, Ok(()), spec_clone).await?;
        } else {
            // Can fail if meanwhile the state of a replica/nexus/child changes, so fail gracefully
            let remove = volume
                .validate_update_step(registry, result, &spec_clone)
                .await?;

            // Remove the replica from its nexus (where it exists as a child)
            let result = self
                .remove_volume_child_candidate(&spec_clone, registry, &remove)
                .await;
            volume
                .validate_update_step(registry, result, &spec_clone)
                .await?;

            // todo: we could ignore it here, since we've already removed it from the nexus
            // now remove the replica from the pool
            let result = self
                .destroy_replica_spec(
                    registry,
                    remove.spec(),
                    ReplicaOwners::from_volume(&state.uuid),
                )
                .await;

            volume.complete_update(registry, result, spec_clone).await?;
        }

        registry.volume(&state.uuid).await
    }

    /// Make the replica accessible on the specified `NodeId`
    /// This means the replica might have to be shared/unshared so it can be open through
    /// the correct protocol (loopback locally, and nvmf remotely)
    pub(crate) async fn make_replica_accessible(
        &self,
        registry: &Registry,
        replica_state: &Replica,
        nexus_node: &NodeId,
    ) -> Result<ChildUri, SvcError> {
        if nexus_node == &replica_state.node {
            // on the same node, so connect via the loopback bdev
            let mut replica = self.replica(&replica_state.uuid).await?;
            match replica.unshare(registry, &replica_state.into()).await {
                Ok(uri) => Ok(uri.into()),
                Err(SvcError::NotShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        } else {
            // on a different node, so connect via an nvmf target
            let mut replica = self.replica(&replica_state.uuid).await?;
            let allowed_hosts = registry.node_nqn(nexus_node).await?;
            let request = ShareReplica::from(replica_state).with_hosts(allowed_hosts);
            match replica.share(registry, &request).await {
                Ok(uri) => Ok(ChildUri::from(uri)),
                Err(SvcError::AlreadyShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        }
    }

    /// Create a nexus for the given volume on the specified target_node
    /// Existing replicas may be shared/unshared so we can connect to them
    pub(crate) async fn volume_create_nexus(
        &self,
        registry: &Registry,
        target_config: &TargetConfig,
        vol_spec: &VolumeSpec,
    ) -> Result<(OperationGuardArc<NexusSpec>, Nexus), SvcError> {
        let target_node = target_config.target().node();
        let nexus_id = target_config.target().nexus();
        let children = healthy_volume_replicas(vol_spec, target_node, registry).await?;
        let (count, items) = match children {
            HealthyChildItems::One(_, candidates) => (1, candidates),
            HealthyChildItems::All(_, candidates) => (candidates.len(), candidates),
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
                .make_replica_accessible(registry, item.state(), target_node)
                .await
            {
                nexus_replicas.push(NexusChild::Replica(ReplicaUri::new(
                    &item.spec().uuid,
                    &uri,
                )));
                nodes.push(item.state().node.clone());
            }
        }
        nexus_replicas.truncate(vol_spec.num_replicas as usize);

        // Create the nexus on the requested node
        let (guard, nexus) = OperationGuardArc::<NexusSpec>::create(
            registry,
            &CreateNexus::new(
                target_node,
                nexus_id,
                vol_spec.size,
                &nexus_replicas,
                true,
                Some(&vol_spec.uuid),
                Some(target_config.config().clone()),
            ),
        )
        .await?;

        if nexus.children.len() < vol_spec.num_replicas as usize {
            vol_spec.warn_span(|| {
                tracing::warn!(
                    "Recreated volume target with only {} out of {} desired replicas",
                    nexus.children.len(),
                    vol_spec.num_replicas
                )
            });
        }

        Ok((guard, nexus))
    }

    /// Attach the specified replica to the volume nexus
    /// The replica might need to be shared/unshared so it can be opened by the nexus
    pub(crate) async fn attach_replica_to_nexus(
        &self,
        registry: &Registry,
        nexus_guard: &mut OperationGuardArc<NexusSpec>,
        volume_uuid: &VolumeId,
        nexus: &Nexus,
        replica: &Replica,
    ) -> Result<(), SvcError> {
        // Adding a replica to a nexus will initiate a rebuild.
        // First check that we are able to start a rebuild.
        registry.rebuild_allowed().await?;

        let uri = self
            .make_replica_accessible(registry, replica, &nexus.node)
            .await?;
        match self
            .add_nexus_replica(
                Some(nexus_guard),
                registry,
                &AddNexusReplica {
                    node: nexus.node.clone(),
                    nexus: nexus.uuid.clone(),
                    replica: ReplicaUri::new(&replica.uuid, &uri),
                    auto_rebuild: true,
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                if let Some(replica) = self.replica_rsc(&replica.uuid) {
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
        volume: &mut OperationGuardArc<VolumeSpec>,
        mut count: usize,
    ) -> Result<(), SvcError> {
        let state = registry.volume_state(volume.uuid()).await?;

        let mut candidates =
            volume_unused_replica_remove_candidates(volume.as_ref(), &state, registry).await?;

        let mut result = Ok(());
        while let Some(replica) = candidates.next() {
            if count == 0 {
                break;
            }
            match self
                .remove_unused_volume_replica(registry, volume, &replica.spec().uuid)
                .await
            {
                Ok(_) => {
                    volume.info(&format!(
                        "Successfully removed unused replica '{}'",
                        replica.spec().uuid
                    ));
                    count -= 1;
                }
                Err(error) => {
                    volume.warn_span(|| {
                        tracing::warn!(
                            "Failed to remove unused replicas, error: {}",
                            error.full_string()
                        )
                    });
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
        volume: &mut OperationGuardArc<VolumeSpec>,
        replica_id: &ReplicaId,
    ) -> Result<(), SvcError> {
        let state = registry.volume_state(volume.uuid()).await?;
        let spec_clone = volume
            .start_update(
                registry,
                &state,
                VolumeOperation::RemoveUnusedReplica(replica_id.clone()),
            )
            .await?;

        // The replica is unused, so we can disown it...
        let replica = self.replica(replica_id).await;
        let mut replica = volume
            .validate_update_step(registry, replica, &spec_clone)
            .await?;

        // disown it from the volume first, so at the very least it can be garbage collected
        // at a later point if the node is not accessible
        let disowner = ReplicaOwners::from_volume(volume.uuid());
        let result = replica.remove_owners(registry, &disowner, true).await;
        volume
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        // the garbage collector will destroy it at a later time
        if let Err(error) = self
            .destroy_volume_replica(registry, None, &mut replica)
            .await
        {
            spec_clone.error_span(|| {
                tracing::error!(
                    "Failed to destroy replica '{}'. Error: '{}'. It will be garbage collected.",
                    replica_id,
                    error.full_string()
                )
            });
        }
        volume.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(())
    }

    /// Attach existing replicas to the given volume nexus until it reaches the required number of
    /// data replicas.
    /// Returns the first encountered error, but tries to add as many as it can until it does so.
    pub(crate) async fn attach_replicas_to_nexus(
        &self,
        registry: &Registry,
        volume: &mut OperationGuardArc<VolumeSpec>,
        nexus: &mut OperationGuardArc<NexusSpec>,
        nexus_state: &Nexus,
    ) -> Result<(), SvcError> {
        let vol_spec_clone = volume.lock().clone();
        let vol_uuid = vol_spec_clone.uuid.clone();
        let nexus_spec_clone = nexus.lock().clone();
        let volume_children = self
            .volume_replicas(&vol_spec_clone.uuid)
            .len()
            .min(vol_spec_clone.num_replicas as usize);
        let mut nexus_children = nexus_spec_clone.children.len();

        let replicas =
            nexus_attach_candidates(&vol_spec_clone, &nexus_spec_clone, registry).await?;

        let mut result = Ok(());
        for replica in replicas {
            if nexus_children >= volume_children {
                break;
            }
            match self
                .attach_replica_to_nexus(registry, nexus, &vol_uuid, nexus_state, replica.state())
                .await
            {
                Ok(_) => {
                    nexus_spec_clone.info(&format!(
                        "Successfully attached replica '{}' to nexus",
                        replica.state().uuid,
                    ));
                    nexus_children += 1;
                }
                Err(error) => {
                    nexus_spec_clone.error(&format!(
                        "Failed to attach replica '{}' to nexus, error: '{}'",
                        replica.state().uuid,
                        error.full_string()
                    ));
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
        volume: &mut OperationGuardArc<VolumeSpec>,
        nexus: &mut OperationGuardArc<NexusSpec>,
        nexus_state: &Nexus,
    ) -> Result<(), SvcError> {
        let vol_spec_clone = volume.lock().clone();
        let nexus_spec_clone = nexus.lock().clone();
        let volume_children = vol_spec_clone.num_replicas as usize;
        let mut nexus_replica_children =
            nexus_spec_clone
                .children
                .iter()
                .fold(0usize, |mut c, child| {
                    if let Some(replica) = child.as_replica() {
                        if registry.specs().replica_rsc(replica.uuid()).is_some() {
                            c += 1;
                        }
                    }
                    c
                });

        let mut candidates =
            nexus_child_remove_candidates(&vol_spec_clone, &nexus_spec_clone, registry).await?;

        while let Some(candidate) = candidates.next() {
            if nexus_replica_children <= volume_children {
                break;
            }
            let child_uri = match candidate.child_state() {
                None => break,
                Some(child) => child.uri.clone(),
            };
            match self
                .remove_nexus_child_by_uri(registry, nexus, nexus_state, &child_uri, true)
                .await
            {
                Ok(_) => {
                    nexus_spec_clone.info(&format!(
                        "Successfully removed child '{child_uri}' from nexus",
                    ));
                    nexus_replica_children -= 1;
                }
                Err(error) => {
                    nexus_spec_clone.error(&format!(
                        "Failed to remove child '{}' from nexus, error: '{}'",
                        child_uri,
                        error.full_string()
                    ));
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Disown nexus from its owner
    /// fixme: not explicitly guarded
    pub(crate) async fn disown_nexus(
        &self,
        registry: &Registry,
        nexus: &ResourceMutex<NexusSpec>,
    ) -> Result<(), SvcError> {
        nexus.lock().disowned_by_volume();
        let clone = nexus.lock().clone();
        registry.store_obj(&clone).await
    }

    /// Destroy the replica from its volume
    pub(crate) async fn destroy_volume_replica(
        &self,
        registry: &Registry,
        node_id: Option<&NodeId>,
        replica: &mut OperationGuardArc<ReplicaSpec>,
    ) -> Result<(), SvcError> {
        let node_id = match node_id {
            Some(node_id) => node_id.clone(),
            None => {
                let replica_uuid = replica.lock().uuid.clone();
                match registry.get_replica(&replica_uuid).await {
                    Ok(state) => state.node.clone(),
                    Err(_) => {
                        let pool_ref = replica.lock().pool.clone();
                        let pool_id = match pool_ref {
                            PoolRef::Named(name) => name,
                            PoolRef::Uuid(name, _) => name,
                        };
                        let pool_spec = self
                            .pool_rsc(&pool_id)
                            .context(errors::PoolNotFound { pool_id })?;
                        let node_id = pool_spec.lock().node.clone();
                        node_id
                    }
                }
            }
        };

        let spec = replica.lock().clone();
        replica
            .destroy(
                registry,
                &Self::destroy_replica_request(spec, ReplicaOwners::new_disown_all(), &node_id),
            )
            .await
    }

    /// Disown and destroy the replica from its volume
    pub(crate) async fn disown_and_destroy_replica(
        &self,
        registry: &Registry,
        node: &NodeId,
        replica: &mut OperationGuardArc<ReplicaSpec>,
    ) -> Result<(), SvcError> {
        // disown it from the volume first, so at the very least it can be garbage collected
        // at a later point if the node is not accessible
        let disowner = ReplicaOwners::new_disown_all();
        replica.remove_owners(registry, &disowner, true).await?;
        self.destroy_volume_replica(registry, Some(node), replica)
            .await
    }

    /// Remove volume by its `id`
    pub(super) fn remove_volume(&self, id: &VolumeId) {
        let mut specs = self.write();
        specs.volumes.remove(id);
    }

    /// Get or Create the resourced VolumeSpec for the given request
    pub(crate) fn get_or_create_volume(&self, request: &CreateVolume) -> ResourceMutex<VolumeSpec> {
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
    pub(crate) async fn reconcile_dirty_volumes(&self, registry: &Registry) -> bool {
        let mut pending_ops = false;

        let volumes = self.volumes_rsc();
        for volume_spec in volumes {
            if let Ok(mut guard) = volume_spec.operation_guard() {
                if !guard.handle_incomplete_ops(registry).await {
                    // Not all pending operations could be handled.
                    pending_ops = true;
                }
            }
        }
        pending_ops
    }
}

pub(crate) async fn get_volume_target_node(
    registry: &Registry,
    status: &VolumeState,
    request: &impl PublishVolumeInfo,
    republish: bool,
) -> Result<NodeId, SvcError> {
    if !republish {
        // We can't configure a new target_node if the volume is currently published
        if let Some(nexus) = &status.target {
            return Err(SvcError::VolumeAlreadyPublished {
                vol_id: status.uuid.to_string(),
                node: nexus.node.to_string(),
                protocol: nexus.share.to_string(),
            });
        }
    }

    match request.target_node().as_ref() {
        None => {
            // in case there is no target node specified, let the control-plane scheduling logic
            // determine a suitable node for the same.
            let volume_spec = registry.specs().volume_clone(&status.uuid)?;
            let candidate = target_node_candidate(&volume_spec, registry).await?;
            tracing::debug!(node.id=%candidate.id(), "Node selected for volume publish by the core-agent");
            Ok(candidate.id().clone())
        }
        Some(node) => {
            // make sure the requested node is available
            // todo: check the max number of nexuses per node is respected
            let node = registry.node_wrapper(node).await?;
            let node = node.read().await;
            if node.is_online() {
                Ok(node.id().clone())
            } else {
                Err(SvcError::NodeNotOnline {
                    node: node.id().clone(),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl GuardedOperationsHelper for OperationGuardArc<VolumeSpec> {
    type Create = CreateVolume;
    type Owners = ();
    type Status = VolumeStatus;
    type State = VolumeState;
    type UpdateOp = VolumeOperation;
    type Inner = VolumeSpec;

    fn remove_spec(&self, registry: &Registry) {
        let uuid = self.lock().uuid.clone();
        registry.specs().remove_volume(&uuid);
    }
}

#[async_trait::async_trait]
impl SpecOperationsHelper for VolumeSpec {
    type Create = CreateVolume;
    type Owners = ();
    type Status = VolumeStatus;
    type State = VolumeState;
    type UpdateOp = VolumeOperation;

    async fn start_update_op(
        &mut self,
        registry: &Registry,
        state: &Self::State,
        operation: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        if !matches!(
            &operation,
            VolumeOperation::Publish(..)
                | VolumeOperation::Unpublish
                | VolumeOperation::Republish(..)
        ) {
            // don't attempt to modify the volume parameters if the nexus target is not "stable"
            if self.target().is_some() != state.target.is_some() {
                return Err(SvcError::NotReady {
                    kind: self.kind(),
                    id: self.uuid_str(),
                });
            }
        }

        match &operation {
            VolumeOperation::Share(protocol) => match protocol {
                VolumeShareProtocol::Nvmf => match &self.target() {
                    None => Err(SvcError::VolumeNotPublished {
                        vol_id: self.uuid_str(),
                    }),
                    Some(target) => match target.protocol() {
                        None => Ok(()),
                        Some(protocol) => Err(SvcError::AlreadyShared {
                            kind: self.kind(),
                            id: self.uuid_str(),
                            share: protocol.to_string(),
                        }),
                    },
                },
                VolumeShareProtocol::Iscsi => Err(SvcError::InvalidShareProtocol {
                    kind: ResourceKind::Volume,
                    id: self.uuid_str(),
                    share: format!("{protocol:?}"),
                }),
            },
            VolumeOperation::Unshare => match self.target() {
                None => Err(SvcError::NotShared {
                    kind: self.kind(),
                    id: self.uuid_str(),
                }),
                Some(target) if target.protocol().is_none() => Err(SvcError::NotShared {
                    kind: self.kind(),
                    id: self.uuid_str(),
                }),
                _ => Ok(()),
            },
            VolumeOperation::PublishOld(_) => Err(SvcError::InvalidArguments {}),
            VolumeOperation::Publish(args) => match args.protocol() {
                None => Ok(()),
                Some(protocol) => match protocol {
                    VolumeShareProtocol::Nvmf => {
                        if let Some(target) = self.target() {
                            Err(SvcError::VolumeAlreadyPublished {
                                vol_id: self.uuid_str(),
                                node: target.node().to_string(),
                                protocol: format!("{:?}", target.protocol()),
                            })
                        } else {
                            self.publish_context = Some(args.publish_context());
                            Ok(())
                        }
                    }
                    VolumeShareProtocol::Iscsi => Err(SvcError::InvalidShareProtocol {
                        kind: ResourceKind::Volume,
                        id: self.uuid_str(),
                        share: format!("{:?}", args.protocol()),
                    }),
                },
            },
            VolumeOperation::Republish(args) => match args.protocol() {
                VolumeShareProtocol::Nvmf => Ok(()),
                VolumeShareProtocol::Iscsi => Err(SvcError::InvalidShareProtocol {
                    kind: ResourceKind::Volume,
                    id: self.uuid_str(),
                    share: format!("{:?}", args.protocol()),
                }),
            },
            VolumeOperation::Unpublish if self.target().is_none() => {
                Err(SvcError::VolumeNotPublished {
                    vol_id: self.uuid_str(),
                })
            }
            VolumeOperation::Unpublish => {
                self.publish_context = None;
                Ok(())
            }

            VolumeOperation::SetReplica(replica_count) => {
                if *replica_count == self.num_replicas {
                    Err(SvcError::ReplicaCountAchieved {
                        id: self.uuid_str(),
                        count: self.num_replicas,
                    })
                } else if *replica_count < 1 {
                    Err(SvcError::LastReplica {
                        replica: "".to_string(),
                        volume: self.uuid_str(),
                    })
                } else if (*replica_count as i16 - self.num_replicas as i16).abs() > 1 {
                    Err(SvcError::ReplicaChangeCount {})
                } else if state.status != VolumeStatus::Online
                    && (*replica_count > self.num_replicas)
                {
                    Err(SvcError::ReplicaIncrease {
                        volume_id: self.uuid_str(),
                        volume_state: state.status.to_string(),
                    })
                } else {
                    Ok(())
                }
            }

            VolumeOperation::RemoveUnusedReplica(uuid) => {
                let last_replica = !registry
                    .specs()
                    .volume_replicas(&self.uuid)
                    .iter()
                    .any(|r| &r.lock().uuid != uuid);
                let nexus = registry.specs().volume_target_nexus_rsc(self);
                let used = nexus.map(|n| n.lock().contains_replica(uuid));
                if last_replica {
                    Err(SvcError::LastReplica {
                        replica: uuid.to_string(),
                        volume: self.uuid_str(),
                    })
                } else if used.unwrap_or_default() {
                    Err(SvcError::InUse {
                        kind: ResourceKind::Replica,
                        id: uuid.to_string(),
                    })
                } else {
                    match registry
                        .nexus_info(Some(&self.uuid), self.health_info_id(), true)
                        .await?
                    {
                        Some(info) => match info
                            .children
                            .iter()
                            .find(|i| i.uuid.as_str() == uuid.as_str())
                        {
                            Some(replica_info)
                                if replica_info.healthy
                                    && !info
                                        .children
                                        .iter()
                                        .filter(|i| i.uuid.as_str() != uuid.as_str())
                                        .any(|i| i.healthy) =>
                            {
                                // if there are no other healthy replicas, then we cannot remove
                                // this replica!
                                Err(SvcError::LastHealthyReplica {
                                    replica: uuid.to_string(),
                                    volume: self.uuid_str(),
                                })
                            }
                            _ => Ok(()),
                        },
                        None => Ok(()),
                    }
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
    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Volume
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
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}
