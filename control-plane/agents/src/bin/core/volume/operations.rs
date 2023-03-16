use crate::{
    controller::{
        reconciler::PollTriggerEvent,
        registry::Registry,
        resources::{
            operations::{
                ResourceLifecycle, ResourceOwnerUpdate, ResourcePublishing, ResourceReplicas,
                ResourceSharing, ResourceShutdownOperations,
            },
            operations_helper::{
                GuardedOperationsHelper, OnCreateFail, OperationSequenceGuard, ResourceSpecsLocked,
                SpecOperationsHelper,
            },
            OperationGuardArc, TraceSpan, TraceStrLog,
        },
        scheduling::pool::ENoSpcReplica,
    },
    volume::specs::{
        create_volume_replicas, get_volume_target_node, healthy_volume_replicas,
        volume_move_replica_candidates,
    },
};
use agents::errors::SvcError;
use http::Uri;
use std::ops::Deref;
use stor_port::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            nexus::NexusSpec,
            nexus_persistence::NexusInfoKey,
            replica::ReplicaSpec,
            volume::{
                FrontendConfig, PublishOperation, RepublishOperation, TargetConfig,
                VolumeOperation, VolumeSpec, VolumeTarget,
            },
        },
        transport::{
            CreateVolume, DestroyNexus, DestroyReplica, DestroyShutdownTargets, DestroyVolume,
            Nexus, NexusId, NexusNvmePreemption, NexusNvmfConfig, NodeId, NvmeReservation,
            NvmfControllerIdRange, Protocol, PublishVolume, Replica, ReplicaId, ReplicaOwners,
            RepublishVolume, SetVolumeReplica, ShareNexus, ShareVolume, ShutdownNexus,
            UnpublishVolume, UnshareNexus, UnshareVolume, Volume, VolumeShareProtocol,
        },
    },
    HostAccessControl,
};
use tracing::info;

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<VolumeSpec> {
    type Create = CreateVolume;
    type CreateOutput = Self;
    type Destroy = DestroyVolume;

    async fn create(
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let specs = registry.specs();
        let volume = specs
            .get_or_create_volume(request)
            .operation_guard_wait()
            .await?;
        let volume_clone = volume.start_create(registry, request).await?;

        // todo: pick nodes and pools using the Node&Pool Topology
        // todo: virtually increase the pool usage to avoid a race for space with concurrent calls
        let result = create_volume_replicas(registry, request, &volume_clone).await;
        let create_replicas = volume
            .validate_create_step_ext(registry, result, OnCreateFail::Delete)
            .await?;

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
            match OperationGuardArc::<ReplicaSpec>::create(registry, &replica).await {
                Ok(replica) => {
                    replicas.push(replica);
                }
                Err(error) => {
                    volume_clone.error(&format!(
                        "Failed to create replica {:?} for volume, error: {}",
                        replica,
                        error.full_string()
                    ));
                    // continue trying...
                }
            };
        }

        // we can't fulfil the required replication factor, so let the caller
        // decide what to do next
        let result = if replicas.len() < request.replicas as usize {
            for replica_state in replicas {
                let result = match specs.replica(&replica_state.uuid).await {
                    Ok(mut replica) => {
                        let request = DestroyReplica::from(replica_state.clone());
                        replica.destroy(registry, &request.with_disown_all()).await
                    }
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    volume_clone.error(&format!(
                        "Failed to delete replica {:?} from volume, error: {}",
                        replica_state,
                        error.full_string()
                    ));
                }
            }
            Err(SvcError::ReplicaCreateNumber {
                id: request.uuid.to_string(),
            })
        } else {
            Ok(())
        };

        // we can destroy volume on error because there's no volume resource created on the nodes,
        // only sub-resources (such as nexuses/replicas which will be garbage-collected later).
        volume
            .complete_create(result, registry, OnCreateFail::Delete)
            .await?;
        Ok(volume)
    }

    /// Destroy a volume based on the given `DestroyVolume` request.
    /// Volume destruction will succeed even if the nexus or replicas cannot be destroyed (i.e. due
    /// to an inaccessible node). In this case the resources will be destroyed by the garbage
    /// collector at a later time.
    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        let specs = registry.specs();
        self.start_destroy(registry).await?;

        let nexuses = specs.volume_nexuses(&request.uuid);
        for nexus_arc in nexuses {
            let nexus = nexus_arc.lock().deref().clone();
            match nexus_arc.operation_guard_wait().await {
                Ok(mut guard) => {
                    let destroy = DestroyNexus::from(&nexus).with_disown(&request.uuid);
                    if let Err(error) = guard.destroy(registry, &destroy).await {
                        nexus.warn_span(|| {
                            tracing::warn!(
                                error=%error,
                                "Nexus destruction failed. It will be garbage collected later."
                            )
                        });
                    }

                    // Delete the NexusInfo entry persisted by the IoEngine.
                    ResourceSpecsLocked::delete_nexus_info(
                        &NexusInfoKey::new(&Some(request.uuid.clone()), &nexus.uuid),
                        registry,
                    )
                    .await;
                }
                Err(error) => {
                    nexus.warn_span(|| {
                        tracing::warn!(error=%error,
                            "Nexus was busy. It will be garbage collected later."
                        )
                    });
                }
            }
        }

        let replicas = specs.volume_replicas(&request.uuid);
        for replica in replicas {
            let mut replica = match replica.operation_guard_wait().await {
                Ok(replica) => replica,
                Err(_) => continue,
            };
            if let Some(node) = ResourceSpecsLocked::replica_node(registry, replica.as_ref()).await
            {
                let replica_spec = replica.as_ref().clone();
                let result = replica
                    .destroy(
                        registry,
                        &ResourceSpecsLocked::destroy_replica_request(
                            replica_spec,
                            ReplicaOwners::new_disown_all(),
                            &node,
                        ),
                    )
                    .await;
                if let Err(error) = result {
                    tracing::warn!(replica.uuid=%replica.uuid(), error=%error,
                        "Replica destruction failed. This will be garbage collected later"
                    );
                }
            } else {
                // The above is able to handle when a pool is moved to a different node but if a
                // pool is unplugged we should disown the replica and allow the garbage
                // collector to destroy it later.
                tracing::warn!(replica.uuid=%replica.uuid(),"Replica node not found");
                let disowner = ReplicaOwners::from_volume(self.uuid());
                if let Err(error) = replica.remove_owners(registry, &disowner, true).await {
                    tracing::error!(replica.uuid=%replica.uuid(), error=%error, "Failed to disown volume replica");
                }
            }
        }

        self.complete_destroy(Ok(()), registry).await
    }
}

#[async_trait::async_trait]
impl ResourceSharing for OperationGuardArc<VolumeSpec> {
    type Share = ShareVolume;
    type Unshare = UnshareVolume;
    type ShareOutput = String;
    type UnshareOutput = ();

    async fn share(
        &mut self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<String, SvcError> {
        let specs = registry.specs();
        let state = registry.volume_state(&request.uuid).await?;

        let spec_clone = self
            .start_update(registry, &state, VolumeOperation::Share(request.protocol))
            .await?;

        let target = state.target.expect("already validated");
        let result = match specs.nexus(&target.uuid).await {
            Ok(mut nexus) => {
                nexus
                    .share(
                        registry,
                        &ShareNexus::new(
                            &target,
                            request.protocol,
                            request
                                .frontend_hosts
                                .clone()
                                .into_iter()
                                .map(TryInto::try_into)
                                .collect::<Result<_, _>>()?,
                        ),
                    )
                    .await
            }
            Err(error) => Err(error),
        };

        self.complete_update(registry, result, spec_clone).await
    }

    async fn unshare(
        &mut self,
        registry: &Registry,
        request: &Self::Unshare,
    ) -> Result<Self::UnshareOutput, SvcError> {
        let specs = registry.specs();
        let state = registry.volume_state(&request.uuid).await?;

        let spec_clone = self
            .start_update(registry, &state, VolumeOperation::Unshare)
            .await?;

        let target = state.target.expect("Already validated");
        let result = match specs.nexus(&target.uuid).await {
            Ok(mut nexus) => nexus.unshare(registry, &UnshareNexus::from(&target)).await,
            Err(error) => Err(error),
        };

        self.complete_update(registry, result, spec_clone).await
    }
}

#[async_trait::async_trait]
impl ResourcePublishing for OperationGuardArc<VolumeSpec> {
    type Publish = PublishVolume;
    type PublishOutput = Volume;
    type Unpublish = UnpublishVolume;
    type Republish = RepublishVolume;

    async fn publish(
        &mut self,
        registry: &Registry,
        request: &Self::Publish,
    ) -> Result<Self::PublishOutput, SvcError> {
        let specs = registry.specs();
        let state = registry.volume_state(&request.uuid).await?;
        let nexus_node = get_volume_target_node(registry, &state, request, false).await?;

        let last_target = self.as_ref().health_info_id().cloned();
        let frontend_nodes = &request.frontend_nodes;
        let target_cfg = self
            .next_target_config(registry, &nexus_node, &request.share, frontend_nodes)
            .await;

        let operation = VolumeOperation::Publish(PublishOperation::new(
            target_cfg.clone(),
            request.publish_context.clone(),
        ));
        let spec_clone = self.start_update(registry, &state, operation).await?;

        // Create a Nexus on the requested or auto-selected node.
        let result = specs
            .volume_create_nexus(registry, &target_cfg, &spec_clone)
            .await;

        let (mut nexus, nexus_state) = self
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        // Share the Nexus if it was requested.
        let mut result = Ok(());
        if let Some(share) = request.share {
            let allowed_hosts = target_cfg.frontend().node_nqns();
            result = match nexus
                .share(
                    registry,
                    &ShareNexus::new(&nexus_state, share, allowed_hosts),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => {
                    // Since we failed to share, we'll revert back to the previous state.
                    // If we fail to do this inline, the reconcilers will pick up the slack.
                    nexus
                        .destroy(registry, &DestroyNexus::from(nexus_state).with_disown_all())
                        .await
                        .ok();
                    Err(error)
                }
            }
        }

        self.complete_update(registry, result, spec_clone).await?;

        // If there was a previous nexus we should delete the persisted NexusInfo structure.
        if let Some(nexus_id) = last_target {
            ResourceSpecsLocked::delete_nexus_info(
                &NexusInfoKey::new(&Some(self.uuid().clone()), &nexus_id),
                registry,
            )
            .await;
        }

        let volume = registry.volume(&request.uuid).await?;
        registry
            .notify_if_degraded(&volume, PollTriggerEvent::VolumeDegraded)
            .await;
        Ok(volume)
    }

    async fn unpublish(
        &mut self,
        registry: &Registry,
        request: &Self::Unpublish,
    ) -> Result<(), SvcError> {
        let specs = registry.specs();

        let state = registry.volume_state(&request.uuid).await?;

        let spec_clone = self
            .start_update(registry, &state, VolumeOperation::Unpublish)
            .await?;

        let volume_target = spec_clone.target().expect("already validated");
        let result = match specs.nexus_opt(volume_target.nexus()).await? {
            None => Ok(()),
            Some(mut nexus) => {
                let nexus_clone = nexus.lock().clone();
                let destroy = DestroyNexus::from(&nexus_clone).with_disown(&request.uuid);
                // Destroy the Nexus
                match nexus.destroy(registry, &destroy).await {
                    Ok(_) => Ok(()),
                    Err(error) if !request.force() => Err(error),
                    Err(error) => {
                        let node_online = match registry.node_wrapper(&nexus_clone.node).await {
                            Ok(node) => {
                                let mut node = node.write().await;
                                node.is_online() && node.liveness_probe().await.is_ok()
                            }
                            _ => false,
                        };
                        if !node_online {
                            nexus_clone.warn_span(|| {
                                tracing::warn!("Force unpublish. Forgetting about the target nexus because the node is not online and it was requested");
                            });
                            Ok(())
                        } else {
                            Err(error)
                        }
                    }
                }
            }
        };

        self.complete_update(registry, result, spec_clone).await
    }

    async fn republish(
        &mut self,
        registry: &Registry,
        request: &Self::Republish,
    ) -> Result<Self::PublishOutput, SvcError> {
        let specs = registry.specs();
        let spec = self.as_ref().clone();
        let state = registry.volume_state(&request.uuid).await?;
        // If the volume is not published then it should issue publish call rather than republish.
        let target_cfg = match spec.active_config() {
            Some(cfg)
                if !cfg
                    .frontend()
                    .nodename_allowed(request.frontend_node.as_str()) =>
            {
                Err(SvcError::FrontendNodeNotAllowed {
                    node: request.frontend_node.to_string(),
                    vol_id: request.uuid.to_string(),
                })
            }
            Some(config) => Ok(config),
            None => Err(SvcError::VolumeNotPublished {
                vol_id: request.uuid.to_string(),
            }),
        }?;

        let mut older_nexus = specs.nexus(target_cfg.target().nexus()).await?;
        let mut move_nexus = true;

        match healthy_volume_replicas(&spec, &older_nexus.as_ref().node, registry).await {
            Ok(_) => {
                if request.reuse_existing
                    && !older_nexus.as_ref().is_shutdown()
                    && older_nexus.missing_nexus_recreate(registry).await.is_ok()
                {
                    move_nexus = false;
                }
            }
            Err(error) => {
                if !older_nexus.as_ref().is_shutdown() {
                    return Err(error);
                }
            }
        }

        if !move_nexus {
            // The older nexus is back again, so completing republish without modifications.
            info!(nexus.uuid=%older_nexus.as_ref().uuid, "Current target is back online, not moving nexus");
            let volume = registry.volume(&request.uuid).await?;
            return Ok(volume);
        }

        // Get the newer target node for the new nexus creation.
        let nexus_node = get_volume_target_node(registry, &state, request, true).await?;
        let nodes = target_cfg.frontend().node_names();
        let target_cfg = self
            .next_target_config(registry, &nexus_node, &Some(request.share), &nodes)
            .await;
        let operation = VolumeOperation::Republish(RepublishOperation::new(target_cfg.clone()));

        let spec_clone = self.start_update(registry, &state, operation).await?;

        let older_nexus_id = older_nexus.uuid().clone();

        // Shutdown the older nexus before newer nexus creation.
        let result = older_nexus
            .shutdown(registry, &ShutdownNexus::new(older_nexus_id, true))
            .await;
        self.validate_update_step(registry, result, &spec_clone)
            .await?;

        // Create a Nexus on the requested or auto-selected node.
        let result = specs
            .volume_create_nexus(registry, &target_cfg, &spec_clone)
            .await;
        let (mut nexus, nexus_state) = self
            .validate_update_step(registry, result, &spec_clone)
            .await?;
        let allowed_host = target_cfg.frontend().node_nqns();
        // Share the Nexus.
        let result = match nexus
            .share(
                registry,
                &ShareNexus::new(&nexus_state, request.share, allowed_host),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                // Since we failed to share, we'll revert back to the previous state.
                // If we fail to do this inline, the reconcilers will pick up the slack.
                nexus
                    .destroy(registry, &DestroyNexus::from(nexus_state).with_disown_all())
                    .await
                    .ok();
                Err(error)
            }
        };

        self.complete_update(registry, result, spec_clone).await?;

        let volume = registry.volume(&request.uuid).await?;
        registry
            .notify_if_degraded(&volume, PollTriggerEvent::VolumeDegraded)
            .await;
        Ok(volume)
    }
}

/// Request to move the given replica to another pool.
/// May be useful to reclaim space in the current pool to handle thin provisioning.
#[derive(Debug, Clone)]
pub(crate) struct MoveReplicaRequest {
    replica: ReplicaId,
    /// Delete the moved replica after we've created the replacement replica?
    /// todo: we might only want to delete after rebuild completes only..
    delete: bool,
}
impl MoveReplicaRequest {
    /// Get a reference to the replica.
    pub(crate) fn replica(&self) -> &ReplicaId {
        &self.replica
    }
    /// Builder-like specification of delete behaviour.
    pub(crate) fn with_delete(mut self, delete: bool) -> Self {
        self.delete = delete;
        self
    }
}
impl From<&ENoSpcReplica> for MoveReplicaRequest {
    fn from(value: &ENoSpcReplica) -> Self {
        Self {
            replica: value.replica().uuid.clone(),
            delete: false,
        }
    }
}

#[async_trait::async_trait]
impl ResourceReplicas for OperationGuardArc<VolumeSpec> {
    type Request = SetVolumeReplica;
    type MoveRequest = MoveReplicaRequest;
    type MoveResp = Replica;

    async fn set_replica(
        &mut self,
        registry: &Registry,
        request: &Self::Request,
    ) -> Result<(), SvcError> {
        let specs = registry.specs();

        let state = registry.volume_state(&request.uuid).await?;

        let operation = VolumeOperation::SetReplica(request.replicas);
        let spec_clone = self.start_update(registry, &state, operation).await?;

        assert_ne!(request.replicas, spec_clone.num_replicas);
        if request.replicas > spec_clone.num_replicas {
            specs
                .increase_volume_replica(self, registry, state, spec_clone)
                .await?;
        } else {
            specs
                .decrease_volume_replica(self, registry, state, spec_clone)
                .await?;
        }
        Ok(())
    }

    async fn move_replica(
        &mut self,
        registry: &Registry,
        request: &Self::MoveRequest,
    ) -> Result<Self::MoveResp, SvcError> {
        let candidates =
            volume_move_replica_candidates(registry, self.as_ref(), request.replica()).await?;

        let new_replica = registry
            .specs()
            .create_volume_replica_with(registry, self.as_ref(), candidates)
            .await?;

        if let Some(nexus_spec) = &self
            .as_ref()
            .target()
            .and_then(|t| registry.specs().nexus_rsc(t.nexus()))
        {
            let mut guard = nexus_spec.operation_guard()?;
            let nexus = registry.nexus(guard.uuid()).await?;
            registry
                .specs()
                .attach_replica_to_nexus(registry, &mut guard, self.uuid(), &nexus, &new_replica)
                .await?;

            if request.delete {
                self.remove_child_replica(request.replica(), &mut guard, registry)
                    .await?;
            }
        } else if request.delete {
            // todo: if there's no nexus, should we delete it?
            // For now let the reconciler delete it?
        }

        Ok(new_replica)
    }
}

#[async_trait::async_trait]
impl ResourceShutdownOperations for OperationGuardArc<VolumeSpec> {
    type RemoveShutdownTargets = DestroyShutdownTargets;
    type Shutdown = ();

    async fn shutdown(
        &mut self,
        _registry: &Registry,
        _request: &Self::Shutdown,
    ) -> Result<(), SvcError> {
        // not applicable for volume
        unimplemented!()
    }

    async fn remove_shutdown_targets(
        &mut self,
        registry: &Registry,
        request: &Self::RemoveShutdownTargets,
    ) -> Result<(), SvcError> {
        let shutdown_nexuses = registry
            .specs()
            .volume_shutdown_nexuses(request.uuid())
            .await;
        let mut result = Ok(());
        for nexus_res in shutdown_nexuses {
            match nexus_res.operation_guard_wait().await {
                Ok(mut guard) => {
                    if let Ok(nexus) = registry.nexus(nexus_res.uuid()).await {
                        if target_registered(request.registered_targets(), nexus)? {
                            continue;
                        }
                    }
                    let nexus_spec = guard.as_ref().clone();
                    let destroy_req = DestroyNexus::from(nexus_spec)
                        .with_disown(request.uuid())
                        .with_lazy(true);
                    match guard.destroy(registry, &destroy_req).await {
                        Ok(_) => {
                            if self.as_ref().health_info_id() != Some(guard.uuid()) {
                                ResourceSpecsLocked::delete_nexus_info(
                                    &NexusInfoKey::new(&Some(request.uuid().clone()), guard.uuid()),
                                    registry,
                                )
                                .await;
                            }
                        }
                        Err(error) => match error {
                            // If the store is not available, no point in trying the others.
                            SvcError::Store { .. } => return Err(error),
                            _ => {
                                tracing::debug!(
                                    %error,
                                    nexus.uuid = %destroy_req.uuid,
                                    "Encountered error while destroying shutdown nexus"
                                );
                                // if we're not at least marked for deletion then we'll have to
                                // get the cluster agent to retry..
                                if !guard.lock().status().deleting_or_deleted() {
                                    result = Err(error);
                                }
                            }
                        },
                    }
                }
                Err(error) => {
                    result = Err(error);
                }
            }
        }
        result
    }
}

/// Checks if Nexus is present in registered target list. Returns true if yes.
fn target_registered(
    registered_target: Option<Vec<String>>,
    nexus: Nexus,
) -> Result<bool, SvcError> {
    // let path = nexus.device_uri;
    if let Some(targets) = registered_target {
        let parsed_uri = nexus
            .device_uri
            .parse::<Uri>()
            .map_err(|_| SvcError::InvalidArguments {})?;
        let host = parsed_uri
            .host()
            .ok_or(SvcError::InvalidArguments {})?
            .to_string();
        Ok(targets.contains(&host))
    } else {
        Ok(false)
    }
}

impl OperationGuardArc<VolumeSpec> {
    fn published(&self) -> bool {
        self.as_ref().target().is_some()
    }
    /// Make the next target config.
    /// This essentially bumps up the controller id by 1 as otherwise the initiator cannot tell
    /// this target apart from others.
    /// Also sets the reservation key based off the nexus uuid.
    async fn next_target_config(
        &self,
        registry: &Registry,
        node: &NodeId,
        share: &Option<VolumeShareProtocol>,
        frontend_nodes: &[String],
    ) -> TargetConfig {
        let nexus = NexusId::new();
        let resv_key = nexus.as_u128() as u64;
        let range = match self.as_ref().config() {
            None => NvmfControllerIdRange::new_min(),
            Some(cfg) => {
                // todo: should the cluster agent tell us which controller Id to use?
                #[allow(clippy::if_same_then_else)]
                if self.published() {
                    cfg.config().controller_id_range().next()
                } else {
                    // if we're not published should we start over?
                    // for now let's carry on to next as there might some cases where we unpublish
                    // but the initiator doesn't disconnect properly.
                    cfg.config().controller_id_range().next()
                }
            }
        };
        let host_acl = registry.host_acl_nodename(HostAccessControl::Nexuses, frontend_nodes);
        let frontend = FrontendConfig::from_acls(host_acl);
        TargetConfig::new(
            VolumeTarget::new(node.clone(), nexus, *share),
            NexusNvmfConfig::new(
                range,
                resv_key,
                NvmeReservation::ExclusiveAccess,
                NexusNvmePreemption::Holder,
            ),
            frontend,
        )
    }

    /// Remove the given NexusChild Replica but make sure we're not removing the last healthy
    /// replica of the volume which would cause data loss!
    /// TODO: Should there be a minimum remaining number of healthy replicas left?
    #[tracing::instrument(level = "info", skip(self, nexus, registry), fields(volume.uuid = %self.uuid()))]
    pub(crate) async fn remove_child_replica(
        &mut self,
        replica_id: &ReplicaId,
        nexus: &mut OperationGuardArc<NexusSpec>,
        registry: &Registry,
    ) -> Result<(), SvcError> {
        let health_info = self.lock().health_info_id().cloned();
        let nexus_info = registry
            .nexus_info(Some(self.uuid()), health_info.as_ref(), true)
            .await?
            .ok_or(SvcError::Internal {
                details: "No NexusInfo for a volume with allocated storage?".into(),
            })?;

        let nexus_state = registry.nexus(nexus.uuid()).await?;

        let replica_uri = match nexus.as_ref().replica_uuid_uri(replica_id) {
            Some(replica_uri) => Ok(replica_uri.clone()),
            // If the child is no longer present, not much point carrying on..
            None => Err(SvcError::ChildNotFound {
                nexus: nexus.uuid().to_string(),
                child: replica_id.to_string(),
            }),
        }?;
        let spec_children = &nexus.as_ref().children;
        let healthy_children_len = nexus_state
            .children
            .into_iter()
            .filter(|c| c.state.online())
            .filter(|c| {
                match spec_children
                    .iter()
                    .find(|sc| sc.uri() == c.uri)
                    .and_then(|sc| sc.as_replica())
                {
                    Some(sc) => nexus_info.is_replica_healthy(sc.uuid()),
                    None => false,
                }
            })
            .filter(|c| &c.uri != replica_uri.uri())
            .count();

        // todo: what should the minimum number of healthy children be?
        if healthy_children_len == 0 {
            self.warn_span(|| {
                tracing::error!(
                    replica.uuid = replica_id.as_str(),
                    "Cannot remove replica as volume has no other healthy children remaining"
                )
            });
            return Err(SvcError::NoHealthyReplicas {
                id: self.uuid().to_string(),
            });
        }

        let mut replica = registry.specs().replica(replica_id).await?;
        replica.fault(nexus, registry).await?;
        self.warn_span(|| {
            tracing::warn!(
                child.uri = replica_uri.uri().as_str(),
                child.uuid = replica_uri.uuid().as_str(),
                "Successfully faulted child"
            )
        });

        Ok(())
    }
}
