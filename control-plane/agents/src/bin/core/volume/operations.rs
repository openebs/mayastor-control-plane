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
                GuardedOperationsHelper, OperationSequenceGuard, ResourceSpecsLocked,
            },
            OperationGuardArc, TraceSpan, TraceStrLog,
        },
    },
    volume::specs::{get_create_volume_replicas, get_volume_target_node},
};
use agents::errors::SvcError;
use common_lib::{
    transport_api::ErrorChain,
    types::v0::{
        store::{
            nexus_persistence::NexusInfoKey,
            replica::ReplicaSpec,
            volume::{VolumeOperation, VolumeSpec},
        },
        transport::{
            CreateVolume, DestroyNexus, DestroyReplica, DestroyShutdownTargets, DestroyVolume,
            NexusId, Protocol, PublishVolume, Replica, ReplicaOwners, RepublishVolume,
            SetVolumeReplica, ShareNexus, ShareVolume, ShutdownNexus, UnpublishVolume,
            UnshareNexus, UnshareVolume, Volume,
        },
    },
};
use std::ops::Deref;
use tracing::debug;

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
        let result = get_create_volume_replicas(registry, request).await;
        let create_replicas = volume.validate_create_step(registry, result).await?;

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

        volume.complete_create(result, registry).await?;
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

        let nexuses = specs.get_volume_nexuses(&request.uuid);
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

        let replicas = specs.get_volume_replicas(&request.uuid);
        for replica in replicas {
            let mut replica = match replica.operation_guard_wait().await {
                Ok(replica) => replica,
                Err(_) => continue,
            };
            if let Some(node) =
                ResourceSpecsLocked::get_replica_node(registry, replica.as_ref()).await
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
        let state = registry.get_volume_state(&request.uuid).await?;

        let spec_clone = self
            .start_update(registry, &state, VolumeOperation::Share(request.protocol))
            .await?;

        let target = state.target.expect("already validated");
        let result = match specs.nexus(&target.uuid).await {
            Ok(mut nexus) => {
                nexus
                    .share(
                        registry,
                        &ShareNexus::from((&target, None, request.protocol)),
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
        let state = registry.get_volume_state(&request.uuid).await?;

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
        let state = registry.get_volume_state(&request.uuid).await?;
        let nexus_node = get_volume_target_node(registry, &state, request, false).await?;
        let nexus_id = NexusId::new();

        let operation =
            VolumeOperation::Publish((nexus_node.clone(), nexus_id.clone(), request.share));
        let spec_clone = self.start_update(registry, &state, operation).await?;

        // Create a Nexus on the requested or auto-selected node
        let result = specs
            .volume_create_nexus(registry, &nexus_node, &nexus_id, &spec_clone)
            .await;

        let (mut nexus, nexus_state) = self
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        let (volume_id, last_nexus_id) = {
            let volume_spec = self.lock();
            (volume_spec.uuid.clone(), volume_spec.last_nexus_id.clone())
        };

        // Share the Nexus if it was requested
        let mut result = Ok(());
        if let Some(share) = request.share {
            result = match nexus
                .share(registry, &ShareNexus::from((&nexus_state, None, share)))
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
        if let Some(nexus_id) = last_nexus_id {
            ResourceSpecsLocked::delete_nexus_info(
                &NexusInfoKey::new(&Some(volume_id), &nexus_id),
                registry,
            )
            .await;
        }

        let volume = registry.get_volume(&request.uuid).await?;
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

        let state = registry.get_volume_state(&request.uuid).await?;

        let spec_clone = self
            .start_update(registry, &state, VolumeOperation::Unpublish)
            .await?;

        let volume_target = spec_clone.target.as_ref().expect("already validated");
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
                        let node_online = match registry.get_node_wrapper(&nexus_clone.node).await {
                            Ok(node) => {
                                let mut node = node.write().await;
                                node.is_online() && node.liveness_probe().await.is_ok()
                            }
                            _ => false,
                        };
                        if !node_online {
                            nexus_clone.warn_span(|| {
                                tracing::warn!("Force unpublish. Forgetting about the target nexus because the node is not online and it was requested")
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
        let state = registry.get_volume_state(&request.uuid).await?;
        // If the volume is not published then it should issue publish call rather than republish.
        let volume_target = match spec.target {
            None => {
                return Err(SvcError::VolumeNotPublished {
                    vol_id: request.uuid.to_string(),
                })
            }
            Some(target) => target,
        };
        let nexus_node = get_volume_target_node(registry, &state, request, true).await?;
        let newer_nexus_id = NexusId::new();
        let operation =
            VolumeOperation::Republish((nexus_node.clone(), newer_nexus_id.clone(), request.share));
        let spec_clone = self.start_update(registry, &state, operation).await?;
        let result = specs.nexus(volume_target.nexus()).await;
        let mut older_nexus = self
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        let older_nexus_id = older_nexus.uuid().clone();
        // shutdown the older nexus before newer nexus creation.
        // todo: Handle scenario when node is offline and shutdown fails.
        let result = older_nexus
            .shutdown(registry, &ShutdownNexus::new(older_nexus_id))
            .await;
        self.validate_update_step(registry, result, &spec_clone)
            .await?;

        // Create a Nexus on the requested or auto-selected node
        let result = specs
            .volume_create_nexus(registry, &nexus_node, &newer_nexus_id, &spec_clone)
            .await;
        let (mut nexus, nexus_state) = self
            .validate_update_step(registry, result, &spec_clone)
            .await?;

        let (volume_id, last_nexus_id) = {
            let volume_spec = self.lock();
            (volume_spec.uuid.clone(), volume_spec.last_nexus_id.clone())
        };

        // Share the Nexus
        let result = match nexus
            .share(
                registry,
                &ShareNexus::from((&nexus_state, None, request.share)),
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

        // If there was a previous nexus we should delete the persisted NexusInfo structure.
        // todo: This needs to be revisited, as there might be inconsistency on child state after
        // creation of nexus.
        if let Some(nexus_id) = last_nexus_id {
            ResourceSpecsLocked::delete_nexus_info(
                &NexusInfoKey::new(&Some(volume_id), &nexus_id),
                registry,
            )
            .await;
        }

        let volume = registry.get_volume(&request.uuid).await?;
        registry
            .notify_if_degraded(&volume, PollTriggerEvent::VolumeDegraded)
            .await;
        Ok(volume)
    }
}

#[async_trait::async_trait]
impl ResourceReplicas for OperationGuardArc<VolumeSpec> {
    type Request = SetVolumeReplica;

    async fn set_replica(
        &mut self,
        registry: &Registry,
        request: &Self::Request,
    ) -> Result<(), SvcError> {
        let specs = registry.specs();

        let state = registry.get_volume_state(&request.uuid).await?;

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
            .get_volume_shutdown_nexuses(&request.uuid)
            .await;

        for mut nexus_res in shutdown_nexuses {
            match nexus_res.operation_guard_wait().await {
                Ok(mut guard) => {
                    let nexus_spec = guard.as_ref().clone();
                    let destroy_req = DestroyNexus::from(nexus_spec).with_disown(&request.uuid);
                    match guard.destroy(registry, &destroy_req).await {
                        Ok(_) => {
                            // todo: handle cleanup properly, as mutiple replublish followed by
                            // destroy is causing loss of key.
                            // ResourceSpecsLocked::delete_nexus_info(
                            //     &NexusInfoKey::new(&Some(request.uuid.clone()), guard.uuid()),
                            //     registry,
                            // )
                            // .await;
                        }
                        Err(err) => match err {
                            SvcError::Store { .. } => return Err(err),
                            SvcError::StoreSave { .. } => return Err(err),
                            _ => {
                                debug!(
                                error = %err,
                                nexus.uuid = %destroy_req.uuid,
                                "Encountered error while destroying shutdown nexus"
                                )
                            }
                        },
                    }
                }
                Err(err) => {
                    debug!(
                        error = %err,
                        nexus.uuid = %nexus_res.uuid(),
                        "Failed to guard one of the destroying shutdown nexus"
                    );
                }
            }
        }
        Ok(())
    }
}
