use crate::{
    controller::{
        io_engine::PoolApi,
        registry::Registry,
        resources::{
            operations::{ResourceCordon, ResourceLabel, ResourceLifecycle, ResourceResize},
            operations_helper::{GuardedOperationsHelper, OnCreateFail, OperationSequenceGuard},
            OperationGuardArc,
        },
    },
    pool::operations_helper::devlink_preflight_checks,
};
use agents::errors::{SvcError, SvcError::CordonedNode};
use grpc::operations::pool::traits::PoolCordonRequest;
use std::collections::{HashMap, HashSet};
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            pool::{CordonDrainState, PoolCordonOp, PoolOperation, PoolSpec},
            snapshots::replica::ReplicaSnapshot,
        },
        transport::{
            CreatePool, CtrlPoolState, DestroyPool, ExpandPool, Pool, PoolDeleteResult, PoolDiag,
            PoolDiskError, PoolId, PoolStatus, ReplicaOwners,
        },
    },
};
use utils::dsp_created_by_key;

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<PoolSpec> {
    type Create = CreatePool;
    type CreateOutput = Pool;
    type Destroy = DestroyPool;
    type DestroyOutput = Option<PoolDeleteResult>;

    async fn create(
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let specs = registry.specs();

        if registry.node_cordoned(&request.node)? {
            return Err(CordonedNode {
                node_id: request.node.to_string(),
            });
        }

        if request.disks.len() != 1 {
            return Err(SvcError::InvalidPoolDeviceNum {
                disks: request.disks.clone(),
            });
        }

        let pool_get_result = registry.specs().pool(&request.id);
        if let Ok(pool) = &pool_get_result {
            if pool.status.created() {
                return Err(SvcError::AlreadyExists {
                    kind: ResourceKind::Pool,
                    id: request.id.to_string(),
                });
            }
        }

        let node = registry.node_wrapper(&request.node).await?;
        // todo: issue rpc to the node to find out?
        if !node.read().await.is_online() {
            return Err(SvcError::NodeNotOnline {
                node: request.node.clone(),
            });
        }

        if pool_get_result.is_err() {
            // If the devlink for a device is used for multiple pools, reject new creation.
            devlink_preflight_checks(request, node.clone(), registry).await?
        }

        let mut pool = specs
            .get_or_create_pool(request)
            .operation_guard_wait()
            .await?;
        let _ = pool.start_create(registry, request).await?;

        let result = node.create_pool(request).await;
        let on_fail = OnCreateFail::on_pool_create_err(&result);
        if matches!(on_fail, OnCreateFail::LeaveAsIs) {
            if let Err(error) = &result {
                if let Some(error) = Self::pool_import_error(error) {
                    let disks = pool.as_ref().disks.first().map(|d| d.to_string());
                    pool.lock().metadata.runtime.diag = Some(PoolDiag {
                        import_errors: vec![PoolDiskError {
                            error: error.clone(),
                            disk: disks.unwrap_or_default(),
                        }],
                        status: PoolStatus::Unknown,
                        error: Some(error),
                    });
                }
            }
        }
        let state = pool.complete_create(result, registry, on_fail).await?;
        let spec = pool.lock().clone();
        Ok(Pool::new(spec, Some(CtrlPoolState::new(state))))
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<Self::DestroyOutput, SvcError> {
        if request.purge {
            self.purge_pool(registry, request).await
        } else {
            self.normal_destroy(registry, request).await
        }
    }
}

#[async_trait::async_trait]
impl ResourceLifecycle for Option<OperationGuardArc<PoolSpec>> {
    type Create = CreatePool;
    type CreateOutput = Pool;
    type Destroy = DestroyPool;
    type DestroyOutput = Option<PoolDeleteResult>;

    async fn create(
        _registry: &Registry,
        _request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        unimplemented!()
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<Self::DestroyOutput, SvcError> {
        if let Some(pool) = self {
            pool.destroy(registry, request).await
        } else {
            // todo: add flag to handle bypassing calls to io-engine!
            Err(SvcError::PoolNotFound {
                pool_id: request.id.clone(),
            })
        }
    }
}

#[async_trait::async_trait]
impl ResourceResize for Option<OperationGuardArc<PoolSpec>> {
    type Resize = ExpandPool;
    type ResizeOutput = Pool;

    async fn resize(
        &mut self,
        registry: &Registry,
        request: &Self::Resize,
    ) -> Result<Self::ResizeOutput, SvcError> {
        if let Some(pool) = self {
            pool.resize(registry, request).await
        } else {
            Err(SvcError::PoolNotFound {
                pool_id: request.id.clone(),
            })
        }
    }
}

#[async_trait::async_trait]
impl ResourceResize for OperationGuardArc<PoolSpec> {
    type Resize = ExpandPool;
    type ResizeOutput = Pool;

    async fn resize(
        &mut self,
        registry: &Registry,
        request: &Self::Resize,
    ) -> Result<Self::ResizeOutput, SvcError> {
        let pool = registry.ctrl_pool(&request.id).await?;
        let node = registry.node_wrapper(&pool.node()).await?;
        if !node.read().await.is_online() {
            return Err(SvcError::NodeNotOnline { node: pool.node() });
        }
        let pool_state = node.expand_pool(request).await?;
        let pool_spec = registry.specs().pool(&request.id)?;

        Ok(Pool::new(pool_spec, Some(CtrlPoolState::new(pool_state))))
    }
}

/// Resource Label Operations.
#[async_trait::async_trait]
impl ResourceLabel for OperationGuardArc<PoolSpec> {
    type LabelOutput = PoolSpec;
    type UnlabelOutput = PoolSpec;

    /// Label a node via operation guard functions.
    async fn label(
        &mut self,
        registry: &Registry,
        labels: HashMap<String, String>,
        overwrite: bool,
    ) -> Result<Self::LabelOutput, SvcError> {
        let cloned_pool_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_pool_spec,
                PoolOperation::Label((labels, overwrite).into()),
            )
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    /// Unlabel a node via operation guard functions.
    async fn unlabel(
        &mut self,
        registry: &Registry,
        label_key: String,
    ) -> Result<Self::UnlabelOutput, SvcError> {
        if label_key == dsp_created_by_key() {
            return Err(SvcError::ForbiddenUnlabelKey {
                labels: label_key,
                resource_kind: ResourceKind::Pool,
            });
        }
        let cloned_pool_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_pool_spec,
                PoolOperation::Unlabel(label_key.into()),
            )
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }
}

/// Resource Cordon Operations.
#[async_trait::async_trait]
impl ResourceCordon for OperationGuardArc<PoolSpec> {
    type CordonOutput = PoolSpec;
    type UncordonOutput = PoolSpec;
    type Request = PoolCordonRequest;

    async fn cordon(
        &mut self,
        registry: &Registry,
        request: PoolCordonRequest,
    ) -> Result<Self::CordonOutput, SvcError> {
        let request = PoolCordonOp {
            replicas: request.replicas,
            snapshots: request.snapshots,
            restores: request.restores,
            import: request.import,
        };
        let spec_clone = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &spec_clone, PoolOperation::Cordon(request))
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    async fn uncordon(
        &mut self,
        registry: &Registry,
        request: PoolCordonRequest,
    ) -> Result<Self::UncordonOutput, SvcError> {
        let request = PoolCordonOp {
            replicas: request.replicas,
            snapshots: request.snapshots,
            restores: request.restores,
            import: request.import,
        };
        let spec_clone = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &spec_clone, PoolOperation::Uncordon(request))
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }
}

impl OperationGuardArc<PoolSpec> {
    /// Normal pool destruction via io-engine.
    async fn normal_destroy(
        &mut self,
        registry: &Registry,
        request: &DestroyPool,
    ) -> Result<Option<PoolDeleteResult>, SvcError> {
        let node = registry.node_wrapper(&request.node).await?;

        self.start_destroy(registry).await?;

        // We may want to prevent this in some situations, example: if a disk URI has changed, we
        // may want to ensure it really is deleted.
        // For now, if there's nothing provisioned on the pool anyway, just allow it..
        // TODO: pass this via REST
        let allow_not_found = self.validate_destroy(registry).is_ok();
        let result = match node.destroy_pool(request).await {
            Ok(_) => Ok(()),
            Err(SvcError::PoolNotFound { .. }) => {
                match node.import_pool(&self.as_ref().into()).await {
                    Ok(_) => node.destroy_pool(request).await,
                    Err(error) => match error.tonic_code() {
                        tonic::Code::NotFound if allow_not_found => Ok(()),
                        tonic::Code::InvalidArgument => Ok(()),
                        tonic::Code::DataLoss => Ok(()),
                        tonic::Code::Cancelled | tonic::Code::Aborted => {
                            if let Some(error) = Self::pool_import_error(&error) {
                                self.lock().metadata.runtime.diag = Some(PoolDiag {
                                    import_errors: vec![],
                                    status: PoolStatus::Unknown,
                                    error: Some(error),
                                });
                            }
                            Err(error)
                        }
                        _other => Err(error),
                    },
                }
            }
            Err(error) => Err(error),
        };
        self.complete_destroy(result, registry).await?;
        Ok(None)
    }

    /// Purge pool without contacting io-engine for pool destruction.
    ///
    /// Use cases:
    /// - Node is permanently offline/decommissioned
    /// - Disk has failed and pool can't be imported (state Unknown)
    ///
    /// This deletes control-plane specs only. Reconcilers handle:
    /// - Volume faulting (when replicas disappear)
    /// - Nexus child cleanup (when children become unreachable)
    /// - Replica snapshot cleanup within VolumeSnapshots
    pub async fn purge_pool(
        &mut self,
        registry: &Registry,
        request: &DestroyPool,
    ) -> Result<Option<PoolDeleteResult>, SvcError> {
        let pool_id = &request.id;
        let pool_spec = self.as_ref().clone();
        let node_id = &pool_spec.node;
        let resuming = pool_spec.status.purging();

        let mut volume_loss_info = None;
        let mut snapshot_loss_info = None;
        let mut replica_snapshots = Vec::new();

        if !resuming {
            // 1. Validate pool state is Unknown (or node offline)
            self.validate_purge_state(registry, pool_id, &pool_spec)
                .await?;

            // 2. Validate pool is cordoned with replicas AND snapshots blocked
            self.validate_purge_cordon(pool_id, &pool_spec)?;

            // 3. Collect replicas on this pool (for pre-flight checks only)
            let replicas = registry.specs().pool_replicas(pool_id);

            // 4. Collect replica snapshots on this pool (for reporting only)
            replica_snapshots = self.collect_pool_replica_snapshots(registry, pool_id);

            // 5. Check accept requirement (if pool has any replicas)
            if !replicas.is_empty() && !request.accept {
                return Err(SvcError::PoolPurgeAcceptRequired {
                    pool_id: pool_id.clone(),
                    replica_count: replicas.len(),
                });
            }

            // 6. Analyze and check volume loss using Volume replica topology
            let volume_ids: HashSet<_> = replicas
                .iter()
                .filter_map(|r| r.lock().owners.volume().cloned())
                .collect();
            volume_loss_info = Self::analyze_volume_loss(registry, pool_id, &volume_ids).await?;
            if let Some(ref info) = volume_loss_info {
                if !request.accept_volume_loss {
                    return Err(SvcError::PoolPurgeVolumeLossAcceptRequired {
                        pool_id: pool_id.clone(),
                        volume_count: info.volumes.len(),
                        volume_loss: info.clone(),
                    });
                }
            }

            // 7. Analyze and check snapshot loss
            snapshot_loss_info = Self::analyze_snapshot_loss(registry, pool_id)?;
            if let Some(ref info) = snapshot_loss_info {
                if !request.accept_snapshot_loss {
                    return Err(SvcError::PoolPurgeSnapshotLossAcceptRequired {
                        pool_id: pool_id.clone(),
                        snapshot_count: info.snapshots.len(),
                        snapshot_loss: info.clone(),
                    });
                }
            }
        }

        // 8. Mark pool as Purging and log the destroy operation.
        //    In the resume case the pool is already Purging but the pending op
        //    may have been cleared on restart — this re-logs it so
        //    complete_destroy can commit the transition to Deleted.
        self.start_destroy_for_purge(registry).await?;

        // 9. Delete replicas: try io-engine RPC first, fall back to spec-only deletion.
        //    Re-collect replicas here — in the resume case the pre-flight collection
        //    was skipped, and even in the normal case some state may have changed.
        let replicas = registry.specs().pool_replicas(pool_id);
        for replica_rsc in &replicas {
            let mut replica = replica_rsc.operation_guard_wait().await?;
            let destroy_request = replica.destroy_request(ReplicaOwners::new_disown_all(), node_id);
            replica.destroy_or_purge(registry, &destroy_request).await?;
        }

        // 10. Note: Replica snapshots are stored within VolumeSnapshot metadata.
        // The VolumeSnapshot reconciler will detect missing replica snapshots
        // and handle cleanup. We just log for visibility.
        if !replica_snapshots.is_empty() {
            tracing::info!(
                pool.id = %pool_id,
                replica_snapshots = replica_snapshots.len(),
                "Replica snapshots on purged pool will be cleaned up by snapshot reconciler"
            );
        }

        // 11. Complete pool deletion
        self.complete_destroy(Ok(()), registry).await?;

        // 12. Build and return result
        let mut result = PoolDeleteResult::new(pool_id.clone());
        if let Some(volume_loss) = volume_loss_info {
            result.volume_loss = volume_loss;
        }
        if let Some(snapshot_loss) = snapshot_loss_info {
            result.snapshot_loss = snapshot_loss;
        }

        tracing::info!(
            pool.id = %pool_id,
            replicas_deleted = replicas.len(),
            snapshots_affected = replica_snapshots.len(),
            volume_loss = result.has_volume_loss(),
            snapshot_loss = result.has_snapshot_loss(),
            "Pool purged successfully. Affected volumes will be marked faulted by reconciler."
        );

        Ok(Some(result))
    }

    /// Validate that the pool state allows purge (must be Unknown, Offline, or node offline).
    async fn validate_purge_state(
        &self,
        registry: &Registry,
        pool_id: &PoolId,
        pool_spec: &PoolSpec,
    ) -> Result<(), SvcError> {
        let node_online = match registry.node_wrapper(&pool_spec.node).await {
            Ok(node) => node.read().await.is_online(),
            Err(_) => false,
        };

        if node_online {
            // Node is online - check pool state via ctrl_pool_state
            if let Ok(ctrl_state) = registry.ctrl_pool_state(pool_id).await {
                if !matches!(ctrl_state.status, PoolStatus::Unknown | PoolStatus::Offline) {
                    return Err(SvcError::PoolStateNotOfflineOrUnknown {
                        pool_id: pool_id.clone(),
                        state: ctrl_state.status.clone(),
                    });
                }
            }
            // If ctrl_pool_state fails, pool state is unknown - allow purge
        }
        // Node is offline - purge is allowed
        Ok(())
    }

    /// Validate that the pool is cordoned with both replicas and snapshots blocked.
    fn validate_purge_cordon(
        &self,
        pool_id: &PoolId,
        pool_spec: &PoolSpec,
    ) -> Result<(), SvcError> {
        match &pool_spec.cordon_drain {
            None => Err(SvcError::PoolNotCordonedForPurge {
                pool_id: pool_id.clone(),
            }),
            Some(CordonDrainState::Cordoned(state)) => {
                if !state.replicas || !state.snapshots {
                    Err(SvcError::PoolCordonInsufficientForPurge {
                        pool_id: pool_id.clone(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Collect all replica snapshots on the given pool.
    fn collect_pool_replica_snapshots(
        &self,
        registry: &Registry,
        pool_id: &PoolId,
    ) -> Vec<ReplicaSnapshot> {
        let mut result = Vec::new();
        for vol_snapshot in registry.specs().volume_snapshots_rsc() {
            let vol_snap = vol_snapshot.lock();
            for txn_replicas in vol_snap.metadata().transactions().values() {
                for replica_snap in txn_replicas {
                    if replica_snap.spec().source_id().pool_id() == pool_id {
                        result.push(replica_snap.clone());
                    }
                }
            }
        }
        result
    }
}
