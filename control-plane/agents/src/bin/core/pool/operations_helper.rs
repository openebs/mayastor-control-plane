use crate::{
    controller::{
        io_engine::{HostApi, PoolApi},
        registry::Registry,
        resources::{
            operations::ResourceLifecycle,
            operations_helper::{GuardedOperationsHelper, OnCreateFail, SpecOperationsHelper},
            OperationGuardArc, ResourceUid,
        },
    },
    node::wrapper::{GetterOps, NodeWrapper},
};
use agents::{errors, errors::SvcError};
use grpc::operations::pool::traits::ClearErrorsRequest;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            pool::{PoolImportOp, PoolOperation, PoolSpec},
            replica::{PoolRef, ReplicaSpec},
        },
        transport::{
            CreatePool, CtrlPoolState, DestroyReplica, GetBlockDevices, ImportBackoff, ImportPool,
            NodeId, NodeStatus, Pool, PoolDeviceUri, PoolDiag, PoolDiskError, PoolError,
            PoolErrorCode, PoolState, PoolStatus, ReplicaOwners,
        },
    },
};

use itertools::Itertools;
use regex::Regex;
use snafu::OptionExt;
use std::{collections::HashSet, ops::Deref, sync::Arc};
use stor_port::types::v0::transport::{PoolId, SnapshotLossInfo, VolumeId, VolumeLossInfo};
use tokio::sync::RwLock;

impl OperationGuardArc<PoolSpec> {
    /// Retries the creation of the pool which is being done in the background by the io-engine.
    /// This may happen if the pool create gRPC times out, for very large pools.
    /// We could increase the timeout but as things stand today that would block all gRPC
    /// access to the node.
    /// TODO: Since the data-plane now allows concurrent gRPC we should also modify the
    ///  control-plane to allow this, which would allows to set large timeouts for some gRPCs.
    pub(crate) async fn retry_creating(&mut self, registry: &Registry) -> Result<(), SvcError> {
        let request = {
            let spec = self.lock();
            if on_create_fail(&spec, registry).is_some() {
                return Ok(());
            }
            CreatePool::from(spec.deref())
        };

        let node = registry.node_wrapper(&request.node).await?;
        if node.pool(&request.id).await.is_none() {
            return Ok(());
        }

        let _ = self.start_create(registry, &request).await?;
        let result = node.create_pool(&request).await;

        let _state = self
            .complete_create(result, registry, OnCreateFail::LeaveAsIs)
            .await?;

        Ok(())
    }

    /// Get the [`NodeWrapper`] for this pool.
    /// # NOTE
    /// If the node is not online, its status is returned.
    pub(crate) async fn node_wrapper(
        &mut self,
        registry: &Registry,
        node: &NodeId,
    ) -> Result<Result<Arc<RwLock<NodeWrapper>>, NodeStatus>, SvcError> {
        let error = match registry.node_wrapper(node).await {
            Ok(node) => {
                let node_guard = node.read().await;
                if node_guard.is_offline() {
                    self.mark_diag_error(PoolError {
                        code: PoolErrorCode::NodeIsOffline,
                        msg: "".to_string(),
                    });
                    return Ok(Err(node_guard.status()));
                }
                drop(node_guard);
                return Ok(Ok(node));
            }
            Err(error) => error,
        };

        if let Ok(node) = registry.specs().node(node) {
            if node.is_shutdown() {
                self.mark_diag_error(PoolError {
                    code: PoolErrorCode::NodeIsOffline,
                    msg: "".to_string(),
                });
                return Ok(Err(NodeStatus::Offline));
            }
        }

        self.mark_diag_error(PoolError {
            code: PoolErrorCode::NodeIsUnknown,
            msg: "".to_string(),
        });
        Err(error)
    }

    fn mark_diag_error(&mut self, error: PoolError) {
        let status = self.pool_error_to_status(error.code);
        self.mark_diag(status, error);
    }

    fn mark_diag(&mut self, status: PoolStatus, error: PoolError) {
        let mut pool = self.lock();
        let Some(ref mut diag) = &mut pool.metadata.runtime.diag else {
            pool.metadata.runtime.diag = Some(PoolDiag {
                error: Some(error),
                status,
                ..Default::default()
            });
            return;
        };
        diag.error = Some(error);
        diag.status = status;
    }

    /// Maps a [`PoolErrorCode`] to a [`PoolStatus`].
    /// We try to create a consistent mapping between the error code and the status.
    /// If the pool device is having issues and we can not import/create the pool then
    /// we map it as offline.
    /// If we're not sure what the error was, or if we timed out then we're unsure, so
    /// we map it as unknown.
    /// If the pool has invalid metadata then we map it as faulted.
    fn pool_error_to_status(&self, error: PoolErrorCode) -> PoolStatus {
        match error {
            PoolErrorCode::Unknown => PoolStatus::Unknown,
            PoolErrorCode::DiskNotFound => PoolStatus::Offline,
            PoolErrorCode::DiskReadIoError => PoolStatus::Offline,
            PoolErrorCode::ForeignPoolName => PoolStatus::Faulted,
            PoolErrorCode::ForeignPoolUid => PoolStatus::Faulted,
            PoolErrorCode::SuperBlock => PoolStatus::Offline,
            PoolErrorCode::InvalidSuperBlock => PoolStatus::Faulted,
            PoolErrorCode::DiskIsADirectory => PoolStatus::Offline,
            PoolErrorCode::NodeIsUnknown => PoolStatus::Unknown,
            PoolErrorCode::NodeIsOffline => PoolStatus::Offline,
            PoolErrorCode::ImportDisabled => PoolStatus::Offline,
            PoolErrorCode::TimeOut => PoolStatus::Unknown,
            PoolErrorCode::DiskClaimed => PoolStatus::Offline,
            PoolErrorCode::PCIDriverUnsupported => PoolStatus::Offline,
            PoolErrorCode::PCIKernelBound => PoolStatus::Offline,
            PoolErrorCode::PCINotNvme => PoolStatus::Offline,
            PoolErrorCode::InvalidDiskUri => PoolStatus::Offline,
        }
    }
    /// Mark the pool in [`PoolErrorCode::ImportDisabled`] since it cannot be
    /// imported due to being cordoned for imports.
    pub(crate) fn mark_as_import_cordoned(&mut self) {
        let error = PoolError {
            code: PoolErrorCode::ImportDisabled,
            msg: "".to_string(),
        };
        self.mark_diag(PoolStatus::Offline, error);
    }

    /// Probes a pool for any errors with its backing devices.
    /// # NOTE
    /// In case probe fails we should not log any information to stdout, as we're trying to avoid
    /// thrashing the logs when a pool is in a bad state for a long time.
    pub(crate) async fn probe(
        &self,
        spec: &PoolSpec,
        node: &Arc<RwLock<NodeWrapper>>,
    ) -> Result<bool, SvcError> {
        let request = ImportPool::new(&spec.node, &spec.id, &spec.disks, &spec.encryption);
        let probed = node.probe_pool(&request.into()).await?;

        if probed.success || probed.unimpl {
            return Ok(true);
        }

        let mut diag = PoolDiag::default();

        for (_, error) in probed.errors {
            for probe in error.error {
                diag.import_errors.push(probe);
            }
        }
        diag.error = diag.import_errors.first().map(|e| e.error.clone());
        diag.status =
            self.pool_error_to_status(diag.error.as_ref().map(|e| e.code).unwrap_or_default());
        let mut pool = self.lock();
        pool.metadata.runtime.diag = Some(diag);

        Ok(false)
    }

    /// Attempt to import a pool.
    /// # NOTE
    /// In case the import fails, we try to map the failure reason into the pool diagnostics.
    /// This should give the user more visibility into the failure.
    pub(crate) async fn import(
        &mut self,
        registry: &Registry,
        spec: &PoolSpec,
        node: Arc<RwLock<NodeWrapper>>,
    ) -> Result<PoolState, SvcError> {
        let reporter = Arc::new(std::sync::Mutex::new(None));
        let operation = PoolOperation::Import(PoolImportOp {
            report: reporter.clone(),
        });
        let pool_spec = self.start_update(registry, spec, operation).await?;

        let request = ImportPool::new(
            &pool_spec.node,
            &pool_spec.id,
            &pool_spec.disks,
            &pool_spec.encryption,
        );
        let result = node.import_pool(&request).await;
        if let Err(error) = &result {
            if let Some(error) = Self::pool_import_error(error) {
                let disks = pool_spec.disks.first().map(|d| d.to_string());
                *reporter.lock().expect("not poisoned") = Some(PoolDiag {
                    import_errors: vec![PoolDiskError {
                        error: error.clone(),
                        disk: disks.unwrap_or_default(),
                    }],
                    status: PoolStatus::Offline,
                    error: Some(error),
                    import: ImportBackoff::new(&spec.metadata.runtime, registry.reconcile_period()),
                });
            }
        }
        self.complete_update(registry, result, pool_spec).await
    }

    /// Ge the `OnCreateFail` policy.
    /// For more information see [`Self::retry_creating`].
    pub(crate) fn on_create_fail(&self, registry: &Registry) -> OnCreateFail {
        let spec = self.lock();
        on_create_fail(&spec, registry).unwrap_or(OnCreateFail::LeaveAsIs)
    }

    /// Maps a [`SvcError`] obtained after a failed creation or import to a [`PoolError`].
    pub(super) fn pool_import_error(error: &SvcError) -> Option<PoolError> {
        let (code, errno) = error.tonic_errno();
        let code = match code {
            tonic::Code::InvalidArgument
                if error.to_string().contains("EISDIR: Is a directory") =>
            {
                PoolErrorCode::DiskIsADirectory
            }
            tonic::Code::DataLoss if errno == nix::Error::EIO => PoolErrorCode::SuperBlock,
            tonic::Code::DataLoss if errno == nix::Error::EILSEQ => {
                PoolErrorCode::InvalidSuperBlock
            }
            tonic::Code::InvalidArgument => PoolErrorCode::InvalidSuperBlock,
            tonic::Code::NotFound => PoolErrorCode::DiskNotFound,
            tonic::Code::Cancelled => PoolErrorCode::TimeOut,
            tonic::Code::Aborted => PoolErrorCode::TimeOut,
            _ => return None,
        };
        let msg = match &error {
            SvcError::GrpcRequestError { source, .. } => {
                format!("{:?}: {}", source.code(), source.message())
            }
            _error => _error.to_string(),
        };
        Some(PoolError { code, msg })
    }

    // todo: fit in a trait
    pub(crate) async fn clear_errors(
        &mut self,
        registry: &Registry,
        request: &ClearErrorsRequest,
    ) -> Result<Pool, SvcError> {
        let pool = registry.ctrl_pool(self.uid()).await?;
        let node = registry.node_wrapper(&pool.node()).await?;
        if !node.read().await.is_online() {
            return Err(SvcError::NodeNotOnline { node: pool.node() });
        }
        let pool_state = node.clear_errors(request).await?;
        let pool_spec = registry.specs().pool(self.uid())?;

        Ok(Pool::new(pool_spec, Some(CtrlPoolState::new(pool_state))))
    }

    /// Analyze volume loss impact for a single pool being purged.
    pub(crate) async fn analyze_volume_loss(
        registry: &Registry,
        pool_id: &PoolId,
        volume_ids: &HashSet<VolumeId>,
    ) -> Result<Option<VolumeLossInfo>, SvcError> {
        let pool_ids = HashSet::from([pool_id.clone()]);
        crate::controller::resources::operations_helper::analyze_volume_loss(
            registry, &pool_ids, volume_ids,
        )
        .await
    }

    /// Analyze snapshot loss impact for a single pool being purged.
    pub(crate) fn analyze_snapshot_loss(
        registry: &Registry,
        pool_id: &PoolId,
    ) -> Result<Option<SnapshotLossInfo>, SvcError> {
        let pool_ids = HashSet::from([pool_id.clone()]);
        crate::controller::resources::operations_helper::analyze_snapshot_loss(registry, &pool_ids)
    }
}

fn on_create_fail(pool: &PoolSpec, registry: &Registry) -> Option<OnCreateFail> {
    if !pool.status().creating() {
        return Some(OnCreateFail::LeaveAsIs);
    }
    let Some(last_mod_elapsed) = pool.creat_tsc.and_then(|t| t.elapsed().ok()) else {
        return Some(OnCreateFail::SetDeleting);
    };
    if last_mod_elapsed > registry.pool_async_creat_tmo() {
        return Some(OnCreateFail::SetDeleting);
    }
    None
}

impl OperationGuardArc<ReplicaSpec> {
    /// Destroy the replica from its volume
    pub(crate) async fn destroy_volume_replica(
        &mut self,
        registry: &Registry,
        node_id: Option<&NodeId>,
    ) -> Result<(), SvcError> {
        let node_id = match node_id {
            Some(node_id) => node_id.clone(),
            None => {
                let replica_uuid = self.lock().uuid.clone();
                match registry.replica(&replica_uuid).await {
                    Ok(state) => state.node.clone(),
                    Err(_) => {
                        let pool_ref = self.lock().pool.clone();
                        let pool_id = match pool_ref {
                            PoolRef::Named(name) => name,
                            PoolRef::Uuid(name, _) => name,
                        };
                        let pool_spec = registry
                            .specs()
                            .pool_rsc(&pool_id)
                            .context(errors::PoolNotFound { pool_id })?;
                        let node_id = pool_spec.lock().node.clone();
                        node_id
                    }
                }
            }
        };

        self.destroy(
            registry,
            &self.destroy_request(ReplicaOwners::new_disown_all(), &node_id),
        )
        .await
    }

    /// Return a `DestroyReplica` request based on the provided arguments
    pub(crate) fn destroy_request(&self, by: ReplicaOwners, node: &NodeId) -> DestroyReplica {
        let spec = self.as_ref().clone();
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
}

pub(crate) async fn devlink_preflight_checks(
    request: &CreatePool,
    node: Arc<RwLock<NodeWrapper>>,
    registry: &Registry,
) -> Result<(), SvcError> {
    let request_disks: HashSet<String> = request
        .disks
        .iter()
        .map(|disk| utils::disk::normalize_disk(disk.as_str()))
        .collect();

    if !registry.allow_non_persistent_devlinks() {
        fn is_persistent_devlink(pattern: &str) -> Result<bool, SvcError> {
            let re = Regex::new(utils::DEVLINK_REGEX).map_err(|_| SvcError::InvalidArguments {})?;
            Ok(re.is_match(pattern))
        }

        if request_disks
            .iter()
            // Only attempt to validate if it starts with "/dev".
            .filter(|disk| disk.starts_with("/dev"))
            .any(|disk| !is_persistent_devlink(disk).is_ok_and(|val| val))
        {
            return Err(SvcError::InvalidDevlink {});
        }
    }

    let node_pools = registry
        .get_node_opt_pools(Some(request.node.clone()))
        .await?;

    if !node_pools.is_empty() {
        let node_pools_disks: Vec<PoolDeviceUri> = node_pools
            .into_iter()
            .filter_map(|pool| pool.spec().map(|spec| spec.disks))
            .flatten()
            .collect();

        let node_pools_disks_normalized: HashSet<String> = node_pools_disks
            .iter()
            .map(|disk| utils::disk::normalize_disk(disk.as_str()))
            .collect();

        let common_disks: HashSet<_> = request_disks
            .intersection(&node_pools_disks_normalized)
            .cloned()
            .collect();

        // Same devpaths or devlinks should be rejected.
        if !common_disks.is_empty() {
            return Err(SvcError::InUse {
                kind: ResourceKind::Block,
                id: common_disks.iter().join(","),
            });
        }

        let node_block_devices = node
            .list_blockdevices(&GetBlockDevices {
                node: request.node.clone(),
                all: true,
            })
            .await?
            .into_inner();

        let matched_devices: Vec<_> = node_block_devices
            .iter()
            .filter(|device| {
                request_disks.contains(&device.devname)
                    || request_disks.contains(&device.devpath)
                    || device
                        .devlinks
                        .iter()
                        .any(|link| request_disks.contains(link))
            })
            .collect();

        // If the requested disk was not found, that could be because it could be a malloc or a file,
        // in that case ignore and move ahead to allow tests. If it is an actual disk that was not
        // detected by blockdevice api then the disk might not be visible to io-engine, so let it fail from io-engine
        // rather than bailing out from control-plane to keep the behaviour as before.
        if !matched_devices.is_empty() {
            if let Some(conflict) = matched_devices.iter().find(|bd| {
                node_pools_disks_normalized.contains(&bd.devname)
                    || node_pools_disks_normalized.contains(&bd.devpath)
                    || bd
                        .devlinks
                        .iter()
                        .any(|link| node_pools_disks_normalized.contains(link))
            }) {
                return Err(SvcError::InUse {
                    kind: ResourceKind::Block,
                    id: conflict.devname.clone(),
                });
            }
        }
    }
    Ok(())
}
