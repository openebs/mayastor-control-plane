use crate::{
    controller::{
        io_engine::{HostApi, PoolApi},
        registry::Registry,
        resources::{
            operations::ResourceLifecycle,
            operations_helper::{GuardedOperationsHelper, OnCreateFail, SpecOperationsHelper},
            OperationGuardArc,
        },
    },
    node::wrapper::{GetterOps, NodeWrapper},
};
use agents::{errors, errors::SvcError};
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            pool::{PoolImportOp, PoolOperation, PoolSpec},
            replica::{PoolRef, ReplicaSpec},
        },
        transport::{
            CreatePool, DestroyReplica, GetBlockDevices, ImportPool, NodeId, PoolDeviceUri,
            PoolDiag, PoolDiskError, PoolError, PoolErrorCode, PoolState, ReplicaOwners,
        },
    },
};

use itertools::Itertools;
use regex::Regex;
use snafu::OptionExt;
use std::{collections::HashSet, ops::Deref, sync::Arc};
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
            let code_map = |code: tonic::Code| -> PoolError {
                let code = match code {
                    tonic::Code::InvalidArgument => PoolErrorCode::InvalidSuperBlock,
                    tonic::Code::NotFound => PoolErrorCode::DiskNotFound,
                    _ => PoolErrorCode::Unknown,
                };
                // todo: this is rather long as it includes details and metadata... trim it?
                let msg = error.to_string();
                PoolError { code, msg }
            };
            match error.tonic_code() {
                tonic::Code::NotFound | tonic::Code::InvalidArgument => {
                    let disks = pool_spec.disks.first().map(|d| d.to_string());
                    *reporter.lock().expect("not poisoned") = Some(PoolDiag {
                        import_errors: vec![PoolDiskError {
                            error: code_map(error.tonic_code()),
                            disk: disks.unwrap_or_default(),
                        }],
                    });
                }
                _ => {}
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
