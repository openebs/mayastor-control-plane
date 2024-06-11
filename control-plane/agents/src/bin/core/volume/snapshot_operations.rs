use crate::{
    controller::{
        io_engine::{
            types::{CreateNexusSnapReplDescr, CreateNexusSnapshot},
            NexusSnapshotApi, ReplicaSnapshotApi,
        },
        registry::Registry,
        resources::{
            operations::{ResourceLifecycleWithLifetime, ResourcePruning, ResourceSnapshotting},
            operations_helper::{GuardedOperationsHelper, OnCreateFail, OperationSequenceGuard},
            OperationGuardArc, ResourceMutex, ResourceUid, UpdateInnerValue,
        },
        scheduling::resources::ChildItem,
    },
    node::wrapper::{NodeWrapper, ReplicaSnapshotInfo},
    volume::snapshot_helpers::{snapshoteable_replica, PrepareVolumeSnapshot},
};
use agents::errors::SvcError;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            snapshots::{
                replica::{ReplicaSnapshot, ReplicaSnapshotSource, ReplicaSnapshotSpec},
                volume::{
                    VolumeSnapshot, VolumeSnapshotCompleter, VolumeSnapshotCreateInfo,
                    VolumeSnapshotCreateResult, VolumeSnapshotOperation, VolumeSnapshotSpecStatus,
                    VolumeSnapshotUserSpec,
                },
            },
            volume::{VolumeOperation, VolumeSpec, VolumeTarget},
        },
        transport::{
            CreateReplicaSnapshot, DestroyReplicaSnapshot, NodeId, SnapshotId, SnapshotParameters,
            SnapshotTxId, VolumeId,
        },
    },
};

use chrono::{DateTime, Utc};
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

#[async_trait::async_trait]
impl ResourceSnapshotting for OperationGuardArc<VolumeSpec> {
    type Create = VolumeSnapshotUserSpec;
    type CreateOutput = OperationGuardArc<VolumeSnapshot>;
    type Destroy = DestroyVolumeSnapshotRequest;
    type List = ();
    type ListOutput = ();

    async fn create_snap(
        &mut self,
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let state = registry.volume_state(request.source_id()).await?;

        if self.as_ref().num_replicas > 1 {
            registry.verify_rebuild_ancestry_fix().await?;
        }

        let operation = VolumeOperation::CreateSnapshot(request.uuid().clone());
        let spec_clone = self.start_update(registry, &state, operation).await?;

        let snap_result = OperationGuardArc::<VolumeSnapshot>::create(
            registry,
            &CreateVolumeSnapshotRequest {
                volume: self,
                request: request.clone(),
            },
        )
        .await;
        self.complete_update(registry, snap_result, spec_clone)
            .await
    }

    async fn list_snaps(
        &self,
        _registry: &Registry,
        _request: &Self::List,
    ) -> Result<Self::ListOutput, SvcError> {
        todo!()
    }

    async fn destroy_snap(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        // Get the snapshot operation guard
        let mut snapshot_guard = request.volume_snapshot.operation_guard_wait().await?;

        // Get the volume state and start update.
        let state = registry
            .volume_state(
                &request
                    .vol_id
                    .clone()
                    .ok_or(SvcError::InvalidArguments {})?,
            )
            .await?;
        let operation = VolumeOperation::DestroySnapshot(request.snap_id.clone());
        let spec_clone = self.start_update(registry, &state, operation).await?;

        // Execute snapshot destroy using snapshot guard.
        let result = snapshot_guard.destroy(registry, request).await;

        // Complete volume spec update.
        self.complete_update(registry, result, spec_clone).await
    }
}

/// Local create a volume snapshot request.
pub(crate) struct CreateVolumeSnapshotRequest<'a> {
    /// A mutable reference to the volume which will own this snapshot.
    /// This helps us mutate it if necessary but most of all ensure nothing else is modifying
    /// the volume.
    volume: &'a mut OperationGuardArc<VolumeSpec>,
    /// Any request specific info - TBD.
    request: VolumeSnapshotUserSpec,
}

/// Local delete a volume snapshot request.
pub(crate) struct DestroyVolumeSnapshotRequest {
    /// A mutable reference to the volume snapshot which will own this snapshot.
    volume_snapshot: ResourceMutex<VolumeSnapshot>,
    /// Source volume id, may not be present at time of deletion.
    vol_id: Option<VolumeId>,
    /// Id of snapshot undergoing deletion.
    snap_id: SnapshotId,
}

impl DestroyVolumeSnapshotRequest {
    /// Create a new DestroyVolumeSnapshotRequest.
    pub(crate) fn new(
        volume_snapshot: ResourceMutex<VolumeSnapshot>,
        vol_id: Option<VolumeId>,
        snap_id: SnapshotId,
    ) -> Self {
        Self {
            volume_snapshot,
            vol_id,
            snap_id,
        }
    }
}

#[async_trait::async_trait]
impl ResourceLifecycleWithLifetime for OperationGuardArc<VolumeSnapshot> {
    type Create<'a> = CreateVolumeSnapshotRequest<'a>;
    type CreateOutput = Self;
    type Destroy = DestroyVolumeSnapshotRequest;

    async fn create(
        registry: &Registry,
        request: &Self::Create<'_>,
    ) -> Result<Self::CreateOutput, SvcError> {
        let volume = &request.volume;
        let request = &request.request;

        let replicas = snapshoteable_replica(volume.as_ref(), registry).await?;
        let target_node = if let Some(target) = volume.as_ref().target() {
            let node = registry.node_wrapper(target.node()).await?;
            Some(node)
        } else {
            None
        };
        let specs = registry.specs();
        let mut snapshot = specs
            .get_or_create_snapshot(request)
            .operation_guard_wait()
            .await?;

        // Abort creation if we have reached max transactions limit.
        if snapshot.as_ref().metadata().transactions().len() > utils::SNAPSHOT_MAX_TRANSACTION_LIMIT
        {
            return Err(SvcError::SnapshotMaxTransactions {
                snap_id: snapshot.uuid().to_string(),
            });
        }

        // Try to prune 1 stale transaction, if present..
        snapshot.prune(registry, Some(1)).await.ok();

        let prepare_snapshot = snapshot.snapshot_params(&replicas)?;
        snapshot
            .start_create_update(
                registry,
                &VolumeSnapshotCreateInfo::new(
                    prepare_snapshot.parameters.txn_id(),
                    prepare_snapshot
                        .replica_snapshot
                        .iter()
                        .map(|(_, snapshot)| snapshot.clone())
                        .collect(),
                    &prepare_snapshot.completer,
                ),
            )
            .await?;

        let result = snapshot
            .snapshot(volume, &prepare_snapshot, registry, target_node)
            .await;
        if let Ok(ref result) = result {
            *prepare_snapshot.completer.lock().unwrap() = Some(result.clone());
        } else {
            // If we encounter any error cleanup the transaction.
            let result = snapshot.undo_transaction(registry).await;
            *prepare_snapshot.completer.lock().unwrap() = Some(result);
        }

        snapshot
            .complete_create(result, registry, OnCreateFail::LeaveAsIs)
            .await?;

        Ok(snapshot)
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        self.start_destroy(registry).await?;
        // Get the volume snapshot persisted info.
        let volume_snapshot = request.volume_snapshot.lock().clone();

        // Create a map to track the transactions upon deletion.
        let transactions = volume_snapshot.metadata().transactions().clone();
        let updated_transactions = Self::destroy_transactions(transactions, registry, None).await;

        if updated_transactions.is_empty() {
            // If there we no errors remove the persisted spec and complete destroy.
            self.lock().set_transactions(updated_transactions);
            self.update();
            self.complete_destroy(Ok(()), registry).await
        } else {
            // If errors were encountered then update the persisted spec with updated transactions.
            self.lock().set_transactions(updated_transactions);
            self.update();
            self.complete_destroy(
                Err(SvcError::Deleting {
                    kind: ResourceKind::VolumeSnapshot,
                }),
                registry,
            )
            .await
        }
    }
}

#[async_trait::async_trait]
impl ResourcePruning for OperationGuardArc<VolumeSnapshot> {
    async fn prune(
        &mut self,
        registry: &Registry,
        max_prune_limit: Option<usize>,
    ) -> Result<(), SvcError> {
        // Check if pruning is allowed based on status and transactions count. Also if no nodes are
        // online don't attempt.
        let transactions = self.as_ref().metadata().transactions().len();
        let pruning_allowed = match self.as_ref().status() {
            VolumeSnapshotSpecStatus::Creating => transactions > 0,
            VolumeSnapshotSpecStatus::Created(_) => transactions > 1,
            _ => false,
        };

        if !pruning_allowed || !self.check_nodes_availability(registry).await {
            return Ok(());
        }

        let spec = self.lock().clone();
        let mut spec_clone = self
            .start_update(
                registry,
                &spec,
                VolumeSnapshotOperation::CleanupStaleTransactions,
            )
            .await?;

        let data_set = spec.metadata().stale_transactions();
        // Create a map to track the remaining stale transactions upon cleanup.
        let remaining_stale_transactions =
            Self::destroy_transactions(data_set, registry, max_prune_limit).await;

        self.lock()
            .set_stale_transactions(remaining_stale_transactions.clone());
        spec_clone.set_stale_transactions(remaining_stale_transactions);

        self.complete_update(registry, Ok(()), spec_clone).await
    }
}

impl OperationGuardArc<VolumeSnapshot> {
    fn snapshot_params(
        &self,
        replicas: &Vec<ChildItem>,
    ) -> Result<PrepareVolumeSnapshot, SvcError> {
        let Some(parameters) = self.as_ref().prepare() else {
            return Err(SvcError::AlreadyExists {
                id: self.uuid().to_string(),
                kind: ResourceKind::VolumeSnapshot,
            });
        };
        let mut replica_snapshots = Vec::with_capacity(replicas.len());
        let volume = self.as_ref().spec().source_id();
        let generic_params = parameters.params().clone();
        for replica in replicas {
            let snapshot_source = ReplicaSnapshotSource::new(
                replica.spec().uid().clone(),
                replica.state().pool_id.clone(),
                replica.state().pool_uuid.clone().unwrap_or_default(),
            );
            let replica_snapshot = ReplicaSnapshot::new_vol(
                ReplicaSnapshotSpec::new(&snapshot_source, SnapshotId::new()),
                SnapshotParameters::new(volume, generic_params.clone()),
                replica.state().size,
                0,
                replica.spec().size,
            );
            let replica = replica.state().clone();
            replica_snapshots.push((replica, replica_snapshot));
        }

        Ok(PrepareVolumeSnapshot {
            parameters,
            replica_snapshot: replica_snapshots,
            completer: VolumeSnapshotCompleter::default(),
        })
    }
    async fn snapshot<N: NexusSnapshotApi + ReplicaSnapshotApi>(
        &self,
        volume: &OperationGuardArc<VolumeSpec>,
        prep_params: &PrepareVolumeSnapshot,
        registry: &Registry,
        target_node: Option<N>,
    ) -> Result<VolumeSnapshotCreateResult, SvcError> {
        let target = volume.as_ref().target();
        if target.is_some() && target_node.is_some() {
            self.snapshot_nexus::<N>(prep_params, target.unwrap(), registry, target_node.unwrap())
                .await
        } else {
            self.snapshot_replica::<N>(prep_params, registry).await
        }
    }

    async fn snapshot_nexus<N: NexusSnapshotApi>(
        &self,
        prep_params: &PrepareVolumeSnapshot,
        target: &VolumeTarget,
        registry: &Registry,
        target_node: N,
    ) -> Result<VolumeSnapshotCreateResult, SvcError> {
        let generic_params = prep_params.parameters.params();
        let nexus_snap_desc = prep_params
            .replica_snapshot
            .iter()
            .map(|(_, snapshot)| {
                CreateNexusSnapReplDescr::new(
                    snapshot.spec().source_id().replica_id(),
                    snapshot.spec().uuid().clone(),
                )
            })
            .collect::<Vec<_>>();
        let response = target_node
            .create_nexus_snapshot(&CreateNexusSnapshot::new(
                SnapshotParameters::new(target.nexus(), generic_params.clone()),
                nexus_snap_desc,
            ))
            .await?;

        if !response.skipped.is_empty() {
            return Err(SvcError::ReplicaSnapSkipped {
                replica: response
                    .skipped
                    .iter()
                    .map(|r| r.to_string())
                    .collect::<Vec<String>>()
                    .join(", "),
            });
        }
        let failed_replicas = response
            .replicas_status
            .iter()
            .filter(|&snap| snap.error.is_some())
            .collect::<Vec<_>>();
        if !failed_replicas.is_empty() {
            return Err(SvcError::ReplicaSnapError {
                failed_replicas: failed_replicas
                    .iter()
                    .map(|&snap| (snap.replica_uuid.to_string(), snap.error.unwrap()))
                    .collect::<Vec<_>>(),
            });
        }
        let timestamp = DateTime::<Utc>::from(response.snap_time);
        // What if snapshot succeeds but we can't fetch the replica snapshot, should we carry
        // on as following, or should we bail out?
        let mut replica_snapshots = prep_params.replica_snapshot.clone();
        for (replica, replica_snap) in replica_snapshots.iter_mut() {
            let node = registry.node_wrapper(&replica.node).await?;
            let snapshot = NodeWrapper::fetch_update_snapshot_state(
                &node,
                ReplicaSnapshotInfo::new(
                    replica_snap.spec().source_id().replica_id(),
                    replica_snap.spec().uuid().clone(),
                ),
            )
            .await?;

            replica_snap.complete_vol(
                snapshot.timestamp().into(),
                snapshot.replica_size(),
                snapshot.allocated_size() + snapshot.predecessor_alloc_size(),
            );
        }
        let snapshots = replica_snapshots
            .iter()
            .map(|(_, replica_snapshot)| replica_snapshot.clone())
            .collect::<Vec<_>>();
        Ok(VolumeSnapshotCreateResult::new_ok(snapshots, timestamp))
    }

    async fn snapshot_replica<N: ReplicaSnapshotApi>(
        &self,
        prep_params: &PrepareVolumeSnapshot,
        registry: &Registry,
    ) -> Result<VolumeSnapshotCreateResult, SvcError> {
        let volume_params = prep_params.parameters.params().clone();
        let mut timestamp = SystemTime::now();

        let mut replica_snapshots = prep_params.replica_snapshot.clone();
        for (replica, replica_snap) in replica_snapshots.iter_mut() {
            let replica_params = volume_params.clone().with_uuid(replica_snap.spec().uuid());
            let target_node = registry.node_wrapper(&replica.node).await?;
            let response = target_node
                .create_repl_snapshot(&CreateReplicaSnapshot::new(SnapshotParameters::new(
                    replica_snap.spec().source_id().replica_id(),
                    replica_params,
                )))
                .await?;
            timestamp = response.timestamp();
            replica_snap.complete_vol(
                timestamp.into(),
                response.replica_size(),
                response.allocated_size() + response.predecessor_alloc_size(),
            );
        }

        Ok(VolumeSnapshotCreateResult::new_ok(
            replica_snapshots
                .iter()
                .map(|(_, replica_snapshot)| replica_snapshot.clone())
                .collect::<Vec<_>>(),
            timestamp.into(),
        ))
    }

    async fn destroy_replica_snapshot(
        registry: &Registry,
        replica_snapshot: &ReplicaSnapshot,
    ) -> Result<(), SvcError> {
        let specs = registry.specs();
        let source = replica_snapshot.spec().source_id();

        // Get the pool using the replica snapshot's pool and extract the node_id.
        let node_id = specs.pool(source.pool_id())?.node;

        // Get the corresponding node wrapper for the same.
        let node_wrapper = registry.node_wrapper(&node_id).await?;

        // Execute the call for corresponding dataplane node.
        node_wrapper
            .destroy_repl_snapshot(&DestroyReplicaSnapshot::new(
                replica_snapshot.spec().uuid().clone(),
                source.pool_uuid().clone(),
            ))
            .await?;

        Ok(())
    }

    async fn check_nodes_availability(&self, registry: &Registry) -> bool {
        let txns = self.as_ref().metadata().stale_transactions_ref();
        let mut nodes: HashSet<NodeId> = HashSet::new();

        // Get the list of nodes from the list of replica_snaps.
        for snap in txns.flat_map(|(_, v)| v) {
            if let Ok(pool_spec) = registry.specs().pool(snap.spec().source_id().pool_id()) {
                nodes.insert(pool_spec.node);
            }
        }
        // If at least one node is online return true.
        for node in nodes {
            if let Ok(node_wrapper) = registry.node_wrapper(&node).await {
                if node_wrapper.read().await.is_online() {
                    return true;
                }
            }
        }
        false
    }

    async fn undo_transaction(&self, registry: &Registry) -> VolumeSnapshotCreateResult {
        let txn_id = self.as_ref().metadata().txn_id();
        let txns = self.as_ref().metadata().transactions();
        let failed = Self::destroy_transaction(registry, txns, txn_id).await;
        VolumeSnapshotCreateResult::new_err(failed)
    }

    async fn destroy_transactions(
        mut transactions: HashMap<SnapshotTxId, Vec<ReplicaSnapshot>>,
        registry: &Registry,
        max_prune_limit: Option<usize>,
    ) -> HashMap<SnapshotTxId, Vec<ReplicaSnapshot>> {
        for snapshots in transactions
            .values_mut()
            .take(max_prune_limit.unwrap_or(usize::MAX))
        {
            *snapshots = Self::destroy_transaction_snapshots(registry, snapshots).await;
        }
        transactions.retain(|_, failed_snaps| !failed_snaps.is_empty());
        transactions
    }

    async fn destroy_transaction(
        registry: &Registry,
        transactions: &HashMap<SnapshotTxId, Vec<ReplicaSnapshot>>,
        txn_delete: &SnapshotTxId,
    ) -> Vec<ReplicaSnapshot> {
        let Some((_, snapshots)) = transactions.iter().find(|(txn, _)| txn == &txn_delete) else {
            return Vec::new();
        };
        Self::destroy_transaction_snapshots(registry, snapshots).await
    }

    async fn destroy_transaction_snapshots(
        registry: &Registry,
        snapshots: &Vec<ReplicaSnapshot>,
    ) -> Vec<ReplicaSnapshot> {
        let mut failed = vec![];
        for snapshot in snapshots {
            if let Err(err) = Self::destroy_replica_snapshot(registry, snapshot).await {
                if err.tonic_code() != tonic::Code::NotFound {
                    let mut snapshot = snapshot.clone();
                    snapshot.set_status_deleting();
                    failed.push(snapshot);
                }
            }
        }
        failed
    }
}
