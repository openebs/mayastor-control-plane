use crate::{
    controller::{
        io_engine::{NexusSnapshotApi, ReplicaSnapshotApi},
        registry::Registry,
        resources::{
            operations::{ResourceLifecycleWithLifetime, ResourceSnapshotting},
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
                replica::{ReplicaSnapshot, ReplicaSnapshotSpec},
                volume::{
                    VolumeSnapshot, VolumeSnapshotCompleter, VolumeSnapshotCreateInfo,
                    VolumeSnapshotCreateResult, VolumeSnapshotUserSpec,
                },
            },
            volume::{VolumeOperation, VolumeSpec, VolumeTarget},
        },
        transport::{
            CreateNexusSnapReplDescr, CreateNexusSnapshot, CreateReplicaSnapshot,
            DestroyReplicaSnapshot, SnapshotId, SnapshotParameters, SnapshotTxId,
        },
    },
};

use chrono::{DateTime, Utc};
use std::collections::HashMap;

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
        let state = registry.volume_state(request.info.source_id()).await?;
        let operation = VolumeOperation::DestroySnapshot(request.info.uuid().clone());
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
    /// Request specific info.
    info: VolumeSnapshotUserSpec,
}

impl DestroyVolumeSnapshotRequest {
    /// Create a new DestroyVolumeSnapshotRequest.
    pub fn new(
        volume_snapshot: ResourceMutex<VolumeSnapshot>,
        info: VolumeSnapshotUserSpec,
    ) -> Self {
        Self {
            volume_snapshot,
            info,
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

        if volume.as_ref().num_replicas != 1 {
            return Err(SvcError::NReplSnapshotNotAllowed {});
        }

        let replica = snapshoteable_replica(volume.as_ref(), registry).await?;
        let target_node = if let Some(target) = volume.as_ref().target() {
            registry.node_wrapper(target.node()).await
        } else {
            registry.node_wrapper(&replica.state().node).await
        }?;

        let specs = registry.specs();
        let mut snapshot = specs
            .get_or_create_snapshot(request)
            .operation_guard_wait()
            .await?;

        let prepare_snapshot = snapshot.snapshot_params(&replica)?;
        snapshot
            .start_create_update(
                registry,
                &VolumeSnapshotCreateInfo::new(
                    prepare_snapshot.parameters.txn_id(),
                    prepare_snapshot.replica_snapshot.1.clone(),
                    &prepare_snapshot.completer,
                ),
            )
            .await?;

        let result = snapshot
            .snapshot(volume, &prepare_snapshot, registry, target_node)
            .await;
        if let Ok(ref result) = result {
            *prepare_snapshot.completer.lock().unwrap() = Some(result.clone());
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
        let mut updated_transactions: HashMap<SnapshotTxId, Vec<ReplicaSnapshot>> = HashMap::new();

        // Iterate over current transactions and destroy each replica snapshot in a transaction,
        // store the transactions and replica snapshots whose deletion failed.
        for (txn_id, snap_list) in volume_snapshot.metadata().transactions() {
            for snap in snap_list {
                if let Err(err) =
                    OperationGuardArc::<VolumeSnapshot>::destroy_replica_snapshot(snap, registry)
                        .await
                {
                    if !matches!(err.tonic_code(), tonic::Code::NotFound) {
                        let mut snap = snap.clone();
                        // If it was an error mark the spec as deleting and push it in a map.
                        snap.set_status_deleting();
                        updated_transactions
                            .entry(txn_id.clone())
                            .or_insert_with(Vec::new)
                            .push(snap);
                    }
                }
            }
        }

        if updated_transactions.is_empty() {
            // If there we no errors remove the persisted spec and complete destroy.
            self.lock().set_transactions(updated_transactions);
            self.update();
            self.complete_destroy(Ok(()), registry).await?;
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
            .await?;
        }
        Ok(())
    }
}

impl OperationGuardArc<VolumeSnapshot> {
    fn snapshot_params(&self, replica: &ChildItem) -> Result<PrepareVolumeSnapshot, SvcError> {
        let Some(parameters) = self.as_ref().prepare() else {
            return Err(SvcError::AlreadyExists {
                id: self.uuid().to_string(),
                kind: ResourceKind::VolumeSnapshot
            })
        };
        let volume = self.as_ref().spec().source_id();
        let generic_params = parameters.params().clone();
        let replica_snapshot = ReplicaSnapshot::new_vol(
            ReplicaSnapshotSpec::new(replica.spec().uid(), SnapshotId::new()),
            SnapshotParameters::new(volume, generic_params),
            replica.spec().size,
        );
        let replica = replica.state().clone();
        Ok(PrepareVolumeSnapshot {
            parameters,
            replica_snapshot: (replica, replica_snapshot),
            completer: VolumeSnapshotCompleter::default(),
        })
    }
    async fn snapshot<N: NexusSnapshotApi + ReplicaSnapshotApi>(
        &self,
        volume: &OperationGuardArc<VolumeSpec>,
        prep_params: &PrepareVolumeSnapshot,
        registry: &Registry,
        target_node: N,
    ) -> Result<VolumeSnapshotCreateResult, SvcError> {
        if let Some(target) = volume.as_ref().target() {
            self.snapshot_nexus(prep_params, target, registry, target_node)
                .await
        } else {
            self.snapshot_replica(prep_params, target_node).await
        }
    }

    async fn snapshot_nexus<N: NexusSnapshotApi>(
        &self,
        prep_params: &PrepareVolumeSnapshot,
        target: &VolumeTarget,
        registry: &Registry,
        target_node: N,
    ) -> Result<VolumeSnapshotCreateResult, SvcError> {
        let mut replica_snap = prep_params.replica_snapshot.1.clone();
        let replica = &prep_params.replica_snapshot.0;
        let generic_params = prep_params.parameters.params();

        let response = target_node
            .create_nexus_snapshot(&CreateNexusSnapshot::new(
                SnapshotParameters::new(target.nexus(), generic_params.clone()),
                vec![CreateNexusSnapReplDescr::new(
                    replica_snap.spec().source_id(),
                    replica_snap.spec().uuid().clone(),
                )],
            ))
            .await?;

        if response.skipped.contains(replica_snap.spec().source_id())
            || !response.skipped.is_empty()
        {
            return Err(SvcError::ReplicaSnapSkipped {
                replica: replica_snap.spec().uuid().to_string(),
            });
        }

        let snapped = match response.replicas_status.as_slice() {
            [snapped] if &snapped.replica_uuid == replica_snap.spec().source_id() => Ok(snapped),
            _ => Err(SvcError::ReplicaSnapMiss {
                replica: replica_snap.spec().uuid().to_string(),
            }),
        }?;

        if snapped.status != 0 {
            return Err(SvcError::ReplicaSnapError {
                replica: replica_snap.spec().uuid().to_string(),
                status: snapped.status,
            });
        }

        let timestamp = DateTime::<Utc>::from(response.snap_time);
        let mut replica_timestamp = timestamp;
        // What if snapshot succeeds but we can't fetch the replica snapshot, should we carry
        // on as following, or should we bail out?
        if let Ok(node) = registry.node_wrapper(&replica.node).await {
            if let Ok(snapshot) = NodeWrapper::fetch_update_snapshot_state(
                &node,
                ReplicaSnapshotInfo::new(
                    replica_snap.spec().source_id(),
                    replica_snap.spec().uuid().clone(),
                ),
            )
            .await
            {
                replica_timestamp = snapshot.timestamp();
            }
        }

        replica_snap.complete_vol(replica_timestamp);
        Ok(VolumeSnapshotCreateResult::new(replica_snap, timestamp))
    }
    async fn snapshot_replica<N: ReplicaSnapshotApi>(
        &self,
        prep_params: &PrepareVolumeSnapshot,
        target_node: N,
    ) -> Result<VolumeSnapshotCreateResult, SvcError> {
        let mut replica_snap = prep_params.replica_snapshot.clone();
        let generic_params = prep_params.parameters.params();

        let response = target_node
            .create_repl_snapshot(&CreateReplicaSnapshot::new(SnapshotParameters::new(
                prep_params.replica_snapshot.1.spec().source_id(),
                generic_params.clone(),
            )))
            .await?;

        let timestamp = response.timestamp();
        replica_snap.1.complete_vol(timestamp);

        Ok(VolumeSnapshotCreateResult::new(replica_snap.1, timestamp))
    }

    async fn destroy_replica_snapshot(
        replica_snapshot: &ReplicaSnapshot,
        registry: &Registry,
    ) -> Result<(), SvcError> {
        let specs = registry.specs();

        // Fetch the replica spec for the snapshot's source replica.
        let replica_spec = specs
            .replica_rsc(replica_snapshot.spec().source_id())
            .ok_or(SvcError::ReplicaNotFound {
                replica_id: replica_snapshot.spec().source_id().clone(),
            })?
            .lock()
            .clone();

        // Get the pool using the replica spec and extract the node_id.
        let node_id = specs.pool(replica_spec.pool_name())?.node;

        // Get the corresponding node wrapper for the same.
        let node_wrapper = registry.node_wrapper(&node_id).await?;

        // Execute the call for corresponding dataplane node.
        node_wrapper
            .destroy_repl_snapshot(&DestroyReplicaSnapshot::new(
                replica_snapshot.spec().uuid().clone(),
            ))
            .await?;

        Ok(())
    }
}
