use super::*;
use grpc::operations::volume::traits::{
    CreateVolumeSnapshot, DeleteVolumeSnapshot, ReplicaSnapshot, VolumeOperations,
    VolumeReplicaSnapshotState, VolumeSnapshot,
};
use humantime::Timestamp;
use models::ReplicaSnapshotStatus;
use rest_client::versions::v0::apis::Uuid;
use std::collections::HashMap;

fn client() -> impl VolumeOperations {
    core_grpc().volume()
}

#[async_trait::async_trait]
impl apis::actix_server::Snapshots for RestApi {
    async fn del_snapshot(Path(snapshot_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        client()
            .delete_snapshot(
                &DeleteVolumeSnapshot {
                    source_id: None,
                    snap_id: snapshot_id.into(),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn del_volume_snapshot(
        Path((volume_id, snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .delete_snapshot(
                &DeleteVolumeSnapshot {
                    source_id: Some(volume_id.into()),
                    snap_id: snapshot_id.into(),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn get_volume_snapshot(
        Path((volume_id, snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<models::VolumeSnapshot, RestError<RestJsonError>> {
        let snaps = client()
            .get_snapshots(
                Filter::VolumeSnapshot(volume_id.into(), snapshot_id.into()),
                true,
                None,
            )
            .await?;
        let snap = if let Some(s) = snaps.entries().first() {
            s
        } else {
            return Err(ReplyError::not_found(
                ResourceKind::VolumeSnapshot,
                "Snapshot not found".to_string(),
                snapshot_id.to_string(),
            )
            .into());
        };

        Ok(models::VolumeSnapshot {
            definition: models::VolumeSnapshotDefinition::new_all(
                models::VolumeSnapshotMetadata::new_all(
                    snap.meta().timestamp().map(|t| t.to_string()),
                    snap.meta().txn_id(),
                    std::collections::HashMap::new(),
                ),
                models::VolumeSnapshotSpec::new_all(snap.spec().snap_id(), snap.spec().source_id()),
            ),
            state: models::VolumeSnapshotState::new_all(
                snap.state().uuid(),
                snap.state().size().unwrap_or_default(),
                snap.state().source_id(),
                snap.state()
                    .timestamp()
                    .map(|t| t.to_string())
                    .unwrap_or_default(),
                snap.state().clone_ready(),
                Vec::<models::ReplicaSnapshotState>::new(),
            ),
        })
    }

    async fn get_volume_snapshots(
        Path(volume_id): Path<Uuid>,
    ) -> Result<models::VolumeSnapshots, RestError<RestJsonError>> {
        let snaps = client()
            .get_snapshots(Filter::Volume(volume_id.into()), true, None)
            .await?;
        Ok(models::VolumeSnapshots {
            entries: snaps
                .entries
                .into_iter()
                .map(|e| to_models_volume_snapshot(&e))
                .collect(),
        })
    }

    async fn get_volumes_snapshot(
        Path(snapshot_id): Path<Uuid>,
    ) -> Result<models::VolumeSnapshot, RestError<RestJsonError>> {
        let snaps = client()
            .get_snapshots(Filter::Snapshot(snapshot_id.into()), true, None)
            .await?;
        let snap = snaps.entries().first().ok_or_else(|| {
            ReplyError::not_found(
                ResourceKind::VolumeSnapshot,
                "Snapshot not found".to_string(),
                snapshot_id.to_string(),
            )
        })?;

        Ok(to_models_volume_snapshot(snap))
    }

    async fn get_volumes_snapshots(
        Query((volume_id, snapshot_id)): Query<(Option<Uuid>, Option<Uuid>)>,
    ) -> Result<models::VolumeSnapshots, RestError<RestJsonError>> {
        let filter = match (snapshot_id, volume_id) {
            (Some(snapshot), Some(volume)) => {
                Filter::VolumeSnapshot(volume.into(), snapshot.into())
            }
            (Some(snapshot), None) => Filter::Snapshot(snapshot.into()),
            (None, Some(volume)) => Filter::Volume(volume.into()),
            _ => Filter::None,
        };

        let snaps = client().get_snapshots(filter, true, None).await?;

        Ok(models::VolumeSnapshots {
            entries: snaps
                .entries
                .into_iter()
                .map(|e| to_models_volume_snapshot(&e))
                .collect(),
        })
    }

    async fn put_volume_snapshot(
        Path((volume_id, snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<models::VolumeSnapshot, RestError<RestJsonError>> {
        let request = CreateVolumeSnapshot::new(&volume_id.into(), snapshot_id.into());
        let snap = client().create_snapshot(&request, None).await?;
        Ok(to_models_volume_snapshot(&snap))
    }
}

fn to_models_volume_snapshot(snap: &VolumeSnapshot) -> models::VolumeSnapshot {
    models::VolumeSnapshot {
        definition: models::VolumeSnapshotDefinition::new_all(
            models::VolumeSnapshotMetadata::new_all(
                snap.meta().timestamp().map(|t| t.to_string()),
                snap.meta().txn_id(),
                snap.meta()
                    .transactions()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.iter().map(to_models_replica_snapshot).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<HashMap<_, _>>(),
            ),
            models::VolumeSnapshotSpec::new_all(snap.spec().snap_id(), snap.spec().source_id()),
        ),
        state: models::VolumeSnapshotState::new_all(
            snap.state().uuid(),
            snap.state().size().unwrap_or_default(),
            snap.state().source_id(),
            snap.state()
                .timestamp()
                .map(|t| t.to_string())
                .unwrap_or_default(),
            snap.state().clone_ready(),
            snap.state()
                .repl_snapshots()
                .iter()
                .map(to_models_replica_snapshot_state)
                .collect::<Vec<_>>(),
        ),
    }
}

fn to_models_replica_snapshot(repl_snap: &ReplicaSnapshot) -> models::ReplicaSnapshot {
    models::ReplicaSnapshot {
        uuid: repl_snap.uuid().to_owned(),
        source_id: repl_snap.source_id().to_owned(),
        status: repl_snap.status().into(),
    }
}

fn to_models_replica_snapshot_state(
    repl_snap_state: &VolumeReplicaSnapshotState,
) -> models::ReplicaSnapshotState {
    match repl_snap_state {
        VolumeReplicaSnapshotState::Online { pool_id: _, state } => models::ReplicaSnapshotState {
            uuid: state.snap_uuid().uuid().to_owned(),
            source_id: state.replica_uuid().uuid().to_owned(),
            timestamp: Timestamp::from(state.timestamp()).to_string(),
            size: state.source_size() as i64,
            referenced_size: state.snap_size() as i64,
            state: ReplicaSnapshotStatus::Online,
        },
        VolumeReplicaSnapshotState::Offline {
            replica_id,
            pool_id: _,
            pool_uuid: _,
            snapshot_id,
        } => models::ReplicaSnapshotState {
            uuid: snapshot_id.uuid().to_owned(),
            source_id: replica_id.uuid().to_owned(),
            timestamp: "".to_string(),
            size: 0,
            referenced_size: 0,
            state: ReplicaSnapshotStatus::Offline,
        },
    }
}
