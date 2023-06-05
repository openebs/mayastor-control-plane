use super::*;
use grpc::operations::volume::traits::{
    CreateVolumeSnapshot, DeleteVolumeSnapshot, VolumeOperations, VolumeSnapshot,
};
use rest_client::versions::v0::apis::Uuid;

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
        Query((snapshot_id, volume_id)): Query<(Option<Uuid>, Option<Uuid>)>,
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

pub fn to_models_volume_snapshot(snap: &VolumeSnapshot) -> models::VolumeSnapshot {
    models::VolumeSnapshot {
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
    }
}
