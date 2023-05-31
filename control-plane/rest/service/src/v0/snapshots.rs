use super::*;
use grpc::operations::volume::traits::{
    CreateVolumeSnapshot, DeleteVolumeSnapshot, VolumeOperations,
};
use rest_client::versions::v0::{apis::Uuid, models::VolumeSnapshot};

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

    async fn get_snapshots() -> Result<Vec<VolumeSnapshot>, RestError<RestJsonError>> {
        Err(ReplyError::unimplemented("Snapshot service is not implemented".to_string()).into())
    }

    async fn get_volume_snapshot(
        Path((_volume_id, _snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<VolumeSnapshot, RestError<RestJsonError>> {
        Err(ReplyError::unimplemented("Snapshot service is not implemented".to_string()).into())
    }

    async fn get_volume_snapshots(
        Path(_volume_id): Path<Uuid>,
    ) -> Result<Vec<VolumeSnapshot>, RestError<RestJsonError>> {
        Err(ReplyError::unimplemented("Snapshot service is not implemented".to_string()).into())
    }

    async fn put_volume_snapshot(
        Path((volume_id, snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<VolumeSnapshot, RestError<RestJsonError>> {
        let request = CreateVolumeSnapshot::new(&volume_id.into(), snapshot_id.into());
        let snap = client().create_snapshot(&request, None).await?;
        Ok(VolumeSnapshot {
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
                snap.state().size(),
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
}
