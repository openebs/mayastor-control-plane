use super::*;
use grpc::operations::{
    volume::traits::{
        CreateSnapshotGroup, CreateVolumeSnapshot, DestroySnapshotGroup, DestroyVolumeSnapshot,
        ReplicaSnapshot, SnapshotGroup, VolumeOperations, VolumeReplicaSnapshotState,
        VolumeSnapshot,
    },
    MaxEntries, Pagination, StartingToken,
};
use humantime::Timestamp;
use rest_client::versions::v0::apis::Uuid;
use std::{collections::HashMap, convert::TryFrom};
use stor_port::types::v0::{
    store::snapshots::group::VolumeSnapshotGroupQuiesce,
    transport::{SnapshotId, VolumeId},
};

fn client() -> impl VolumeOperations {
    core_grpc().volume()
}

#[async_trait::async_trait]
impl apis::actix_server::Snapshots for RestApi {
    async fn del_snapshot(Path(snapshot_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        client()
            .destroy_snapshot(
                &DestroyVolumeSnapshot {
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
            .destroy_snapshot(
                &DestroyVolumeSnapshot {
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
                None,
            )
            .await?;
        let snapshot = snaps.entries().first().ok_or_else(|| {
            ReplyError::not_found(
                ResourceKind::VolumeSnapshot,
                "Snapshot not found".to_string(),
                snapshot_id.to_string(),
            )
        })?;

        Ok(to_models_volume_snapshot(snapshot))
    }

    async fn get_volume_snapshots(
        Path(volume_id): Path<Uuid>,
        Query((max_entries, starting_token)): Query<(isize, Option<isize>)>,
    ) -> Result<models::VolumeSnapshots, RestError<RestJsonError>> {
        let starting_token = starting_token.unwrap_or_default();
        // If max entries is 0, pagination is disabled. All snapshots will be returned in a single
        // call.
        let pagination = if max_entries > 0 {
            Some(Pagination::new(
                max_entries as MaxEntries,
                starting_token as StartingToken,
            ))
        } else {
            None
        };

        let snaps = client()
            .get_snapshots(Filter::Volume(volume_id.into()), true, pagination, None)
            .await?;
        Ok(models::VolumeSnapshots {
            next_token: snaps.next_token().map(|t| t as isize),
            entries: snaps
                .entries()
                .iter()
                .map(to_models_volume_snapshot)
                .collect(),
        })
    }

    async fn get_volumes_snapshot(
        Path(snapshot_id): Path<Uuid>,
    ) -> Result<models::VolumeSnapshot, RestError<RestJsonError>> {
        let snaps = client()
            .get_snapshots(Filter::Snapshot(snapshot_id.into()), true, None, None)
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
        Query((snapshot_id, volume_id, max_entries, starting_token)): Query<(
            Option<Uuid>,
            Option<Uuid>,
            isize,
            Option<isize>,
        )>,
    ) -> Result<models::VolumeSnapshots, RestError<RestJsonError>> {
        let starting_token = starting_token.unwrap_or_default();
        // If max entries is 0, pagination is disabled. All snapshots will be returned in a single
        // call.
        let pagination = if max_entries > 0 {
            Some(Pagination::new(
                max_entries as MaxEntries,
                starting_token as StartingToken,
            ))
        } else {
            None
        };

        let filter = match (snapshot_id, volume_id) {
            (Some(snapshot), Some(volume)) => {
                Filter::VolumeSnapshot(volume.into(), snapshot.into())
            }
            (Some(snapshot), None) => Filter::Snapshot(snapshot.into()),
            (None, Some(volume)) => Filter::Volume(volume.into()),
            _ => Filter::None,
        };

        let snaps = client()
            .get_snapshots(filter, true, pagination, None)
            .await?;

        Ok(models::VolumeSnapshots {
            next_token: snaps.next_token().map(|t| t as isize),
            entries: snaps
                .entries()
                .iter()
                .map(to_models_volume_snapshot)
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

    async fn del_snapshot_group(
        Path(group_id): Path<Uuid>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .destroy_snapshot_group(&DestroySnapshotGroup::new(group_id.into()), None)
            .await?;
        Ok(())
    }

    async fn get_snapshot_group(
        Path(group_id): Path<Uuid>,
    ) -> Result<models::VolumeSnapshotGroup, RestError<RestJsonError>> {
        let groups = client()
            .get_snapshot_groups(Some(group_id.into()), None)
            .await?;
        let group = groups.entries().first().ok_or_else(|| {
            ReplyError::not_found(
                ResourceKind::VolumeSnapshotGroup,
                "Snapshot group not found".to_string(),
                group_id.to_string(),
            )
        })?;
        Ok(to_models_snapshot_group(group))
    }

    async fn get_snapshot_groups() -> Result<models::VolumeSnapshotGroups, RestError<RestJsonError>>
    {
        let groups = client().get_snapshot_groups(None, None).await?;
        Ok(models::VolumeSnapshotGroups {
            entries: groups
                .entries()
                .iter()
                .map(to_models_snapshot_group)
                .collect(),
        })
    }

    async fn put_snapshot_group(
        Path(group_id): Path<Uuid>,
        Body(body): Body<models::CreateVolumeSnapshotGroupBody>,
    ) -> Result<models::VolumeSnapshotGroup, RestError<RestJsonError>> {
        let members = body
            .members
            .into_iter()
            .map(|(volume_id, snapshot_id)| {
                let volume_id = VolumeId::try_from(volume_id).map_err(|error| {
                    ReplyError::invalid_argument(
                        ResourceKind::VolumeSnapshotGroup,
                        "members.volume_id",
                        error,
                    )
                })?;
                Ok((volume_id, SnapshotId::from(snapshot_id)))
            })
            .collect::<Result<HashMap<VolumeId, SnapshotId>, ReplyError>>()?;
        let quiesce = match body.quiesce.unwrap_or_default() {
            models::SnapshotGroupQuiesce::Freeze => VolumeSnapshotGroupQuiesce::Freeze,
            models::SnapshotGroupQuiesce::None => VolumeSnapshotGroupQuiesce::None,
        };
        let request = CreateSnapshotGroup::new(group_id.into(), members, quiesce);
        let group = client().create_snapshot_group(&request, None).await?;
        Ok(to_models_snapshot_group(&group))
    }
}

fn to_models_snapshot_group(group: &SnapshotGroup) -> models::VolumeSnapshotGroup {
    models::VolumeSnapshotGroup::new_all(
        group.group_id().uuid().to_owned(),
        group
            .members()
            .iter()
            .map(|(volume_id, snapshot_id)| (volume_id.to_string(), snapshot_id.uuid().to_owned()))
            .collect::<HashMap<_, _>>(),
        group.status().clone(),
        group.timestamp().map(|t| t.to_string()),
        match group.quiesce() {
            VolumeSnapshotGroupQuiesce::Freeze => models::SnapshotGroupQuiesce::Freeze,
            VolumeSnapshotGroupQuiesce::None => models::SnapshotGroupQuiesce::None,
        },
        group
            .snapshots()
            .iter()
            .map(to_models_volume_snapshot)
            .collect::<Vec<_>>(),
    )
}

fn to_models_volume_snapshot(snap: &VolumeSnapshot) -> models::VolumeSnapshot {
    models::VolumeSnapshot {
        definition: models::VolumeSnapshotDefinition::new_all(
            models::VolumeSnapshotMetadata::new_all(
                snap.meta().status().clone(),
                snap.meta().timestamp().map(|t| t.to_string()),
                snap.meta().size(),
                snap.meta().spec_size(),
                snap.meta().spec_repl_size(),
                Some(snap.meta().label_version()),
                snap.meta().total_allocated_size(),
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
                snap.meta().num_restores(),
                snap.meta().num_snapshot_replicas(),
            ),
            models::VolumeSnapshotSpec::new_all(snap.spec().snap_id(), snap.spec().source_id()),
        ),
        state: models::VolumeSnapshotState::new_all(
            snap.state().uuid(),
            snap.state().allocated_size().unwrap_or_default(),
            snap.state().source_id(),
            snap.state().timestamp().map(|t| t.to_string()),
            snap.state().ready_as_source(),
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
        VolumeReplicaSnapshotState::Online { pool_id, state } => {
            models::ReplicaSnapshotState::online(models::OnlineReplicaSnapshotState {
                uuid: state.snap_uuid().uuid().to_owned(),
                source_id: state.replica_uuid().uuid().to_owned(),
                pool_id: pool_id.to_string(),
                pool_uuid: state.pool_uuid().uuid().to_owned(),
                timestamp: Timestamp::from(state.timestamp()).to_string(),
                size: state.replica_size(),
                allocated_size: state.allocated_size(),
                predecessor_alloc_size: state.predecessor_alloc_size(),
            })
        }
        VolumeReplicaSnapshotState::Offline {
            replica_id,
            pool_id,
            pool_uuid,
            snapshot_id,
        } => models::ReplicaSnapshotState::offline(models::OfflineReplicaSnapshotState {
            uuid: snapshot_id.uuid().to_owned(),
            source_id: replica_id.uuid().to_owned(),
            pool_id: pool_id.to_string(),
            pool_uuid: pool_uuid.uuid().to_owned(),
        }),
    }
}
