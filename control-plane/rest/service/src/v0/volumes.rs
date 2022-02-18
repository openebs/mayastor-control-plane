use super::*;
use common_lib::types::v0::{
    message_bus::{
        DestroyVolume, Filter, PublishVolume, SetVolumeReplica, ShareVolume, UnpublishVolume,
        UnshareVolume, Volume,
    },
    openapi::{apis::Uuid, models::VolumeShareProtocol},
};
use grpc::operations::volume::traits::VolumeOperations;

fn client() -> impl VolumeOperations {
    core_grpc().volume()
}

#[async_trait::async_trait]
impl apis::actix_server::Volumes for RestApi {
    async fn del_share(Path(volume_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        client()
            .unshare(
                &UnshareVolume {
                    uuid: volume_id.into(),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn del_volume(Path(volume_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        client()
            .destroy(
                &DestroyVolume {
                    uuid: volume_id.into(),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn del_volume_target(
        Path(volume_id): Path<Uuid>,
        Query(force): Query<Option<bool>>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = client()
            .unpublish(
                &UnpublishVolume::new(&volume_id.into(), force.unwrap_or(false)),
                None,
            )
            .await?;
        Ok(volume.into())
    }

    async fn get_volume(
        Path(volume_id): Path<Uuid>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = volume(
            volume_id.to_string(),
            client()
                .get(Filter::Volume(volume_id.into()), None)
                .await?
                .into_inner()
                .get(0),
        )?;
        Ok(volume.into())
    }

    async fn get_volumes() -> Result<Vec<models::Volume>, RestError<RestJsonError>> {
        let volumes = client().get(Filter::None, None).await?;
        Ok(volumes.into_inner().into_iter().map(From::from).collect())
    }

    async fn put_volume(
        Path(volume_id): Path<Uuid>,
        Body(create_volume_body): Body<models::CreateVolumeBody>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let create = CreateVolumeBody::from(create_volume_body).to_create_volume(volume_id.into());
        let volume = client().create(&create, None).await?;
        Ok(volume.into())
    }

    async fn put_volume_replica_count(
        Path((volume_id, replica_count)): Path<(Uuid, u8)>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = client()
            .set_volume_replica(
                &SetVolumeReplica {
                    uuid: volume_id.into(),
                    replicas: replica_count,
                },
                None,
            )
            .await?;
        Ok(volume.into())
    }

    async fn put_volume_share(
        Path((volume_id, protocol)): Path<(Uuid, models::VolumeShareProtocol)>,
    ) -> Result<String, RestError<RestJsonError>> {
        let share_uri = client()
            .share(
                &ShareVolume {
                    uuid: volume_id.into(),
                    protocol: protocol.into(),
                },
                None,
            )
            .await?;
        Ok(share_uri)
    }

    async fn put_volume_target(
        Path(volume_id): Path<Uuid>,
        Query((node, protocol)): Query<(String, VolumeShareProtocol)>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = client()
            .publish(
                &PublishVolume {
                    uuid: volume_id.into(),
                    target_node: Some(node.into()),
                    share: Some(protocol.into()),
                },
                None,
            )
            .await?;
        Ok(volume.into())
    }
}

/// returns volume from volume option and returns an error on non existence
fn volume(volume_id: String, volume: Option<&Volume>) -> Result<Volume, ReplyError> {
    match volume {
        Some(volume) => Ok(volume.clone()),
        None => Err(ReplyError {
            kind: ReplyErrorKind::NotFound,
            resource: ResourceKind::Volume,
            source: "Requested volume was not found".to_string(),
            extra: format!("Volume id : {}", volume_id),
        }),
    }
}
