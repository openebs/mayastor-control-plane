use super::*;
use common_lib::types::v0::message_bus::{
    DestroyVolume, Filter, NexusShareProtocol, ShareNexus, UnshareNexus, VolumeId,
};
use mbus_api::{
    message_bus::v0::{MessageBus, MessageBusTrait},
    ReplyError, ReplyErrorKind, ResourceKind,
};

async fn volume_share(
    volume_id: VolumeId,
    protocol: NexusShareProtocol,
) -> Result<String, RestError<RestJsonError>> {
    let volume = MessageBus::get_volume(Filter::Volume(volume_id.clone())).await?;

    // TODO: For ANA we will want to share all nexuses not just the first.
    match volume.children.first() {
        Some(nexus) => MessageBus::share_nexus(ShareNexus {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
            key: None,
            protocol,
        })
        .await
        .map_err(From::from),
        None => Err(RestError::from(ReplyError {
            kind: ReplyErrorKind::NotFound,
            resource: ResourceKind::Nexus,
            source: "".to_string(),
            extra: format!("No nexuses found for volume {}", volume_id),
        })),
    }
}

async fn volume_unshare(volume_id: VolumeId) -> Result<(), RestError<RestJsonError>> {
    let volume = MessageBus::get_volume(Filter::Volume(volume_id.clone())).await?;

    match volume.children.first() {
        Some(nexus) => MessageBus::unshare_nexus(UnshareNexus {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
        })
        .await
        .map_err(RestError::from),
        None => Err(RestError::from(ReplyError {
            kind: ReplyErrorKind::NotFound,
            resource: ResourceKind::Nexus,
            source: "".to_string(),
            extra: format!("No nexuses found for volume {}", volume_id),
        })),
    }?;
    Ok(())
}

#[async_trait::async_trait]
impl apis::Volumes for RestApi {
    async fn del_share(Path(volume_id): Path<String>) -> Result<(), RestError<RestJsonError>> {
        volume_unshare(volume_id.into()).await
    }

    async fn del_volume(Path(volume_id): Path<String>) -> Result<(), RestError<RestJsonError>> {
        let request = DestroyVolume {
            uuid: volume_id.into(),
        };
        MessageBus::delete_volume(request).await?;
        Ok(())
    }

    async fn get_node_volume(
        Path((node_id, volume_id)): Path<(String, String)>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume =
            MessageBus::get_volume(Filter::NodeVolume(node_id.into(), volume_id.into())).await?;
        Ok(volume.into())
    }

    async fn get_node_volumes(
        Path(node_id): Path<String>,
    ) -> Result<Vec<models::Volume>, RestError<RestJsonError>> {
        let volumes = MessageBus::get_volumes(Filter::Node(node_id.into())).await?;
        Ok(volumes.into_iter().map(From::from).collect())
    }

    async fn get_volume(
        Path(volume_id): Path<String>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = MessageBus::get_volume(Filter::Volume(volume_id.into())).await?;
        Ok(volume.into())
    }

    async fn get_volumes() -> Result<Vec<models::Volume>, RestError<RestJsonError>> {
        let volumes = MessageBus::get_volumes(Filter::None).await?;
        Ok(volumes.into_iter().map(From::from).collect())
    }

    async fn put_volume(
        Path(volume_id): Path<String>,
        Body(create_volume_body): Body<models::CreateVolumeBody>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let create = CreateVolumeBody::from(create_volume_body).bus_request(volume_id.into());
        let volume = MessageBus::create_volume(create).await?;
        Ok(volume.into())
    }

    async fn put_volume_share(
        Path((volume_id, protocol)): Path<(String, models::VolumeShareProtocol)>,
    ) -> Result<String, RestError<RestJsonError>> {
        volume_share(volume_id.into(), protocol.into()).await
    }
}
