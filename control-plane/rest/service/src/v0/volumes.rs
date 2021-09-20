use super::*;
use common_lib::types::v0::{
    message_bus::{DestroyVolume, Filter},
    openapi::{apis::Uuid, models::VolumeShareProtocol},
};
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

#[async_trait::async_trait]
impl apis::Volumes for RestApi {
    async fn del_share(Path(volume_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        MessageBus::unshare_volume(volume_id.into()).await?;
        Ok(())
    }

    async fn del_volume(Path(volume_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        let request = DestroyVolume {
            uuid: volume_id.into(),
        };
        MessageBus::delete_volume(request).await?;
        Ok(())
    }

    async fn del_volume_target(
        Path(volume_id): Path<Uuid>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = MessageBus::unpublish_volume(volume_id.into()).await?;
        Ok(volume.into())
    }

    async fn get_node_volumes(
        Path(node_id): Path<String>,
    ) -> Result<Vec<models::Volume>, RestError<RestJsonError>> {
        let volumes = MessageBus::get_volumes(Filter::Node(node_id.into())).await?;
        Ok(volumes.into_iter().map(From::from).collect())
    }

    async fn get_volume(
        Path(volume_id): Path<Uuid>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = MessageBus::get_volume(Filter::Volume(volume_id.into())).await?;
        Ok(volume.into())
    }

    async fn get_volumes() -> Result<Vec<models::Volume>, RestError<RestJsonError>> {
        let volumes = MessageBus::get_volumes(Filter::None).await?;
        Ok(volumes.into_iter().map(From::from).collect())
    }

    async fn put_volume(
        Path(volume_id): Path<Uuid>,
        Body(create_volume_body): Body<models::CreateVolumeBody>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let create = CreateVolumeBody::from(create_volume_body).bus_request(volume_id.into());
        let volume = MessageBus::create_volume(create).await?;
        Ok(volume.into())
    }

    async fn put_volume_replica_count(
        Path((volume_id, replica_count)): Path<(Uuid, u8)>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume = MessageBus::set_volume_replica(volume_id.into(), replica_count).await?;
        Ok(volume.into())
    }

    async fn put_volume_share(
        Path((volume_id, protocol)): Path<(Uuid, models::VolumeShareProtocol)>,
    ) -> Result<String, RestError<RestJsonError>> {
        let share_uri = MessageBus::share_volume(volume_id.into(), protocol.into()).await?;
        Ok(share_uri)
    }

    async fn put_volume_target(
        Path(volume_id): Path<Uuid>,
        Query((node, protocol)): Query<(String, VolumeShareProtocol)>,
    ) -> Result<models::Volume, RestError<RestJsonError>> {
        let volume =
            MessageBus::publish_volume(volume_id.into(), Some(node.into()), Some(protocol.into()))
                .await?;
        Ok(volume.into())
    }
}
