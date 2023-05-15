use super::*;
use rest_client::versions::v0::{apis::Uuid, models::VolumeSnapshot};

#[async_trait::async_trait]
impl apis::actix_server::Snapshots for RestApi {
    async fn del_snapshot(Path(_snapshot_id): Path<Uuid>) -> Result<(), RestError<RestJsonError>> {
        Err(ReplyError::unimplemented("Snapshot deletion is not implemented".to_string()).into())
    }

    async fn del_volume_snapshot(
        Path((_volume_id, _snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<(), RestError<RestJsonError>> {
        Err(ReplyError::unimplemented("Snapshot deletion is not implemented".to_string()).into())
    }

    async fn put_volume_snapshot(
        Path((_volume_id, _snapshot_id)): Path<(Uuid, Uuid)>,
    ) -> Result<VolumeSnapshot, RestError<RestJsonError>> {
        Err(ReplyError::unimplemented("Snapshot creation is not implemented".to_string()).into())
    }
}
