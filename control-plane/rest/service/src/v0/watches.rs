use super::*;
use common_lib::types::v0::message_bus::{
    CreateWatch, DeleteWatch, GetWatchers, WatchCallback, WatchResourceId, WatchType,
};
use mbus_api::Message;
use std::convert::TryFrom;

#[async_trait::async_trait]
impl apis::Watches for RestApi {
    async fn del_watch_volume(
        Path(volume_id): Path<String>,
        Query(callback): Query<url::Url>,
    ) -> Result<(), RestError<RestJsonError>> {
        DeleteWatch {
            id: WatchResourceId::Volume(volume_id.into()),
            callback: WatchCallback::Uri(callback.to_string()),
            watch_type: WatchType::Actual,
        }
        .request()
        .await?;

        Ok(())
    }

    async fn get_watch_volume(
        Path(volume_id): Path<String>,
    ) -> Result<Vec<models::RestWatch>, RestError<RestJsonError>> {
        let watches = GetWatchers {
            resource: WatchResourceId::Volume(volume_id.into()),
        }
        .request()
        .await?;
        let watches = watches.0.iter();
        let watches = watches
            .filter_map(|w| models::RestWatch::try_from(w).ok())
            .collect();
        Ok(watches)
    }

    async fn put_watch_volume(
        Path(volume_id): Path<String>,
        Query(callback): Query<url::Url>,
    ) -> Result<(), RestError<RestJsonError>> {
        CreateWatch {
            id: WatchResourceId::Volume(volume_id.into()),
            callback: WatchCallback::Uri(callback.to_string()),
            watch_type: WatchType::Actual,
        }
        .request()
        .await?;

        Ok(())
    }
}
