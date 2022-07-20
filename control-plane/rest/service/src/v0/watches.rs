use super::*;
use common_lib::types::v0::{
    openapi::apis::Uuid,
    transport::{CreateWatch, DeleteWatch, GetWatches, WatchCallback, WatchResourceId, WatchType},
};
use grpc::operations::watch::traits::WatchOperations;
use std::convert::TryFrom;

fn client() -> impl WatchOperations {
    core_grpc().watch()
}

#[async_trait::async_trait]
impl apis::actix_server::Watches for RestApi {
    async fn del_watch_volume(
        Path(volume_id): Path<Uuid>,
        Query(callback): Query<url::Url>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .destroy(
                &DeleteWatch {
                    id: WatchResourceId::Volume(volume_id.into()),
                    callback: WatchCallback::Uri(callback.to_string()),
                    watch_type: WatchType::Actual,
                },
                None,
            )
            .await?;

        Ok(())
    }

    async fn get_watch_volume(
        Path(volume_id): Path<Uuid>,
    ) -> Result<Vec<models::RestWatch>, RestError<RestJsonError>> {
        let watches = client()
            .get(
                &GetWatches {
                    resource: WatchResourceId::Volume(volume_id.into()),
                },
                None,
            )
            .await?;
        let watches = watches.0.iter();
        let watches = watches
            .filter_map(|w| models::RestWatch::try_from(w).ok())
            .collect();
        Ok(watches)
    }

    async fn put_watch_volume(
        Path(volume_id): Path<Uuid>,
        Query(callback): Query<url::Url>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .create(
                &CreateWatch {
                    id: WatchResourceId::Volume(volume_id.into()),
                    callback: WatchCallback::Uri(callback.to_string()),
                    watch_type: WatchType::Actual,
                },
                None,
            )
            .await?;

        Ok(())
    }
}
