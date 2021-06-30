use super::*;
use common_lib::types::v0::message_bus::{
    CreateWatch, DeleteWatch, GetWatchers, VolumeId, WatchCallback, WatchResourceId, WatchType,
};
use mbus_api::Message;
use std::convert::TryFrom;

pub(super) fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.service(put_watch)
        .service(del_watch)
        .service(get_watches);
}

#[put("/watches/volumes/{volume_id}")]
async fn put_watch(
    web::Path(volume_id): web::Path<VolumeId>,
    web::Query(watch): web::Query<WatchTypeQueryParam>,
) -> Result<Json<()>, RestError> {
    CreateWatch {
        id: WatchResourceId::Volume(volume_id),
        callback: WatchCallback::Uri(watch.callback.to_string()),
        watch_type: WatchType::Actual,
    }
    .request()
    .await?;

    Ok(Json(()))
}

#[get("/watches/volumes/{volume_id}")]
async fn get_watches(
    web::Path(volume_id): web::Path<VolumeId>,
) -> Result<Json<Vec<RestWatch>>, RestError> {
    let watches = GetWatchers {
        resource: WatchResourceId::Volume(volume_id),
    }
    .request()
    .await?;
    let watches = watches.0.iter();
    let watches = watches
        .filter_map(|w| RestWatch::try_from(w).ok())
        .collect();
    Ok(Json(watches))
}

#[delete("/watches/volumes/{volume_id}")]
async fn del_watch(
    web::Path(volume_id): web::Path<VolumeId>,
    web::Query(watch): web::Query<WatchTypeQueryParam>,
) -> Result<JsonUnit, RestError> {
    DeleteWatch {
        id: WatchResourceId::Volume(volume_id),
        callback: WatchCallback::Uri(watch.callback.to_string()),
        watch_type: WatchType::Actual,
    }
    .request()
    .await?;

    Ok(JsonUnit::default())
}
