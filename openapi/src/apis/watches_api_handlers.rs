#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use actix_web::{
    web::{self, Json, Path, Query, ServiceConfig},
    FromRequest, HttpRequest,
};

/// Configure handlers for the WatchesApi resource
pub fn configure<T: crate::apis::WatchesApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/watches/volumes/{volume_id}")
            .name("del_watch_volume")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_watch_volume::<T, A>)),
    )
    .service(
        actix_web::web::resource("/watches/volumes/{volume_id}")
            .name("get_watch_volume")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_watch_volume::<T, A>)),
    )
    .service(
        actix_web::web::resource("/watches/volumes/{volume_id}")
            .name("put_watch_volume")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_watch_volume::<T, A>)),
    );
}

#[derive(serde::Deserialize)]
struct del_watch_volumeQueryParams {
    /// URL callback
    #[serde(rename = "callback")]
    pub callback: url::Url,
}
#[derive(serde::Deserialize)]
struct put_watch_volumeQueryParams {
    /// URL callback
    #[serde(rename = "callback")]
    pub callback: url::Url,
}

async fn del_watch_volume<T: crate::apis::WatchesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
    Query(query): Query<del_watch_volumeQueryParams>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_watch_volume(Path(volume_id), query.callback)
        .await
        .map(|_| Json(()))
}

async fn get_watch_volume<T: crate::apis::WatchesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
) -> Result<Json<Vec<crate::models::RestWatch>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_watch_volume(Path(volume_id)).await
}

async fn put_watch_volume<T: crate::apis::WatchesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
    Query(query): Query<put_watch_volumeQueryParams>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_watch_volume(Path(volume_id), query.callback)
        .await
        .map(|_| Json(()))
}
