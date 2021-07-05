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

/// Configure handlers for the VolumesApi resource
pub fn configure<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/volumes{volume_id}/share")
            .name("del_share")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_share::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes/{volume_id}")
            .name("del_volume")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_volume::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/volumes/{volume_id}")
            .name("get_node_volume")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_volume::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/volumes")
            .name("get_node_volumes")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_volumes::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes/{volume_id}")
            .name("get_volume")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_volume::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes")
            .name("get_volumes")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_volumes::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes/{volume_id}")
            .name("put_volume")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_volume::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes/{volume_id}/share/{protocol}")
            .name("put_volume_share")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_volume_share::<T, A>)),
    );
}

async fn del_share<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_share(Path(volume_id)).await.map(|_| Json(()))
}

async fn del_volume<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_volume(Path(volume_id)).await.map(|_| Json(()))
}

async fn get_node_volume<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, volume_id)): Path<(String, String)>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_volume(Path((node_id, volume_id))).await
}

async fn get_node_volumes<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(node_id): Path<String>,
) -> Result<Json<Vec<crate::models::Volume>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_node_volumes(Path(node_id)).await
}

async fn get_volume<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_volume(Path(volume_id)).await
}

async fn get_volumes<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Volume>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_volumes().await
}

async fn put_volume<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(volume_id): Path<String>,
    Json(create_volume_body): Json<crate::models::CreateVolumeBody>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_volume(Path(volume_id), Json(create_volume_body)).await
}

async fn put_volume_share<T: crate::apis::VolumesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((volume_id, protocol)): Path<(String, crate::models::VolumeShareProtocol)>,
) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_volume_share(Path((volume_id, protocol))).await
}
