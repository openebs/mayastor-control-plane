#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use crate::apis::{Body, NoContent};
use actix_web::{
    web::{Json, Path, Query, ServiceConfig},
    FromRequest, HttpRequest,
};

/// Configure handlers for the Volumes resource
pub fn configure<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
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
        actix_web::web::resource("/volumes/{volume_id}/target")
            .name("del_volume_target")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_volume_target::<T, A>)),
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
        actix_web::web::resource("/volumes/{volume_id}/replica_count/{replica_count}")
            .name("put_volume_replica_count")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_volume_replica_count::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes/{volume_id}/share/{protocol}")
            .name("put_volume_share")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_volume_share::<T, A>)),
    )
    .service(
        actix_web::web::resource("/volumes/{volume_id}/target")
            .name("put_volume_target")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_volume_target::<T, A>)),
    );
}

#[derive(serde::Deserialize)]
struct put_volume_targetQueryParams {
    /// The node where the front-end workload resides. If the workload moves then the volume must
    /// be republished.
    #[serde(rename = "node")]
    pub node: String,
    /// The protocol used to connect to the front-end node.
    #[serde(rename = "protocol")]
    pub protocol: crate::models::VolumeShareProtocol,
}

async fn del_share<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
) -> Result<NoContent, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_share(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
        .map(Into::into)
}

async fn del_volume<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
) -> Result<NoContent, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_volume(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
        .map(Into::into)
}

async fn del_volume_target<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_volume_target(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
}

async fn get_node_volumes<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
) -> Result<Json<Vec<crate::models::Volume>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_node_volumes(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
}

async fn get_volume<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_volume(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
}

async fn get_volumes<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Volume>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_volumes().await.map(Json)
}

async fn put_volume<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
    Json(create_volume_body): Json<crate::models::CreateVolumeBody>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_volume(
        crate::apis::Path(path.into_inner()),
        Body(create_volume_body),
    )
    .await
    .map(Json)
}

async fn put_volume_replica_count<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<(String, u8)>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_volume_replica_count(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
}

async fn put_volume_share<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<(String, crate::models::VolumeShareProtocol)>,
) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_volume_share(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
}

/// Create a volume target connectable for front-end IO from the specified node. Due to a
/// limitation, this must currently be a mayastor storage node.
async fn put_volume_target<T: crate::apis::Volumes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
    query: Query<put_volume_targetQueryParams>,
) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>> {
    let query = query.into_inner();
    T::put_volume_target(
        crate::apis::Path(path.into_inner()),
        crate::apis::Query((query.node, query.protocol)),
    )
    .await
    .map(Json)
}
