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

/// Configure handlers for the ReplicasApi resource
pub fn configure<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}")
            .name("del_node_pool_replica")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_node_pool_replica::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share")
            .name("del_node_pool_replica_share")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_node_pool_replica_share::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools/{pool_id}/replicas/{replica_id}")
            .name("del_pool_replica")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_pool_replica::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools/{pool_id}/replicas/{replica_id}/share")
            .name("del_pool_replica_share")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_pool_replica_share::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}")
            .name("get_node_pool_replica")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_pool_replica::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}/replicas")
            .name("get_node_pool_replicas")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_pool_replicas::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{id}/replicas")
            .name("get_node_replicas")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_replicas::<T, A>)),
    )
    .service(
        actix_web::web::resource("/replicas/{id}")
            .name("get_replica")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_replica::<T, A>)),
    )
    .service(
        actix_web::web::resource("/replicas")
            .name("get_replicas")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_replicas::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}")
            .name("put_node_pool_replica")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_node_pool_replica::<T, A>)),
    )
    .service(
        actix_web::web::resource(
            "/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share/{protocol}",
        )
        .name("put_node_pool_replica_share")
        .guard(actix_web::guard::Put())
        .route(actix_web::web::put().to(put_node_pool_replica_share::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools/{pool_id}/replicas/{replica_id}")
            .name("put_pool_replica")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_pool_replica::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools/{pool_id}/replicas/{replica_id}/share/{protocol}")
            .name("put_pool_replica_share")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_pool_replica_share::<T, A>)),
    );
}

async fn del_node_pool_replica<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_node_pool_replica(Path((node_id, pool_id, replica_id)))
        .await
        .map(|_| Json(()))
}

async fn del_node_pool_replica_share<
    T: crate::apis::ReplicasApi + 'static,
    A: FromRequest + 'static,
>(
    _token: A,
    Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_node_pool_replica_share(Path((node_id, pool_id, replica_id)))
        .await
        .map(|_| Json(()))
}

async fn del_pool_replica<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((pool_id, replica_id)): Path<(String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_pool_replica(Path((pool_id, replica_id)))
        .await
        .map(|_| Json(()))
}

async fn del_pool_replica_share<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((pool_id, replica_id)): Path<(String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_pool_replica_share(Path((pool_id, replica_id)))
        .await
        .map(|_| Json(()))
}

async fn get_node_pool_replica<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_pool_replica(Path((node_id, pool_id, replica_id))).await
}

async fn get_node_pool_replicas<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id)): Path<(String, String)>,
) -> Result<Json<Vec<crate::models::Replica>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_node_pool_replicas(Path((node_id, pool_id))).await
}

async fn get_node_replicas<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::models::Replica>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_node_replicas(Path(id)).await
}

async fn get_replica<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(id): Path<String>,
) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_replica(Path(id)).await
}

async fn get_replicas<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Replica>>, crate::apis::RestError<crate::models::RestJsonError>>
{
    T::get_replicas().await
}

async fn put_node_pool_replica<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    Json(create_replica_body): Json<crate::models::CreateReplicaBody>,
) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_pool_replica(
        Path((node_id, pool_id, replica_id)),
        Json(create_replica_body),
    )
    .await
}

async fn put_node_pool_replica_share<
    T: crate::apis::ReplicasApi + 'static,
    A: FromRequest + 'static,
>(
    _token: A,
    Path((node_id, pool_id, replica_id, protocol)): Path<(
        String,
        String,
        String,
        crate::models::ReplicaShareProtocol,
    )>,
) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_pool_replica_share(Path((node_id, pool_id, replica_id, protocol))).await
}

async fn put_pool_replica<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((pool_id, replica_id)): Path<(String, String)>,
    Json(create_replica_body): Json<crate::models::CreateReplicaBody>,
) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_pool_replica(Path((pool_id, replica_id)), Json(create_replica_body)).await
}

async fn put_pool_replica_share<T: crate::apis::ReplicasApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((pool_id, replica_id, protocol)): Path<(
        String,
        String,
        crate::models::ReplicaShareProtocol,
    )>,
) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_pool_replica_share(Path((pool_id, replica_id, protocol))).await
}
