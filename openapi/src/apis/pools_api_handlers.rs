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

/// Configure handlers for the PoolsApi resource
pub fn configure<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}")
            .name("del_node_pool")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_node_pool::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools/{pool_id}")
            .name("del_pool")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_pool::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}")
            .name("get_node_pool")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_pool::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{id}/pools")
            .name("get_node_pools")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_pools::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools/{pool_id}")
            .name("get_pool")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_pool::<T, A>)),
    )
    .service(
        actix_web::web::resource("/pools")
            .name("get_pools")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_pools::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/pools/{pool_id}")
            .name("put_node_pool")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_node_pool::<T, A>)),
    );
}

async fn del_node_pool<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id)): Path<(String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_node_pool(Path((node_id, pool_id)))
        .await
        .map(|_| Json(()))
}

async fn del_pool<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(pool_id): Path<String>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_pool(Path(pool_id)).await.map(|_| Json(()))
}

async fn get_node_pool<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id)): Path<(String, String)>,
) -> Result<Json<crate::models::Pool>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_pool(Path((node_id, pool_id))).await
}

async fn get_node_pools<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::models::Pool>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_pools(Path(id)).await
}

async fn get_pool<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(pool_id): Path<String>,
) -> Result<Json<crate::models::Pool>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_pool(Path(pool_id)).await
}

async fn get_pools<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Pool>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_pools().await
}

async fn put_node_pool<T: crate::apis::PoolsApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, pool_id)): Path<(String, String)>,
    Json(create_pool_body): Json<crate::models::CreatePoolBody>,
) -> Result<Json<crate::models::Pool>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_pool(Path((node_id, pool_id)), Json(create_pool_body)).await
}
