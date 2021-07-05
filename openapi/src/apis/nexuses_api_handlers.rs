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

/// Configure handlers for the NexusesApi resource
pub fn configure<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nexuses/{nexus_id}")
            .name("del_nexus")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_nexus::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}")
            .name("del_node_nexus")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_node_nexus::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}/share")
            .name("del_node_nexus_share")
            .guard(actix_web::guard::Delete())
            .route(actix_web::web::delete().to(del_node_nexus_share::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nexuses/{nexus_id}")
            .name("get_nexus")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_nexus::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nexuses")
            .name("get_nexuses")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_nexuses::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}")
            .name("get_node_nexus")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_nexus::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{id}/nexuses")
            .name("get_node_nexuses")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_nexuses::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}")
            .name("put_node_nexus")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_node_nexus::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes/{node_id}/nexuses/{nexus_id}/share/{protocol}")
            .name("put_node_nexus_share")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_node_nexus_share::<T, A>)),
    );
}

async fn del_nexus<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(nexus_id): Path<String>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_nexus(Path(nexus_id)).await.map(|_| Json(()))
}

async fn del_node_nexus<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, nexus_id)): Path<(String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_node_nexus(Path((node_id, nexus_id)))
        .await
        .map(|_| Json(()))
}

async fn del_node_nexus_share<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, nexus_id)): Path<(String, String)>,
) -> Result<Json<()>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::del_node_nexus_share(Path((node_id, nexus_id)))
        .await
        .map(|_| Json(()))
}

async fn get_nexus<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(nexus_id): Path<String>,
) -> Result<Json<crate::models::Nexus>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_nexus(Path(nexus_id)).await
}

async fn get_nexuses<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Nexus>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_nexuses().await
}

async fn get_node_nexus<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, nexus_id)): Path<(String, String)>,
) -> Result<Json<crate::models::Nexus>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_nexus(Path((node_id, nexus_id))).await
}

async fn get_node_nexuses<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::models::Nexus>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node_nexuses(Path(id)).await
}

async fn put_node_nexus<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, nexus_id)): Path<(String, String)>,
    Json(create_nexus_body): Json<crate::models::CreateNexusBody>,
) -> Result<Json<crate::models::Nexus>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_nexus(Path((node_id, nexus_id)), Json(create_nexus_body)).await
}

async fn put_node_nexus_share<T: crate::apis::NexusesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node_id, nexus_id, protocol)): Path<(String, String, crate::models::NexusShareProtocol)>,
) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_nexus_share(Path((node_id, nexus_id, protocol))).await
}
