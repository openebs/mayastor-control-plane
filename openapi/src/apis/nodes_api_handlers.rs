#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use crate::apis::Body;
use actix_web::{
    web::{Json, Path, Query, ServiceConfig},
    FromRequest, HttpRequest,
};

/// Configure handlers for the Nodes resource
pub fn configure<T: crate::apis::Nodes + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nodes/{id}")
            .name("get_node")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node::<T, A>)),
    )
    .service(
        actix_web::web::resource("/nodes")
            .name("get_nodes")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_nodes::<T, A>)),
    );
}

async fn get_node<T: crate::apis::Nodes + 'static, A: FromRequest + 'static>(
    _token: A,
    path: Path<String>,
) -> Result<Json<crate::models::Node>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node(crate::apis::Path(path.into_inner()))
        .await
        .map(Json)
}

async fn get_nodes<T: crate::apis::Nodes + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Node>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_nodes().await.map(Json)
}
