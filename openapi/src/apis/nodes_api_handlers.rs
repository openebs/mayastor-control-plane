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

/// Configure handlers for the NodesApi resource
pub fn configure<T: crate::apis::NodesApi + 'static, A: FromRequest + 'static>(
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

async fn get_node<T: crate::apis::NodesApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path(id): Path<String>,
) -> Result<Json<crate::models::Node>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_node(Path(id)).await
}

async fn get_nodes<T: crate::apis::NodesApi + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<Vec<crate::models::Node>>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_nodes().await
}
