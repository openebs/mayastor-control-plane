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

/// Configure handlers for the JsonGrpcApi resource
pub fn configure<T: crate::apis::JsonGrpcApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nodes/{node}/jsongrpc/{method}")
            .name("put_node_jsongrpc")
            .guard(actix_web::guard::Put())
            .route(actix_web::web::put().to(put_node_jsongrpc::<T, A>)),
    );
}

async fn put_node_jsongrpc<T: crate::apis::JsonGrpcApi + 'static, A: FromRequest + 'static>(
    _token: A,
    Path((node, method)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::put_node_jsongrpc(Path((node, method)), Json(body)).await
}
