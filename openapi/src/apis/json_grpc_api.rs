#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use actix_web::web::{self, Json, Path, Query};

#[async_trait::async_trait]
pub trait JsonGrpcApi {
    async fn put_node_jsongrpc(
        Path((node, method)): Path<(String, String)>,
        Json(body): Json<serde_json::Value>,
    ) -> Result<Json<serde_json::Value>, crate::apis::RestError<crate::models::RestJsonError>>;
}
