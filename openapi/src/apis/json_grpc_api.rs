#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use crate::apis::{Body, Path, Query};
use actix_web::web::Json;

#[async_trait::async_trait]
pub trait JsonGrpc {
    async fn put_node_jsongrpc(
        Path((node, method)): Path<(String, String)>,
        Body(body): Body<serde_json::Value>,
    ) -> Result<serde_json::Value, crate::apis::RestError<crate::models::RestJsonError>>;
}
