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
pub trait NodesApi {
    async fn get_node(
        Path(id): Path<String>,
    ) -> Result<Json<crate::models::Node>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_nodes(
    ) -> Result<Json<Vec<crate::models::Node>>, crate::apis::RestError<crate::models::RestJsonError>>;
}
