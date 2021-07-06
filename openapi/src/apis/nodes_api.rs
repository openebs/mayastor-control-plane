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
pub trait Nodes {
    async fn get_node(
        Path(id): Path<String>,
    ) -> Result<crate::models::Node, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_nodes(
    ) -> Result<Vec<crate::models::Node>, crate::apis::RestError<crate::models::RestJsonError>>;
}
