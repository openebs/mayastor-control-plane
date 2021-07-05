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
pub trait SpecsApi {
    async fn get_specs(
    ) -> Result<Json<crate::models::Specs>, crate::apis::RestError<crate::models::RestJsonError>>;
}
