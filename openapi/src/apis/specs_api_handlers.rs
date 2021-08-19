#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]

use crate::apis::{Body, NoContent};
use actix_web::{
    web::{Json, Path, Query, ServiceConfig},
    FromRequest, HttpRequest,
};

/// Configure handlers for the Specs resource
pub fn configure<T: crate::apis::Specs + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/specs")
            .name("get_specs")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_specs::<T, A>)),
    );
}

async fn get_specs<T: crate::apis::Specs + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<crate::models::Specs>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_specs().await.map(Json)
}
