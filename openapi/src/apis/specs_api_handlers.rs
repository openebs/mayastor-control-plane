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

/// Configure handlers for the SpecsApi resource
pub fn configure<T: crate::apis::SpecsApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/specs")
            .name("get_specs")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_specs::<T, A>)),
    );
}

async fn get_specs<T: crate::apis::SpecsApi + 'static, A: FromRequest + 'static>(
    _token: A,
) -> Result<Json<crate::models::Specs>, crate::apis::RestError<crate::models::RestJsonError>> {
    T::get_specs().await
}
