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

/// Configure handlers for the BlockDevicesApi resource
pub fn configure<T: crate::apis::BlockDevicesApi + 'static, A: FromRequest + 'static>(
    cfg: &mut ServiceConfig,
) {
    cfg.service(
        actix_web::web::resource("/nodes/{node}/block_devices")
            .name("get_node_block_devices")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_node_block_devices::<T, A>)),
    );
}

#[derive(serde::Deserialize)]
struct get_node_block_devicesQueryParams {
    /// specifies whether to list all devices or only usable ones
    #[serde(rename = "all", skip_serializing_if = "Option::is_none")]
    pub all: Option<bool>,
}

async fn get_node_block_devices<
    T: crate::apis::BlockDevicesApi + 'static,
    A: FromRequest + 'static,
>(
    _token: A,
    Path(node): Path<String>,
    Query(query): Query<get_node_block_devicesQueryParams>,
) -> Result<
    Json<Vec<crate::models::BlockDevice>>,
    crate::apis::RestError<crate::models::RestJsonError>,
> {
    T::get_node_block_devices(Path(node), query.all).await
}
