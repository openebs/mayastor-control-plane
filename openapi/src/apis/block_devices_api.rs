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
pub trait BlockDevicesApi {
    async fn get_node_block_devices(
        Path(node): Path<String>,
        all: Option<bool>,
    ) -> Result<
        Json<Vec<crate::models::BlockDevice>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
}
