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
pub trait VolumesApi {
    async fn del_share(
        Path(volume_id): Path<String>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_volume(
        Path(volume_id): Path<String>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_volume(
        Path((node_id, volume_id)): Path<(String, String)>,
    ) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_volumes(
        Path(node_id): Path<String>,
    ) -> Result<
        Json<Vec<crate::models::Volume>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
    async fn get_volume(
        Path(volume_id): Path<String>,
    ) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_volumes() -> Result<
        Json<Vec<crate::models::Volume>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
    async fn put_volume(
        Path(volume_id): Path<String>,
        Json(create_volume_body): Json<crate::models::CreateVolumeBody>,
    ) -> Result<Json<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_volume_share(
        Path((volume_id, protocol)): Path<(String, crate::models::VolumeShareProtocol)>,
    ) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>>;
}
