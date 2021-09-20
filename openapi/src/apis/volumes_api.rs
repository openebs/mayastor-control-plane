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
pub trait Volumes {
    async fn del_share(
        Path(volume_id): Path<uuid::Uuid>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_volume(
        Path(volume_id): Path<uuid::Uuid>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_volume_target(
        Path(volume_id): Path<uuid::Uuid>,
        Query(force): Query<Option<bool>>,
    ) -> Result<crate::models::Volume, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_volumes(
        Path(node_id): Path<String>,
    ) -> Result<Vec<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_volume(
        Path(volume_id): Path<uuid::Uuid>,
    ) -> Result<crate::models::Volume, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_volumes(
    ) -> Result<Vec<crate::models::Volume>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_volume(
        Path(volume_id): Path<uuid::Uuid>,
        Body(create_volume_body): Body<crate::models::CreateVolumeBody>,
    ) -> Result<crate::models::Volume, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_volume_replica_count(
        Path((volume_id, replica_count)): Path<(uuid::Uuid, u8)>,
    ) -> Result<crate::models::Volume, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_volume_share(
        Path((volume_id, protocol)): Path<(uuid::Uuid, crate::models::VolumeShareProtocol)>,
    ) -> Result<String, crate::apis::RestError<crate::models::RestJsonError>>;
    /// Create a volume target connectable for front-end IO from the specified node. Due to a
    /// limitation, this must currently be a mayastor storage node.
    async fn put_volume_target(
        Path(volume_id): Path<uuid::Uuid>,
        Query((node, protocol)): Query<(String, crate::models::VolumeShareProtocol)>,
    ) -> Result<crate::models::Volume, crate::apis::RestError<crate::models::RestJsonError>>;
}
