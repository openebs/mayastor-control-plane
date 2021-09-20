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
pub trait Children {
    async fn del_nexus_child(
        query: &str,
        Path((nexus_id, child_id)): Path<(uuid::Uuid, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_node_nexus_child(
        query: &str,
        Path((node_id, nexus_id, child_id)): Path<(String, uuid::Uuid, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_nexus_child(
        query: &str,
        Path((nexus_id, child_id)): Path<(uuid::Uuid, String)>,
    ) -> Result<crate::models::Child, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_nexus_children(
        Path(nexus_id): Path<uuid::Uuid>,
    ) -> Result<Vec<crate::models::Child>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_nexus_child(
        query: &str,
        Path((node_id, nexus_id, child_id)): Path<(String, uuid::Uuid, String)>,
    ) -> Result<crate::models::Child, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_nexus_children(
        Path((node_id, nexus_id)): Path<(String, uuid::Uuid)>,
    ) -> Result<Vec<crate::models::Child>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_nexus_child(
        query: &str,
        Path((nexus_id, child_id)): Path<(uuid::Uuid, String)>,
    ) -> Result<crate::models::Child, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_node_nexus_child(
        query: &str,
        Path((node_id, nexus_id, child_id)): Path<(String, uuid::Uuid, String)>,
    ) -> Result<crate::models::Child, crate::apis::RestError<crate::models::RestJsonError>>;
}
