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
pub trait NexusesApi {
    async fn del_nexus(
        Path(nexus_id): Path<String>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_node_nexus(
        Path((node_id, nexus_id)): Path<(String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_node_nexus_share(
        Path((node_id, nexus_id)): Path<(String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_nexus(
        Path(nexus_id): Path<String>,
    ) -> Result<Json<crate::models::Nexus>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_nexuses(
    ) -> Result<Json<Vec<crate::models::Nexus>>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_nexus(
        Path((node_id, nexus_id)): Path<(String, String)>,
    ) -> Result<Json<crate::models::Nexus>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_nexuses(
        Path(id): Path<String>,
    ) -> Result<Json<Vec<crate::models::Nexus>>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_node_nexus(
        Path((node_id, nexus_id)): Path<(String, String)>,
        Json(create_nexus_body): Json<crate::models::CreateNexusBody>,
    ) -> Result<Json<crate::models::Nexus>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_node_nexus_share(
        Path((node_id, nexus_id, protocol)): Path<(
            String,
            String,
            crate::models::NexusShareProtocol,
        )>,
    ) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>>;
}
