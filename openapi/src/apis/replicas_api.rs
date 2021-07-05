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
pub trait ReplicasApi {
    async fn del_node_pool_replica(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_node_pool_replica_share(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_pool_replica(
        Path((pool_id, replica_id)): Path<(String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_pool_replica_share(
        Path((pool_id, replica_id)): Path<(String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_pool_replica(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    ) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_pool_replicas(
        Path((node_id, pool_id)): Path<(String, String)>,
    ) -> Result<
        Json<Vec<crate::models::Replica>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
    async fn get_node_replicas(
        Path(id): Path<String>,
    ) -> Result<
        Json<Vec<crate::models::Replica>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
    async fn get_replica(
        Path(id): Path<String>,
    ) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_replicas() -> Result<
        Json<Vec<crate::models::Replica>>,
        crate::apis::RestError<crate::models::RestJsonError>,
    >;
    async fn put_node_pool_replica(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
        Json(create_replica_body): Json<crate::models::CreateReplicaBody>,
    ) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_node_pool_replica_share(
        Path((node_id, pool_id, replica_id, protocol)): Path<(
            String,
            String,
            String,
            crate::models::ReplicaShareProtocol,
        )>,
    ) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_pool_replica(
        Path((pool_id, replica_id)): Path<(String, String)>,
        Json(create_replica_body): Json<crate::models::CreateReplicaBody>,
    ) -> Result<Json<crate::models::Replica>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_pool_replica_share(
        Path((pool_id, replica_id, protocol)): Path<(
            String,
            String,
            crate::models::ReplicaShareProtocol,
        )>,
    ) -> Result<Json<String>, crate::apis::RestError<crate::models::RestJsonError>>;
}
