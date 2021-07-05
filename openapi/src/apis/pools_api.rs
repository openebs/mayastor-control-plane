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
pub trait PoolsApi {
    async fn del_node_pool(
        Path((node_id, pool_id)): Path<(String, String)>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn del_pool(
        Path(pool_id): Path<String>,
    ) -> Result<(), crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_pool(
        Path((node_id, pool_id)): Path<(String, String)>,
    ) -> Result<Json<crate::models::Pool>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_node_pools(
        Path(id): Path<String>,
    ) -> Result<Json<Vec<crate::models::Pool>>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_pool(
        Path(pool_id): Path<String>,
    ) -> Result<Json<crate::models::Pool>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn get_pools(
    ) -> Result<Json<Vec<crate::models::Pool>>, crate::apis::RestError<crate::models::RestJsonError>>;
    async fn put_node_pool(
        Path((node_id, pool_id)): Path<(String, String)>,
        Json(create_pool_body): Json<crate::models::CreatePoolBody>,
    ) -> Result<Json<crate::models::Pool>, crate::apis::RestError<crate::models::RestJsonError>>;
}
