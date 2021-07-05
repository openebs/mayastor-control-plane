use super::*;
use actix_web::web::Path;
use common_lib::types::v0::message_bus::{DestroyPool, Filter};
use mbus_api::{
    message_bus::v0::{BusError, MessageBus, MessageBusTrait},
    ReplyErrorKind, ResourceKind,
};

async fn destroy_pool(filter: Filter) -> Result<(), RestError<RestJsonError>> {
    let destroy = match filter.clone() {
        Filter::NodePool(node_id, pool_id) => DestroyPool {
            node: node_id,
            id: pool_id,
        },
        Filter::Pool(pool_id) => {
            let node_id = match MessageBus::get_pool(filter).await {
                Ok(pool) => pool.node,
                Err(error) => return Err(RestError::from(error)),
            };
            DestroyPool {
                node: node_id,
                id: pool_id,
            }
        }
        _ => {
            return Err(RestError::from(BusError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Pool,
                source: "destroy_pool".to_string(),
                extra: "invalid filter for resource".to_string(),
            }))
        }
    };

    MessageBus::destroy_pool(destroy).await?;
    Ok(())
}

#[async_trait::async_trait]
impl apis::PoolsApi for RestApi {
    async fn del_node_pool(
        Path((node_id, pool_id)): Path<(String, String)>,
    ) -> Result<(), RestError<RestJsonError>> {
        destroy_pool(Filter::NodePool(node_id.into(), pool_id.into())).await
    }

    async fn del_pool(Path(pool_id): Path<String>) -> Result<(), RestError<RestJsonError>> {
        destroy_pool(Filter::Pool(pool_id.into())).await
    }

    async fn get_node_pool(
        Path((node_id, pool_id)): Path<(String, String)>,
    ) -> Result<Json<models::Pool>, RestError<RestJsonError>> {
        let pool = MessageBus::get_pool(Filter::NodePool(node_id.into(), pool_id.into())).await?;
        Ok(Json(pool.into()))
    }

    async fn get_node_pools(
        Path(id): Path<String>,
    ) -> Result<Json<Vec<models::Pool>>, RestError<RestJsonError>> {
        let pools = MessageBus::get_pools(Filter::Node(id.into())).await?;
        Ok(Json(pools.into_iter().map(From::from).collect()))
    }

    async fn get_pool(
        Path(pool_id): Path<String>,
    ) -> Result<Json<models::Pool>, RestError<RestJsonError>> {
        let pool = MessageBus::get_pool(Filter::Pool(pool_id.into())).await?;
        Ok(Json(pool.into()))
    }

    async fn get_pools() -> Result<Json<Vec<models::Pool>>, RestError<RestJsonError>> {
        let pools = MessageBus::get_pools(Filter::None).await?;
        Ok(Json(pools.into_iter().map(From::from).collect()))
    }

    async fn put_node_pool(
        Path((node_id, pool_id)): Path<(String, String)>,
        Json(create_pool_body): Json<models::CreatePoolBody>,
    ) -> Result<Json<models::Pool>, RestError<RestJsonError>> {
        let create =
            CreatePoolBody::from(create_pool_body).bus_request(node_id.into(), pool_id.into());
        let pool = MessageBus::create_pool(create).await?;
        Ok(Json(pool.into()))
    }
}
