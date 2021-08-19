use super::*;
use common_lib::types::v0::message_bus::{
    DestroyReplica, Filter, ReplicaShareProtocol, ShareReplica, UnshareReplica,
};
use mbus_api::{
    message_bus::v0::{BusError, MessageBus, MessageBusTrait},
    ReplyErrorKind, ResourceKind,
};

async fn put_replica(
    filter: Filter,
    body: CreateReplicaBody,
) -> Result<models::Replica, RestError<RestJsonError>> {
    let create = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
            body.bus_request(node_id, pool_id, replica_id)
        }
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_pool(Filter::Pool(pool_id.clone())).await {
                Ok(pool) => pool.node(),
                Err(error) => return Err(RestError::from(error)),
            };
            body.bus_request(node_id, pool_id, replica_id)
        }
        _ => {
            return Err(RestError::from(BusError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Replica,
                source: "put_replica".to_string(),
                extra: "invalid filter for resource".to_string(),
            }))
        }
    };

    let replica = MessageBus::create_replica(create).await?;
    Ok(replica.into())
}

async fn destroy_replica(filter: Filter) -> Result<(), RestError<RestJsonError>> {
    let destroy = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => DestroyReplica {
            node: node_id,
            pool: pool_id,
            uuid: replica_id,
            ..Default::default()
        },
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return Err(RestError::from(error)),
            };

            DestroyReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
                ..Default::default()
            }
        }
        _ => {
            return Err(RestError::from(BusError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Replica,
                source: "destroy_replica".to_string(),
                extra: "invalid filter for resource".to_string(),
            }))
        }
    };

    MessageBus::destroy_replica(destroy).await?;
    Ok(())
}

async fn share_replica(
    filter: Filter,
    protocol: ReplicaShareProtocol,
) -> Result<String, RestError<RestJsonError>> {
    let share = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => ShareReplica {
            node: node_id,
            pool: pool_id,
            uuid: replica_id,
            protocol,
        },
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return Err(RestError::from(error)),
            };

            ShareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
                protocol,
            }
        }
        _ => {
            return Err(RestError::from(BusError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Replica,
                source: "share_replica".to_string(),
                extra: "invalid filter for resource".to_string(),
            }))
        }
    };

    let share_uri = MessageBus::share_replica(share).await?;
    Ok(share_uri)
}

async fn unshare_replica(filter: Filter) -> Result<(), RestError<RestJsonError>> {
    let unshare = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => UnshareReplica {
            node: node_id,
            pool: pool_id,
            uuid: replica_id,
        },
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return Err(RestError::from(error)),
            };

            UnshareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        _ => {
            return Err(RestError::from(BusError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Replica,
                source: "unshare_replica".to_string(),
                extra: "invalid filter for resource".to_string(),
            }))
        }
    };

    MessageBus::unshare_replica(unshare).await?;
    Ok(())
}

#[async_trait::async_trait]
impl apis::Replicas for RestApi {
    async fn del_node_pool_replica(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    ) -> Result<(), RestError<RestJsonError>> {
        destroy_replica(Filter::NodePoolReplica(
            node_id.into(),
            pool_id.into(),
            replica_id.into(),
        ))
        .await
    }

    async fn del_node_pool_replica_share(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    ) -> Result<(), RestError<RestJsonError>> {
        unshare_replica(Filter::NodePoolReplica(
            node_id.into(),
            pool_id.into(),
            replica_id.into(),
        ))
        .await
    }

    async fn del_pool_replica(
        Path((pool_id, replica_id)): Path<(String, String)>,
    ) -> Result<(), RestError<RestJsonError>> {
        destroy_replica(Filter::PoolReplica(pool_id.into(), replica_id.into())).await
    }

    async fn del_pool_replica_share(
        Path((pool_id, replica_id)): Path<(String, String)>,
    ) -> Result<(), RestError<RestJsonError>> {
        unshare_replica(Filter::PoolReplica(pool_id.into(), replica_id.into())).await
    }

    async fn get_node_pool_replica(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
    ) -> Result<models::Replica, RestError<RestJsonError>> {
        let replica = MessageBus::get_replica(Filter::NodePoolReplica(
            node_id.into(),
            pool_id.into(),
            replica_id.into(),
        ))
        .await?;
        Ok(replica.into())
    }

    async fn get_node_pool_replicas(
        Path((node_id, pool_id)): Path<(String, String)>,
    ) -> Result<Vec<models::Replica>, RestError<RestJsonError>> {
        let replicas =
            MessageBus::get_replicas(Filter::NodePool(node_id.into(), pool_id.into())).await?;
        Ok(replicas.into_iter().map(From::from).collect())
    }

    async fn get_node_replicas(
        Path(id): Path<String>,
    ) -> Result<Vec<models::Replica>, RestError<RestJsonError>> {
        let replicas = MessageBus::get_replicas(Filter::Node(id.into())).await?;
        Ok(replicas.into_iter().map(From::from).collect())
    }

    async fn get_replica(
        Path(id): Path<String>,
    ) -> Result<models::Replica, RestError<RestJsonError>> {
        let replica = MessageBus::get_replica(Filter::Replica(id.into())).await?;
        Ok(replica.into())
    }

    async fn get_replicas() -> Result<Vec<models::Replica>, RestError<RestJsonError>> {
        let replicas = MessageBus::get_replicas(Filter::None).await?;
        Ok(replicas.into_iter().map(From::from).collect())
    }

    async fn put_node_pool_replica(
        Path((node_id, pool_id, replica_id)): Path<(String, String, String)>,
        Body(create_replica_body): Body<models::CreateReplicaBody>,
    ) -> Result<models::Replica, RestError<RestJsonError>> {
        put_replica(
            Filter::NodePoolReplica(node_id.into(), pool_id.into(), replica_id.into()),
            CreateReplicaBody::from(create_replica_body),
        )
        .await
    }

    async fn put_node_pool_replica_share(
        Path((node_id, pool_id, replica_id, protocol)): Path<(
            String,
            String,
            String,
            models::ReplicaShareProtocol,
        )>,
    ) -> Result<String, RestError<RestJsonError>> {
        share_replica(
            Filter::NodePoolReplica(node_id.into(), pool_id.into(), replica_id.into()),
            protocol.into(),
        )
        .await
    }

    async fn put_pool_replica(
        Path((pool_id, replica_id)): Path<(String, String)>,
        Body(create_replica_body): Body<models::CreateReplicaBody>,
    ) -> Result<models::Replica, RestError<RestJsonError>> {
        put_replica(
            Filter::PoolReplica(pool_id.into(), replica_id.into()),
            CreateReplicaBody::from(create_replica_body),
        )
        .await
    }

    async fn put_pool_replica_share(
        Path((pool_id, replica_id, protocol)): Path<(String, String, models::ReplicaShareProtocol)>,
    ) -> Result<String, RestError<RestJsonError>> {
        share_replica(
            Filter::PoolReplica(pool_id.into(), replica_id.into()),
            protocol.into(),
        )
        .await
    }
}
