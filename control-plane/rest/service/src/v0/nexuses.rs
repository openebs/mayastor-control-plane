use super::*;
use mbus_api::{
    message_bus::v0::{BusError, MessageBus, MessageBusTrait},
    ReplyErrorKind, ResourceKind,
};
use types::v0::message_bus::mbus::{
    DestroyNexus, Filter, Nexus, NexusId, NexusShareProtocol, NodeId, ShareNexus, UnshareNexus,
};

pub(super) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_nexuses)
        .service(get_nexus)
        .service(get_node_nexuses)
        .service(get_node_nexus)
        .service(put_node_nexus)
        .service(del_node_nexus)
        .service(del_nexus)
        .service(put_node_nexus_share)
        .service(del_node_nexus_share);
}

#[get("/nexuses", tags(Nexuses))]
async fn get_nexuses() -> Result<Json<Vec<Nexus>>, RestClusterError> {
    RestRespond::result(MessageBus::get_nexuses(Filter::None).await).map_err(RestClusterError::from)
}
#[get("/nexuses/{nexus_id}", tags(Nexuses))]
async fn get_nexus(web::Path(nexus_id): web::Path<NexusId>) -> Result<Json<Nexus>, RestError> {
    RestRespond::result(MessageBus::get_nexus(Filter::Nexus(nexus_id)).await)
}

#[get("/nodes/{id}/nexuses", tags(Nexuses))]
async fn get_node_nexuses(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<Json<Vec<Nexus>>, RestError> {
    RestRespond::result(MessageBus::get_nexuses(Filter::Node(node_id)).await)
}
#[get("/nodes/{node_id}/nexuses/{nexus_id}", tags(Nexuses))]
async fn get_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> Result<Json<Nexus>, RestError> {
    RestRespond::result(MessageBus::get_nexus(Filter::NodeNexus(node_id, nexus_id)).await)
}

#[put("/nodes/{node_id}/nexuses/{nexus_id}", tags(Nexuses))]
async fn put_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
    create: web::Json<CreateNexusBody>,
) -> Result<Json<Nexus>, RestError> {
    let create = create.into_inner().bus_request(node_id, nexus_id);
    RestRespond::result(MessageBus::create_nexus(create).await)
}

#[delete("/nodes/{node_id}/nexuses/{nexus_id}", tags(Nexuses))]
async fn del_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> Result<JsonUnit, RestError> {
    destroy_nexus(Filter::NodeNexus(node_id, nexus_id)).await
}
#[delete("/nexuses/{nexus_id}", tags(Nexuses))]
async fn del_nexus(web::Path(nexus_id): web::Path<NexusId>) -> Result<JsonUnit, RestError> {
    destroy_nexus(Filter::Nexus(nexus_id)).await
}

#[put("/nodes/{node_id}/nexuses/{nexus_id}/share/{protocol}", tags(Nexuses))]
async fn put_node_nexus_share(
    web::Path((node_id, nexus_id, protocol)): web::Path<(NodeId, NexusId, NexusShareProtocol)>,
) -> Result<Json<String>, RestError> {
    let share = ShareNexus {
        node: node_id,
        uuid: nexus_id,
        key: None,
        protocol,
    };
    RestRespond::result(MessageBus::share_nexus(share).await)
}

#[delete("/nodes/{node_id}/nexuses/{nexus_id}/share", tags(Nexuses))]
async fn del_node_nexus_share(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> Result<JsonUnit, RestError> {
    let unshare = UnshareNexus {
        node: node_id,
        uuid: nexus_id,
    };
    RestRespond::result(MessageBus::unshare_nexus(unshare).await).map(JsonUnit::from)
}

async fn destroy_nexus(filter: Filter) -> Result<JsonUnit, RestError> {
    let destroy = match filter.clone() {
        Filter::NodeNexus(node_id, nexus_id) => DestroyNexus {
            node: node_id,
            uuid: nexus_id,
        },
        Filter::Nexus(nexus_id) => {
            let node_id = match MessageBus::get_nexus(filter).await {
                Ok(nexus) => nexus.node,
                Err(error) => return Err(RestError::from(error)),
            };
            DestroyNexus {
                node: node_id,
                uuid: nexus_id,
            }
        }
        _ => {
            return Err(RestError::from(BusError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Nexus,
                source: "destroy_nexus".to_string(),
                extra: "invalid filter for resource".to_string(),
            }))
        }
    };

    RestRespond::result(MessageBus::destroy_nexus(destroy).await).map(JsonUnit::from)
}
