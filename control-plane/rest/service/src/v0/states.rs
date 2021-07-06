use super::*;
use common_lib::types::v0::message_bus::{GetStates, States};
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

pub(super) fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.service(get_states);
}

#[get("/states")]
async fn get_states() -> Result<Json<States>, RestError> {
    RestRespond::result(MessageBus::get_states(GetStates {}).await)
}
