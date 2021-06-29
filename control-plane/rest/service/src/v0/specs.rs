use super::*;
use common_lib::types::v0::message_bus::mbus::{GetSpecs, Specs};
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

pub(super) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_specs);
}

#[get("/specs", tags(Specs))]
async fn get_specs() -> Result<Json<Specs>, RestError> {
    RestRespond::result(MessageBus::get_specs(GetSpecs {}).await)
}
