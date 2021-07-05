use super::*;
use common_lib::types::v0::message_bus::GetSpecs;
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

#[async_trait::async_trait]
impl apis::SpecsApi for RestApi {
    async fn get_specs() -> Result<Json<models::Specs>, RestError<RestJsonError>> {
        let specs = MessageBus::get_specs(GetSpecs {}).await?;
        Ok(Json(specs.into()))
    }
}
