use super::*;
use common_lib::types::v0::message_bus::GetSpecs;
use grpc::operations::registry::traits::RegistryOperations;

fn client() -> impl RegistryOperations {
    core_grpc().registry()
}

#[async_trait::async_trait]
impl apis::actix_server::Specs for RestApi {
    async fn get_specs() -> Result<models::Specs, RestError<RestJsonError>> {
        let specs = client().get_specs(&GetSpecs {}, None).await?;
        Ok(specs.into())
    }
}
