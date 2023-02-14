use super::*;
use grpc::operations::registry::traits::RegistryOperations;
use stor_port::types::v0::transport::GetSpecs;

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
