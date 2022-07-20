use super::*;
use common_lib::types::v0::message_bus::{GetStates, States};
use grpc::operations::registry::traits::RegistryOperations;

fn client() -> impl RegistryOperations {
    core_grpc().registry()
}

// todo: once the state schema is added to the spec yaml then replace this with the autogen code
pub(super) fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.service(
        actix_web::web::resource("/states")
            .name("get_states")
            .guard(actix_web::guard::Get())
            .route(actix_web::web::get().to(get_states)),
    );
}

async fn get_states() -> Result<actix_web::web::Json<States>, RestError<RestJsonError>> {
    let states = client().get_states(&GetStates {}, None).await?;
    Ok(actix_web::web::Json(states))
}
