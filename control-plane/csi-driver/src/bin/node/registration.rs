use csi_driver::client::RestApiClient;
use snafu::Snafu;
use std::{collections::HashMap, time::Duration};
use tokio::task::JoinError;
use tracing::{info, trace};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub(crate) enum RegistrationError {
    TokioTaskWait { source: JoinError },
}

pub(crate) async fn run_registration_loop(
    id: String,
    endpoint: String,
    labels: Option<HashMap<String, String>>,
) -> Result<(), RegistrationError> {
    let client = RestApiClient::get_client();

    tokio::spawn(async move {
        loop {
            let interval_duration =
                match client.register_frontend_node(&id, &endpoint, &labels).await {
                    Ok(_) => {
                        info!("Successful registration");
                        // If register_node is successful, set the interval to 5 minutes
                        Duration::from_secs(5)
                    }
                    Err(e) => {
                        trace!("Failed to register node: {:?}", e);
                        // If register_node fails, set the interval to 30 seconds
                        Duration::from_secs(1)
                    }
                };

            tokio::time::sleep(interval_duration).await;
        }
    })
    .await
    .map_err(|error| RegistrationError::TokioTaskWait { source: error })?;

    Ok(())
}
