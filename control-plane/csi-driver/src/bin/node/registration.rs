use csi_driver::client::IoEngineApiClient;
use futures::FutureExt;
use snafu::Snafu;
use std::{collections::HashMap, time::Duration};
use tokio::task::JoinError;
use tracing::{info, trace};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub(crate) enum RegistrationError {
    TokioTaskWait { source: JoinError },
}

#[allow(unused_assignments)]
pub(crate) async fn run_registration_loop(
    id: String,
    endpoint: String,
    labels: Option<HashMap<String, String>>,
) -> Result<(), RegistrationError> {
    let client = IoEngineApiClient::get_client();

    tokio::spawn(async move {
        let mut interval_duration = Duration::from_secs(5);
        loop {
            match client.register_frontend_node(&id, &endpoint, &labels).await {
                Ok(_) => {
                    info!("Successful registration");
                    // If register_node is successful, set the interval to 5 minutes
                    interval_duration = Duration::from_secs(5);
                }
                Err(e) => {
                    trace!("Failed to register node: {:?}", e);
                    // If register_node fails, set the interval to 30 seconds
                    interval_duration = Duration::from_secs(1);
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(interval_duration).fuse() => continue,
            }
        }
    })
    .await
    .map_err(|error| RegistrationError::TokioTaskWait { source: error })?;

    Ok(())
}
