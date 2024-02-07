use crate::{client::AppNodesClientWrapper, shutdown_event::Shutdown};
use snafu::Snafu;
use std::{collections::HashMap, time::Duration};
use tokio::task::JoinError;
use tracing::error;

#[derive(Debug, Snafu)]
pub(crate) enum RegistrationError {
    TokioTaskWait { source: JoinError },
}

/// Default registration interval.
const REGISTRATION_INTERVAL_ON_SUCCESS: Duration = Duration::from_secs(60 * 5);
/// Default registration interval on error.
const REGISTRATION_INTERVAL_ON_ERROR: Duration = Duration::from_secs(30);

pub(crate) async fn run_registration_loop(
    id: String,
    endpoint: String,
    labels: Option<HashMap<String, String>>,
    client: &Option<AppNodesClientWrapper>,
    registration_enabled: bool,
) -> anyhow::Result<()> {
    if !registration_enabled {
        return Ok(());
    }

    let Some(client) = client else {
        return Err(anyhow::anyhow!(
            "Rest API Client should have been initialized if registration is enabled"
        ));
    };

    let mut logged_error = false;
    loop {
        let interval_duration = match client.register_app_node(&id, &endpoint, &labels).await {
            Ok(_) => {
                if logged_error {
                    tracing::info!("Successfully re-registered the app node");
                    logged_error = false;
                }
                REGISTRATION_INTERVAL_ON_SUCCESS
            }
            Err(e) => {
                if !logged_error {
                    error!("Failed to register app node: {:?}", e);
                    logged_error = true;
                }
                REGISTRATION_INTERVAL_ON_ERROR
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(interval_duration) => {}
            _ = Shutdown::wait() => {
                break;
            }
        }
    }
    // Deregister the node from the control plane on termination.
    if let Err(error) = client.deregister_app_node(&id).await {
        error!("Failed to deregister node, {:?}", error);
    }
    Ok(())
}
