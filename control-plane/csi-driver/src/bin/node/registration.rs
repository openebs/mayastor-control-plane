use crate::{client::AppNodesClientWrapper, shutdown_event::Shutdown};
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::openapi::models::TransportCaps;
use tracing::error;

/// Default registration interval.
const REGISTRATION_INTERVAL_ON_SUCCESS: Duration = Duration::from_secs(60 * 5);
/// Default registration interval on error.
const REGISTRATION_INTERVAL_ON_ERROR: Duration = Duration::from_secs(30);

pub(crate) async fn run_registration_loop(
    id: String,
    endpoint: String,
    labels: Option<HashMap<String, String>>,
    transport_caps: Option<TransportCaps>,
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
        let interval_duration = match client
            .register_app_node(&id, &endpoint, &labels, transport_caps.clone())
            .await
        {
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
