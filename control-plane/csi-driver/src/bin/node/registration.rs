use crate::client::AppNodesClientWrapper;
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
    client: &AppNodesClientWrapper,
) {
    let mut logged_error = false;
    loop {
        let interval_duration = match client.register_app_node(&id, &endpoint, &labels).await {
            Ok(_) => REGISTRATION_INTERVAL_ON_SUCCESS,
            Err(e) => {
                if !logged_error {
                    error!("Failed to register app node: {:?}", e);
                    logged_error = true;
                }
                REGISTRATION_INTERVAL_ON_ERROR
            }
        };
        tokio::time::sleep(interval_duration).await;
    }
}
