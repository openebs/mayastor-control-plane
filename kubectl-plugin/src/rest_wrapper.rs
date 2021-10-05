use anyhow::Result;
use once_cell::sync::OnceCell;
use openapi::tower::client::{ApiClient, Configuration, Url};
use std::time::Duration;

static REST_SERVER: OnceCell<ApiClient> = OnceCell::new();

/// REST client
pub struct RestClient {}

impl RestClient {
    /// Initialise the URL of the REST server.
    pub fn init(url: &Url) -> Result<()> {
        // Only HTTP is supported, so fix up the scheme and port.
        // TODO: Support HTTPS
        let mut url = url.clone();
        url.set_scheme("http")
            .map_err(|_| anyhow::anyhow!("Failed to set REST client scheme"))?;
        if url.port().is_none() {
            url.set_port(Some(30011))
                .map_err(|_| anyhow::anyhow!("Failed to set REST client port"))?;
        }
        url.set_path(&format!("{}/v0", url.path().trim_end_matches('/')));
        let cfg = Configuration::new(url, Duration::from_secs(5), None, None, false).map_err(
            |error| {
                anyhow::anyhow!(
                    "Failed to create openapi configuration, Error: '{:?}'",
                    error
                )
            },
        )?;
        REST_SERVER.get_or_init(|| ApiClient::new(cfg));
        Ok(())
    }

    /// Get an ApiClient to use for REST calls.
    pub(crate) fn client() -> &'static ApiClient {
        REST_SERVER.get().unwrap()
    }
}
