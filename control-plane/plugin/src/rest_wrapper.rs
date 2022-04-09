use anyhow::Result;
use once_cell::sync::OnceCell;
use openapi::tower::client::{ApiClient, Configuration, Url};

static REST_SERVER: OnceCell<RestClient> = OnceCell::new();

/// REST client
pub struct RestClient {
    url: Url,
    client: ApiClient,
}

impl RestClient {
    /// Initialise the URL of the REST server.
    pub fn init(url: Url, timeout: std::time::Duration) -> Result<()> {
        REST_SERVER.get_or_try_init(|| Self::new(url, timeout))?;
        Ok(())
    }

    /// Create new Rest Client.
    pub fn new(mut url: Url, timeout: std::time::Duration) -> Result<RestClient> {
        // TODO: Support HTTPS Certificates
        if url.port().is_none() {
            url.set_port(Some(30011))
                .map_err(|_| anyhow::anyhow!("Failed to set REST client port"))?;
        }
        let cfg = Configuration::new(url.clone(), timeout, None, None, true).map_err(|error| {
            anyhow::anyhow!(
                "Failed to create openapi configuration, Error: '{:?}'",
                error
            )
        })?;
        Ok(Self {
            url,
            client: ApiClient::new(cfg),
        })
    }

    /// Get a global `Self` or panic
    pub fn get_or_panic() -> &'static Self {
        REST_SERVER.get().unwrap()
    }

    /// Get an ApiClient to use for REST calls.
    pub fn client() -> &'static ApiClient {
        &REST_SERVER.get().unwrap().client
    }

    /// Get the Rest Base Url.
    pub fn url(&self) -> &Url {
        &self.url
    }
}
