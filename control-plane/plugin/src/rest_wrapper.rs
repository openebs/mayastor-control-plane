use anyhow::Result;
use once_cell::sync::OnceCell;
use openapi::tower::client::{ApiClient, Configuration, Uri, Url};

static REST_SERVER: OnceCell<RestClient> = OnceCell::new();

/// REST client
pub struct RestClient {
    uri: Uri,
    client: ApiClient,
}

impl RestClient {
    /// Initialize the URL of the REST server.
    pub fn init(url: Url, tracing: bool, timeout: std::time::Duration) -> Result<()> {
        REST_SERVER.get_or_try_init(|| Self::new(url, tracing, timeout))?;
        Ok(())
    }

    /// Initialize the URL of the REST server.
    pub fn init_with_config(config: Configuration) -> Result<()> {
        REST_SERVER.get_or_init(|| RestClient::new_with_config(config));
        Ok(())
    }

    /// Create new Rest Client.
    pub fn new(url: Url, tracing: bool, timeout: std::time::Duration) -> Result<RestClient> {
        // TODO: Support HTTPS Certificates
        let uri = url.as_str().parse()?;
        let cfg = Configuration::builder()
            .with_timeout(timeout)
            .with_tracing(tracing)
            .build_url(url)
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed to create openapi configuration, Error: '{:?}'",
                    error
                )
            })?;
        Ok(Self {
            uri,
            client: ApiClient::new(cfg),
        })
    }

    /// Create new Rest Client from the given `Configuration`.
    pub fn new_with_config(config: Configuration) -> RestClient {
        Self {
            uri: config.base_path.clone(),
            client: ApiClient::new(config),
        }
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
    pub fn uri(&self) -> &Uri {
        &self.uri
    }
}
