use anyhow::Result;
use awc::ClientBuilder;
use once_cell::sync::OnceCell;
use openapi::apis::{client::ApiClient, configuration::Configuration};
use reqwest::Url;

static REST_SERVER: OnceCell<Url> = OnceCell::new();

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
        REST_SERVER.get_or_init(|| url);
        Ok(())
    }

    /// Get an ApiClient to use for REST calls.
    pub(crate) fn client() -> ApiClient {
        let client = ClientBuilder::new().finish();
        let url = REST_SERVER.get().unwrap().join("/v0").unwrap();
        let cfg = Configuration::new_with_client(url.as_str(), client, None, true);
        ApiClient::new(cfg)
    }
}
