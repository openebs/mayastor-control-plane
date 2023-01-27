#![warn(missing_docs)]
#![allow(clippy::field_reassign_with_default)]
//! Client library which exposes information from the different mayastor
//! control plane services through REST
//! Different versions are exposed through `versions`
//!
//! # Example:
//!
//! async fn main() {
//!     use rest_client::versions::v0::RestClient;
//!     let client = RestClient::new("https://localhost:8080");
//!     let _nodes = client.get_nodes().await.unwrap();
//! }

/// expose different versions of the client
pub mod versions;

use common_lib::types::v0::openapi::client;

/// Tower Rest Client
#[derive(Clone)]
pub struct RestClient {
    openapi_client_v0: client::direct::ApiClient,
    trace: bool,
}

impl RestClient {
    /// creates a new client which uses the specified `url`
    /// uses the rustls connector if the url has the https scheme
    pub fn new(url: &str, trace: bool, bearer_token: Option<String>) -> anyhow::Result<Self> {
        Self::new_timeout(url, trace, bearer_token, std::time::Duration::from_secs(5))
    }
    /// creates a new client which uses the specified `url`
    /// uses the rustls connector if the url has the https scheme
    pub fn new_timeout(
        url: &str,
        trace: bool,
        bearer_token: Option<String>,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let url: url::Url = url.parse()?;

        match url.scheme() {
            "https" => Self::new_https(url, timeout, bearer_token, trace),
            "http" => Self::new_http(url, timeout, bearer_token, trace),
            invalid => {
                let msg = format!("Invalid url scheme: {}", invalid);
                Err(anyhow::Error::msg(msg))
            }
        }
    }
    /// creates a new secure client
    fn new_https(
        url: url::Url,
        timeout: std::time::Duration,
        bearer_token: Option<String>,
        trace: bool,
    ) -> anyhow::Result<Self> {
        let cert_file = &std::include_bytes!("../certs/rsa/ca.cert")[..];

        let openapi_client_config =
            client::Configuration::new(url, timeout, bearer_token, Some(cert_file), trace, None)
                .map_err(|e| anyhow::anyhow!("Failed to create rest client config: '{:?}'", e))?;
        let openapi_client = client::direct::ApiClient::new(openapi_client_config);

        Ok(Self {
            openapi_client_v0: openapi_client,
            trace,
        })
    }
    /// creates a new client
    fn new_http(
        url: url::Url,
        timeout: std::time::Duration,
        bearer_token: Option<String>,
        trace: bool,
    ) -> anyhow::Result<Self> {
        let openapi_client_config =
            client::Configuration::new(url, timeout, bearer_token, None, trace, None)
                .map_err(|e| anyhow::anyhow!("Failed to create rest client config: '{:?}'", e))?;
        let openapi_client = client::direct::ApiClient::new(openapi_client_config);
        Ok(Self {
            openapi_client_v0: openapi_client,
            trace,
        })
    }
}
