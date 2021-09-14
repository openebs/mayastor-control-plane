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

use actix_http::client::{TcpConnect, TcpConnectError, TcpConnection};
use actix_service::Service;
use actix_web::rt::net::TcpStream;

use awc::{http::Uri, Client, ClientBuilder};

use common_lib::types::v0::openapi::apis::{client, configuration};

use std::{io::BufReader, string::ToString};

/// Actix Rest Client
#[derive(Clone)]
pub struct ActixRestClient {
    openapi_client_v0: client::ApiClient,
    url: String,
    trace: bool,
}

impl ActixRestClient {
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
        let mut builder = Client::builder().timeout(timeout);
        if let Some(token) = &bearer_token {
            builder = builder.bearer_auth(token);
        }

        match url.scheme() {
            "https" => Self::new_https(
                builder,
                url.as_str().trim_end_matches('/'),
                bearer_token,
                trace,
            ),
            "http" => Ok(Self::new_http(
                builder,
                url.as_str().trim_end_matches('/'),
                bearer_token,
                trace,
            )),
            invalid => {
                let msg = format!("Invalid url scheme: {}", invalid);
                Err(anyhow::Error::msg(msg))
            }
        }
    }
    /// creates a new secure client
    fn new_https(
        client: ClientBuilder<
            impl Service<
                    TcpConnect<Uri>,
                    Response = TcpConnection<Uri, TcpStream>,
                    Error = TcpConnectError,
                > + Clone
                + 'static,
        >,
        url: &str,
        bearer_token: Option<String>,
        trace: bool,
    ) -> anyhow::Result<Self> {
        let cert_file = &mut BufReader::new(&std::include_bytes!("../certs/rsa/ca.cert")[..]);

        let mut config = rustls::ClientConfig::new();
        config
            .root_store
            .add_pem_file(cert_file)
            .map_err(|_| anyhow::anyhow!("Add pem file to the root store!"))?;
        let connector = awc::Connector::new().rustls(std::sync::Arc::new(config));

        let rest_client = client.connector(connector).finish();

        let openapi_client_config = configuration::Configuration::new_with_client(
            &format!("{}/v0", url),
            rest_client,
            bearer_token,
            trace,
        );
        let openapi_client = client::ApiClient::new(openapi_client_config);

        Ok(Self {
            openapi_client_v0: openapi_client,
            url: url.to_string(),
            trace,
        })
    }
    /// creates a new client
    fn new_http(
        client: ClientBuilder<
            impl Service<
                    TcpConnect<Uri>,
                    Response = TcpConnection<Uri, TcpStream>,
                    Error = TcpConnectError,
                > + Clone
                + 'static,
        >,
        url: &str,
        bearer_token: Option<String>,
        trace: bool,
    ) -> Self {
        let client = client.finish();
        let openapi_client_config = configuration::Configuration::new_with_client(
            &format!("{}/v0", url),
            client,
            bearer_token,
            trace,
        );
        let openapi_client = client::ApiClient::new(openapi_client_config);
        Self {
            openapi_client_v0: openapi_client,
            url: url.to_string(),
            trace,
        }
    }
}
