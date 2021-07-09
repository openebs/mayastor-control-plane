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

use actix_http::{
    client::{SendRequestError, TcpConnect, TcpConnectError, TcpConnection},
    encoding::Decoder,
    error::PayloadError,
    Payload, PayloadStream,
};
use actix_service::Service;
use actix_web::{body::Body, dev::ResponseHead, rt::net::TcpStream, web::Bytes};
use actix_web_opentelemetry::ClientExt;
use awc::{http::Uri, Client, ClientBuilder, ClientResponse};

use common_lib::types::v0::openapi::apis::{client, configuration};
use futures::Stream;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::{io::BufReader, string::ToString};

/// Actix Rest Client
#[derive(Clone)]
pub struct ActixRestClient {
    openapi_client: client::ApiClient,
    client: awc::Client,
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
            rest_client.clone(),
            bearer_token,
            trace,
        );
        let openapi_client = client::ApiClient::new(openapi_client_config);

        Ok(Self {
            openapi_client,
            client: rest_client,
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
            client.clone(),
            bearer_token,
            trace,
        );
        let openapi_client = client::ApiClient::new(openapi_client_config);
        Self {
            openapi_client,
            client,
            url: url.to_string(),
            trace,
        }
    }
    async fn get<R>(&self, urn: String) -> ClientResult<R>
    where
        for<'de> R: Deserialize<'de> + Default,
    {
        let uri = format!("{}{}", self.url, urn);
        let rest_response = self.do_get(&uri).await.context(Send {
            details: format!("Failed to get uri {}", uri),
        })?;
        Self::rest_result(rest_response).await
    }
    async fn get_vec<R>(&self, urn: String) -> ClientResult<Vec<R>>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);
        let rest_response = self.do_get(&uri).await.context(Send {
            details: format!("Failed to get_vec uri {}", uri),
        })?;
        Self::rest_vec_result(rest_response).await
    }

    async fn do_get(
        &self,
        uri: &str,
    ) -> Result<ClientResponse<Decoder<Payload<PayloadStream>>>, SendRequestError> {
        if self.trace {
            self.client.get(uri).trace_request().send().await
        } else {
            self.client.get(uri).send().await
        }
    }

    async fn put<R, B: Into<Body>>(&self, urn: String, body: B) -> Result<R, ClientError>
    where
        for<'de> R: Deserialize<'de> + Default,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client
                .put(uri.clone())
                .content_type("application/json")
                .trace_request()
                .send_body(body)
                .await
        } else {
            self.client
                .put(uri.clone())
                .content_type("application/json")
                .send_body(body)
                .await
        };

        let rest_response = result.context(Send {
            details: format!("Failed to put uri {}", uri),
        })?;

        Self::rest_result(rest_response).await
    }
    async fn del<R>(&self, urn: String) -> ClientResult<R>
    where
        for<'de> R: Deserialize<'de> + Default,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client.delete(uri.clone()).trace_request().send().await
        } else {
            self.client.delete(uri.clone()).send().await
        };

        let rest_response = result.context(Send {
            details: format!("Failed to delete uri {}", uri),
        })?;

        Self::rest_result(rest_response).await
    }

    async fn rest_vec_result<S, R>(mut rest_response: ClientResponse<S>) -> ClientResult<Vec<R>>
    where
        S: Stream<Item = Result<Bytes, PayloadError>> + Unpin,
        for<'de> R: Deserialize<'de>,
    {
        let status = rest_response.status();
        let headers = rest_response.headers().clone();
        let head = || {
            let mut head = ResponseHead::new(status);
            head.headers = headers.clone();
            head
        };
        let body = rest_response
            .body()
            .await
            .context(InvalidPayload { head: head() })?;
        if status.is_success() {
            match serde_json::from_slice(&body) {
                Ok(r) => Ok(r),
                Err(_) => {
                    let result = serde_json::from_slice(&body)
                        .context(InvalidBody { head: head(), body })?;
                    Ok(vec![result])
                }
            }
        } else if body.is_empty() {
            Err(ClientError::Header { head: head() })
        } else {
            let error = serde_json::from_slice::<serde_json::Value>(&body)
                .context(InvalidBody { head: head(), body })?;
            Err(ClientError::RestServer {
                head: head(),
                error,
            })
        }
    }

    async fn rest_result<S, R>(mut rest_response: ClientResponse<S>) -> Result<R, ClientError>
    where
        S: Stream<Item = Result<Bytes, PayloadError>> + Unpin,
        for<'de> R: Deserialize<'de> + Default,
    {
        let status = rest_response.status();
        let headers = rest_response.headers().clone();
        let head = || {
            let mut head = ResponseHead::new(status);
            head.headers = headers.clone();
            head
        };
        let body = rest_response
            .body()
            .await
            .context(InvalidPayload { head: head() })?;
        if status.is_success() {
            let empty = body.is_empty();
            let result = serde_json::from_slice(&body).context(InvalidBody { head: head(), body });
            match result {
                Ok(result) => Ok(result),
                Err(_) if empty && std::any::type_name::<R>() == "()" => Ok(R::default()),
                Err(error) => Err(error),
            }
        } else if body.is_empty() {
            Err(ClientError::Header { head: head() })
        } else {
            let error = serde_json::from_slice::<serde_json::Value>(&body)
                .context(InvalidBody { head: head(), body })?;
            Err(ClientError::RestServer {
                head: head(),
                error,
            })
        }
    }
}

/// Result of a Rest Client Operation
/// T is the Object parsed from the Json body
pub type ClientResult<T> = Result<T, ClientError>;

/// Rest Client Error
#[derive(Debug, Snafu)]
pub enum ClientError {
    /// Failed to send message to the server (details in source)
    #[snafu(display("{}, reason: {}", details, source))]
    Send {
        /// Message
        details: String,
        /// Source Request Error
        source: SendRequestError,
    },
    /// Invalid Resource Filter so couldn't send the request
    #[snafu(display("Invalid Resource Filter: {}", details))]
    InvalidFilter {
        /// Message
        details: String,
    },
    /// Response an error code and with an invalid payload
    #[snafu(display("Invalid payload, header: {:?}, reason: {}", head, source))]
    InvalidPayload {
        /// http Header
        head: ResponseHead,
        /// source payload error
        source: PayloadError,
    },
    /// Response an error code and also with an invalid body
    #[snafu(display(
        "Invalid body, header: {:?}, body: {:?}, reason: {}",
        head,
        body,
        source
    ))]
    InvalidBody {
        /// http Header
        head: ResponseHead,
        /// http Body
        body: Bytes,
        /// source json deserialize error
        source: serde_json::Error,
    },
    /// Response an error code and only the header (and so no additional info)
    #[snafu(display("No body, header: {:?}", head))]
    Header {
        /// http Header
        head: ResponseHead,
    },
    /// Error within the Body in valid JSON format, returned by the Rest Server
    #[snafu(display("Http status: {}, error: {}", head.status, error.to_string()))]
    RestServer {
        /// http Header
        head: ResponseHead,
        /// JSON error
        error: serde_json::Value,
    },
}

impl ClientError {
    fn filter(message: &str) -> ClientError {
        ClientError::InvalidFilter {
            details: message.to_string(),
        }
    }
}
