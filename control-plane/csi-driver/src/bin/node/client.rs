use stor_port::types::v0::openapi::{
    apis::app_nodes_api::tower::client::AppNodesClient,
    clients,
    clients::tower::StatusCode,
    models::{RegisterAppNode, RestJsonError},
};

use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc, time::Duration};
use stor_port::types::v0::openapi::apis::app_nodes_api::tower::client::direct::AppNodes;
use tonic::Status;
use tracing::info;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ApiClientError {
    // Error while communicating with the server.
    ServerCommunication(String),
    // Requested resource already exists. This error has a dedicated variant
    // in order to handle resource idempotency properly.
    ResourceAlreadyExists(String),
    // No resource instance exists.
    ResourceNotExists(String),
    NotImplemented(String),
    RequestTimeout(String),
    Aborted(String),
    Conflict(String),
    ResourceExhausted(String),
    // Generic operation errors.
    GenericOperation(StatusCode, String),
    // Problems with parsing response body.
    InvalidResponse(String),
    /// URL is malformed.
    MalformedUrl(String),
    /// Invalid argument.
    InvalidArgument(String),
    /// Unavailable.
    Unavailable(String),
    /// Precondition Failed.
    PreconditionFailed(String),
}

impl From<ApiClientError> for Status {
    fn from(error: ApiClientError) -> Self {
        match error {
            ApiClientError::ResourceNotExists(reason) => Status::not_found(reason),
            ApiClientError::NotImplemented(reason) => Status::unimplemented(reason),
            ApiClientError::RequestTimeout(reason) => Status::deadline_exceeded(reason),
            ApiClientError::Conflict(reason) => Status::aborted(reason),
            ApiClientError::Aborted(reason) => Status::aborted(reason),
            ApiClientError::Unavailable(reason) => Status::unavailable(reason),
            ApiClientError::InvalidArgument(reason) => Status::invalid_argument(reason),
            ApiClientError::PreconditionFailed(reason) => Status::failed_precondition(reason),
            ApiClientError::ResourceExhausted(reason) => Status::resource_exhausted(reason),
            error => Status::internal(format!("Operation failed: {error:?}")),
        }
    }
}

impl From<clients::tower::Error<RestJsonError>> for ApiClientError {
    fn from(error: clients::tower::Error<RestJsonError>) -> Self {
        match error {
            clients::tower::Error::Request(request) => {
                Self::ServerCommunication(request.to_string())
            }
            clients::tower::Error::Response(response) => match response {
                clients::tower::ResponseError::Expected(_) => {
                    // TODO: Revisit status codes checks after improving REST API HTTP codes
                    // (CAS-1124).
                    let detailed = response.to_string();
                    match response.status() {
                        StatusCode::NOT_FOUND => Self::ResourceNotExists(detailed),
                        StatusCode::UNPROCESSABLE_ENTITY => Self::ResourceAlreadyExists(detailed),
                        StatusCode::NOT_IMPLEMENTED => Self::NotImplemented(detailed),
                        StatusCode::REQUEST_TIMEOUT => Self::RequestTimeout(detailed),
                        StatusCode::CONFLICT => Self::Conflict(detailed),
                        StatusCode::INSUFFICIENT_STORAGE => Self::ResourceExhausted(detailed),
                        StatusCode::SERVICE_UNAVAILABLE => Self::Unavailable(detailed),
                        StatusCode::PRECONDITION_FAILED => Self::PreconditionFailed(detailed),
                        StatusCode::BAD_REQUEST => Self::InvalidArgument(detailed),
                        status => Self::GenericOperation(status, detailed),
                    }
                }
                clients::tower::ResponseError::PayloadError { .. } => {
                    Self::InvalidResponse(response.to_string())
                }
                clients::tower::ResponseError::Unexpected(_) => {
                    Self::InvalidResponse(response.to_string())
                }
            },
        }
    }
}

/// Default rest api timeout for requests.
const DEFAULT_TIMEOUT_FOR_REST_REQUESTS: Duration = Duration::from_secs(5);

/// Wrapper for AppNodes REST API client.
pub(crate) struct AppNodesClientWrapper {
    client: AppNodesClient,
}

impl AppNodesClientWrapper {
    /// Initialize AppNodes API client instance.
    pub(crate) fn initialize(
        endpoint: Option<&String>,
    ) -> anyhow::Result<Option<AppNodesClientWrapper>> {
        let Some(endpoint) = endpoint else {
            return Ok(None);
        };

        let url = clients::tower::Url::parse(endpoint)
            .map_err(|error| anyhow!("Invalid API endpoint URL {}: {:?}", endpoint, error))?;

        let tower = clients::tower::Configuration::builder()
            .with_timeout(DEFAULT_TIMEOUT_FOR_REST_REQUESTS)
            .build_url(url)
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed to create openapi configuration, Error: '{:?}'",
                    error
                )
            })?;

        info!(
            "API client is initialized with endpoint {}, request timeout = {:?}",
            endpoint, DEFAULT_TIMEOUT_FOR_REST_REQUESTS,
        );

        Ok(Some(Self {
            client: AppNodesClient::new(Arc::new(tower)),
        }))
    }

    /// Register an app node.
    pub(crate) async fn register_app_node(
        &self,
        app_node_id: &str,
        endpoint: &str,
        labels: &Option<HashMap<String, String>>,
    ) -> Result<(), ApiClientError> {
        self.client
            .register_app_node(
                app_node_id,
                RegisterAppNode::new_all(endpoint, labels.clone()),
            )
            .await?;

        Ok(())
    }

    /// Deregister an app node.
    pub(crate) async fn deregister_app_node(
        &self,
        app_node_id: &str,
    ) -> Result<(), ApiClientError> {
        self.client.deregister_app_node(app_node_id).await?;

        Ok(())
    }
}
