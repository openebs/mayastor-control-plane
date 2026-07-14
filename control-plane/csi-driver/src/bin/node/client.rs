use stor_port::types::v0::openapi::{
    apis::{
        app_nodes_api::tower::client::{direct::AppNodes, AppNodesClient},
        volumes_api::tower::client::{direct::Volumes, VolumesClient},
    },
    clients::{
        self,
        tower::{configuration::Configuration, StatusCode},
    },
    models::{NexusState, RegisterAppNode, RestJsonError, TransportCaps},
};

use std::sync::Arc;
use tonic::Status;

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

/// Wrapper for AppNodes REST API client.
pub(crate) struct AppNodesClientWrapper {
    client: AppNodesClient,
}

impl AppNodesClientWrapper {
    /// Create from a shared configuration.
    pub(crate) fn new(configuration: Configuration) -> Self {
        Self {
            client: AppNodesClient::new(Arc::new(configuration)),
        }
    }

    /// Register an app node.
    pub(crate) async fn register_app_node(
        &self,
        app_node_id: &str,
        endpoint: &str,
        labels: &Option<std::collections::HashMap<String, String>>,
        transport_caps: Option<TransportCaps>,
    ) -> Result<(), ApiClientError> {
        self.client
            .register_app_node(
                app_node_id,
                RegisterAppNode::new_all(endpoint, labels.clone(), transport_caps),
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

/// Wrapper for Volumes REST API client.
#[derive(Clone)]
pub(crate) struct VolumesClientWrapper {
    client: VolumesClient,
}

impl VolumesClientWrapper {
    /// Create from a shared configuration.
    pub(crate) fn new(configuration: Configuration) -> Self {
        Self {
            client: VolumesClient::new(Arc::new(configuration)),
        }
    }

    /// Get the target URI for the given volume.
    pub(crate) async fn volume_uri(
        &self,
        volume_id: &uuid::Uuid,
    ) -> Result<String, ApiClientError> {
        let volume: stor_port::types::v0::openapi::models::Volume =
            self.client.get_volume(volume_id).await?;
        let Some(target) = volume.state.target else {
            return Err(ApiClientError::Unavailable(
                "Volume target is not available".into(),
            ));
        };
        if !matches!(target.state, NexusState::Online | NexusState::Degraded) {
            return Err(ApiClientError::Unavailable(
                "Volume target is not ready for I/O".into(),
            ));
        }
        // TODO: check for other volume statuses, example ONLINE?
        Ok(target.device_uri)
    }
}
