use crate::CsiControllerConfig;
use common_lib::types::v0::openapi::{
    clients,
    clients::tower::StatusCode,
    models::{
        CreateVolumeBody, ExplicitNodeTopology, LabelledTopology, Node, NodeTopology, Pool,
        PoolTopology, RestJsonError, Topology, Volume, VolumePolicy, VolumeShareProtocol, Volumes,
    },
};

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use tracing::{debug, info, instrument};

#[derive(Debug, PartialEq, Eq)]
pub enum ApiClientError {
    // Error while communicating with the server.
    ServerCommunication(String),
    // Requested resource already exists. This error has a dedicated variant
    // in order to handle resource idempotency properly.
    ResourceAlreadyExists(String),
    // No resource instance exists.
    ResourceNotExists(String),
    // Generic operation errors.
    GenericOperation(String),
    // Problems with parsing response body.
    InvalidResponse(String),
    /// URL is malformed.
    MalformedUrl(String),
    /// Invalid argument.
    InvalidArgument(String),
}

/// Placeholder for volume topology for volume creation operation.
#[derive(Debug)]
pub struct CreateVolumeTopology {
    inclusive_label_topology: HashMap<String, String>,
    allowed_nodes: Vec<String>,
    preferred_nodes: Vec<String>,
}

impl CreateVolumeTopology {
    pub fn new(
        allowed_nodes: Vec<String>,
        preferred_nodes: Vec<String>,
        inclusive_label_topology: HashMap<String, String>,
    ) -> Self {
        Self {
            allowed_nodes,
            preferred_nodes,
            inclusive_label_topology,
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
                    if response.status() == StatusCode::NOT_FOUND {
                        Self::ResourceNotExists(response.to_string())
                    } else if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
                        Self::ResourceAlreadyExists(response.to_string())
                    } else {
                        Self::GenericOperation(response.to_string())
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

static REST_CLIENT: OnceCell<IoEngineApiClient> = OnceCell::new();

/// Single instance API client for accessing REST API gateway.
/// Encapsulates communication with REST API by exposing a set of
/// high-level API functions, which perform (de)serialization
/// of API request/response objects.
#[derive(Debug)]
pub struct IoEngineApiClient {
    rest_client: clients::tower::ApiClient,
}

impl IoEngineApiClient {
    /// Initialize API client instance. Must be called prior to
    /// obtaining the client instance.
    pub fn initialize() -> Result<()> {
        if REST_CLIENT.get().is_some() {
            return Err(anyhow!("API client already initialized"));
        }

        let cfg = CsiControllerConfig::get_config();
        let endpoint = cfg.rest_endpoint();

        let url = clients::tower::Url::parse(endpoint)
            .map_err(|error| anyhow!("Invalid API endpoint URL {}: {:?}", endpoint, error))?;
        let tower = clients::tower::Configuration::new(url, cfg.io_timeout(), None, None, true)
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed to create openapi configuration, Error: '{:?}'",
                    error
                )
            })?;

        REST_CLIENT.get_or_init(|| Self {
            rest_client: clients::tower::ApiClient::new(tower),
        });

        info!(
            "API client is initialized with endpoint {}, I/O timeout = {:?}",
            endpoint,
            cfg.io_timeout(),
        );
        Ok(())
    }

    /// Obtain client instance. Panics if called before the client
    /// has been initialized.
    pub fn get_client() -> &'static IoEngineApiClient {
        REST_CLIENT.get().expect("Rest client is not initialized")
    }
}

impl IoEngineApiClient {
    /// List all nodes available in IoEngine cluster.
    pub async fn list_nodes(&self) -> Result<Vec<Node>, ApiClientError> {
        let response = self.rest_client.nodes_api().get_nodes().await?;
        Ok(response.into_body())
    }

    /// List all pools available in IoEngine cluster.
    pub async fn list_pools(&self) -> Result<Vec<Pool>, ApiClientError> {
        let response = self.rest_client.pools_api().get_pools().await?;
        Ok(response.into_body())
    }

    /// List all volumes available in IoEngine cluster.
    pub async fn list_volumes(
        &self,
        max_entries: i32,
        starting_token: String,
    ) -> Result<Volumes, ApiClientError> {
        let max_entries = max_entries as isize;
        let starting_token = if starting_token.is_empty() {
            0
        } else {
            starting_token.parse::<isize>().map_err(|_| {
                ApiClientError::InvalidArgument(
                    "Failed to parse starting token as an isize".to_string(),
                )
            })?
        };

        let response = self
            .rest_client
            .volumes_api()
            .get_volumes(max_entries, Some(starting_token))
            .await?;
        Ok(response.into_body())
    }

    /// List pools available on target IoEngine node.
    pub async fn get_node_pools(&self, node: &str) -> Result<Vec<Pool>, ApiClientError> {
        let pools = self.rest_client.pools_api().get_node_pools(node).await?;
        Ok(pools.into_body())
    }

    /// Create a volume of target size and provision storage resources for it.
    /// This operation is not idempotent, so the caller is responsible for taking
    /// all actions with regards to idempotency.
    #[instrument(fields(volume.uuid = %volume_id), skip(volume_id))]
    pub async fn create_volume(
        &self,
        volume_id: &uuid::Uuid,
        replicas: u8,
        size: u64,
        volume_topology: CreateVolumeTopology,
        _pinned_volume: bool,
        thin: bool,
    ) -> Result<Volume, ApiClientError> {
        let topology = Topology::new_all(
            Some(NodeTopology::explicit(ExplicitNodeTopology::new(
                volume_topology.allowed_nodes,
                volume_topology.preferred_nodes,
            ))),
            Some(PoolTopology::labelled(LabelledTopology::new(
                HashMap::new(),
                volume_topology.inclusive_label_topology,
            ))),
        );

        let req = CreateVolumeBody {
            replicas,
            size,
            thin,
            topology: Some(topology),
            policy: VolumePolicy::new_all(true),
            labels: None,
        };

        let result = self
            .rest_client
            .volumes_api()
            .put_volume(volume_id, req)
            .await?;
        Ok(result.into_body())
    }

    /// Delete volume and reclaim all storage resources associated with it.
    /// This operation is idempotent, so the caller does not see errors indicating
    /// absence of the resource.
    #[instrument(fields(volume.uuid = %volume_id), skip(volume_id))]
    pub async fn delete_volume(&self, volume_id: &uuid::Uuid) -> Result<(), ApiClientError> {
        Self::delete_idempotent(
            self.rest_client.volumes_api().del_volume(volume_id).await,
            true,
        )?;
        debug!(volume.uuid=%volume_id, "Volume successfully deleted");
        Ok(())
    }

    /// Check HTTP status code, handle DELETE idempotency transparently.
    pub fn delete_idempotent<T>(
        result: Result<clients::tower::ResponseContent<T>, clients::tower::Error<RestJsonError>>,
        idempotent: bool,
    ) -> Result<(), ApiClientError> {
        match result {
            Ok(_) => Ok(()),
            Err(clients::tower::Error::Request(error)) => {
                Err(clients::tower::Error::Request(error).into())
            }
            Err(clients::tower::Error::Response(response)) => match response.status() {
                // Handle idempotency as requested by the caller.
                StatusCode::NOT_FOUND
                | StatusCode::NO_CONTENT
                | StatusCode::PRECONDITION_FAILED => {
                    if idempotent {
                        Ok(())
                    } else {
                        Err(clients::tower::Error::Response(response).into())
                    }
                }
                _ => Err(clients::tower::Error::Response(response).into()),
            },
        }
    }

    /// Get specific volume.
    #[instrument(fields(volume.uuid = %volume_id), skip(volume_id))]
    pub async fn get_volume(&self, volume_id: &uuid::Uuid) -> Result<Volume, ApiClientError> {
        let volume = self.rest_client.volumes_api().get_volume(volume_id).await?;
        Ok(volume.into_body())
    }

    /// Unpublish volume (i.e. destroy a target which exposes the volume).
    #[instrument(fields(volume.uuid = %volume_id), skip(volume_id))]
    pub async fn unpublish_volume(
        &self,
        volume_id: &uuid::Uuid,
        force: bool,
    ) -> Result<(), ApiClientError> {
        Self::delete_idempotent(
            self.rest_client
                .volumes_api()
                .del_volume_target(volume_id, Some(force))
                .await,
            true,
        )?;
        debug!(volume.uuid=%volume_id, "Volume target successfully deleted");
        Ok(())
    }

    /// Publish volume (i.e. make it accessible via specified protocol by creating a target).
    #[instrument(fields(volume.uuid = %volume_id), skip(volume_id))]
    pub async fn publish_volume(
        &self,
        volume_id: &uuid::Uuid,
        node: &str,
        protocol: VolumeShareProtocol,
    ) -> Result<Volume, ApiClientError> {
        let volume = self
            .rest_client
            .volumes_api()
            .put_volume_target(volume_id, node, protocol)
            .await?;
        Ok(volume.into_body())
    }
}
