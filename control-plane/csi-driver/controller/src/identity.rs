use crate::{ApiClientError, IoEngineApiClient};
use rpc::csi::*;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::{debug, error, instrument};

#[derive(Debug, Default)]
pub struct CsiIdentitySvc {}

const CSI_PLUGIN_NAME: &str = "io.openebs.csi-mayastor";
const CSI_PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tonic::async_trait]
impl rpc::csi::identity_server::Identity for CsiIdentitySvc {
    #[instrument]
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        debug!(
            "Request to get CSI plugin info, plugin: {}:{}",
            CSI_PLUGIN_NAME, CSI_PLUGIN_VERSION,
        );
        Ok(Response::new(GetPluginInfoResponse {
            name: CSI_PLUGIN_NAME.to_string(),
            vendor_version: CSI_PLUGIN_VERSION.to_string(),
            // Optional manifest is empty.
            manifest: HashMap::new(),
        }))
    }

    #[instrument]
    async fn get_plugin_capabilities(
        &self,
        _request: tonic::Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        debug!("Request to get CSI plugin capabilities");

        let capabilities = vec![
            plugin_capability::service::Type::ControllerService,
            plugin_capability::service::Type::VolumeAccessibilityConstraints,
        ];

        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: capabilities
                .into_iter()
                .map(|c| PluginCapability {
                    r#type: Some(plugin_capability::Type::Service(
                        plugin_capability::Service { r#type: c as i32 },
                    )),
                })
                .collect(),
        }))
    }

    #[instrument]
    async fn probe(
        &self,
        _request: tonic::Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        debug!("Request to probe CSI plugin");

        // Make sure REST API gateway is accessible.
        // If a server communication error occurs, return false rather than an error. This
        // communicates to the Container Orchestrator that the plugin is not yet initialised but
        // should not be restarted. See the CSI spec:
        // https://github.com/container-storage-interface/spec/blob/5b0d4540158a260cb3347ef1c87ede8600afb9bf/csi.proto#L252-L256
        let ready = match IoEngineApiClient::get_client().list_nodes().await {
            Ok(_) => true,
            Err(ApiClientError::ServerCommunication { .. }) => {
                error!("Failed to access REST API gateway, CSI Controller plugin is not ready",);
                false
            }
            Err(_) => true,
        };

        debug!("CSI plugin ready: {}", ready);
        Ok(Response::new(ProbeResponse { ready: Some(ready) }))
    }
}
