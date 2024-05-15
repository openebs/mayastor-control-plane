//! Implementation of gRPC methods from CSI Identity gRPC service.

use csi_driver::{csi::*, csi_plugin_name, plugin_capabilities::plugin_capabilities};

use std::{boxed::Box, collections::HashMap};
use tonic::{Request, Response, Status};
use tracing::debug;

// TODO: can we generate version with commit SHA dynamically?
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
pub(crate) struct Identity {}

impl Identity {}
#[tonic::async_trait]
impl identity_server::Identity for Identity {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        debug!(
            "GetPluginInfo request ({}:{})",
            csi_plugin_name(),
            PLUGIN_VERSION
        );

        Ok(Response::new(GetPluginInfoResponse {
            name: csi_plugin_name(),
            vendor_version: PLUGIN_VERSION.to_owned(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        debug!("GetPluginCapabilities request: {:?}", request);

        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: plugin_capabilities(),
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        // CSI plugin is independent of the io-engine so it's always ready
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}
