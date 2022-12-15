use crate::{dev::Device, volume_serializer::VolumeOpGuard};
use grpc::csi_node_nvme::{
    nvme_operations_server::NvmeOperations, NvmeConnectRequest, NvmeConnectResponse,
};
use std::collections::HashMap;
use tonic::Response;
use tracing::{info, warn};

#[derive(Debug, Default)]
pub(crate) struct NvmeOperationsSvc {}

#[tonic::async_trait]
impl NvmeOperations for NvmeOperationsSvc {
    async fn nvme_connect(
        &self,
        request: tonic::Request<NvmeConnectRequest>,
    ) -> Result<tonic::Response<NvmeConnectResponse>, tonic::Status> {
        let req = request.into_inner();
        info!(request=?req, "Nvme connection request for replace path");

        let uri: &str = req.uri.as_str();

        // Create a new Volume Operation Guard.
        let _guard = VolumeOpGuard::new(uri)?;

        let publish_context: HashMap<String, String> = match req.publish_context {
            Some(map_wrapper) => map_wrapper.map,
            None => HashMap::new(),
        };

        // Get the nvmf device object from the uri.
        let mut device = Device::parse(uri)?;

        // Parse the parameters from publish context.
        device.parse_parameters(&publish_context).await?;
        // Make nvme connection.
        device.attach().await?;
        // Fixup for nvme timeout.
        let _ = device
            .fixup()
            .await
            .map_err(|error| warn!(error=%error, "Failed to do fixup after connect"));

        Ok(Response::new(NvmeConnectResponse {}))
    }
}
