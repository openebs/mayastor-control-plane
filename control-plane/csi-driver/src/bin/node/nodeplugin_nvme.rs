use crate::dev::Device;
use grpc::csi_node_nvme::{
    nvme_operations_server::NvmeOperations, NvmeConnectRequest, NvmeConnectResponse,
};
use std::collections::HashMap;
use tonic::Response;

#[derive(Debug, Default)]
pub(crate) struct NvmeOperationsSvc {}

#[tonic::async_trait]
impl NvmeOperations for NvmeOperationsSvc {
    async fn nvme_connect(
        &self,
        request: tonic::Request<NvmeConnectRequest>,
    ) -> Result<tonic::Response<NvmeConnectResponse>, tonic::Status> {
        let req = request.into_inner();

        let uri: &str = req.uri.as_str();
        let publish_context: HashMap<String, String> = req.publish_context;

        // Get the nvmf device object from the uri
        let mut device = Device::parse(uri)?;

        // Parse the parameters from publish context
        device.parse_parameters(&publish_context).await?;
        // Make nvme connection
        device.attach().await?;
        Ok(Response::new(NvmeConnectResponse {}))
    }
}
