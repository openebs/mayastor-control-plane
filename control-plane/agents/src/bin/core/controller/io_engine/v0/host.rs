use common_lib::{
    transport_api::{v0::BlockDevices, ResourceKind},
    types::v0::transport::{GetBlockDevices, Register},
};

use snafu::ResultExt;

use crate::controller::io_engine::V0ListBlockDevicesRequest;
use agents::{
    errors::{GrpcRequest as GrpcRequestError, SvcError},
    msg_translation::IoEngineToAgent,
};
use common_lib::types::v0::transport::ApiVersion;

#[async_trait::async_trait]
impl crate::controller::io_engine::HostApi for super::RpcClient {
    async fn liveness_probe(&self) -> Result<Register, SvcError> {
        self.client()
            .get_mayastor_info(rpc::io_engine::Null {})
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Node,
                request: "v0::get_mayastor_info",
            })?;

        // V0 GetMayastorInfo Liveness call doesn't return the registration info,
        // thus fill it from context and hard-code the version as V0
        Ok(Register {
            id: self.context.node.clone(),
            grpc_endpoint: self.context.endpoint(),
            api_versions: Some(vec![ApiVersion::V0]),
            instance_uuid: None,
            node_nqn: None,
        })
    }

    async fn list_blockdevices(&self, request: &GetBlockDevices) -> Result<BlockDevices, SvcError> {
        let result = self
            .client()
            .list_block_devices(V0ListBlockDevicesRequest { all: request.all })
            .await;

        let response = result
            .context(GrpcRequestError {
                resource: ResourceKind::Block,
                request: "list_block_devices",
            })?
            .into_inner();

        let bdevs = response
            .devices
            .iter()
            .map(|rpc_bdev| rpc_bdev.to_agent())
            .collect();
        Ok(BlockDevices(bdevs))
    }
}
