use crate::controller::io_engine::translation::IoEngineToAgent;
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use grpc::operations::registration;
use rpc::v1::host::ListBlockDevicesRequest;
use stor_port::{
    transport_api::{v0::BlockDevices, ResourceKind},
    types::v0::transport::{GetBlockDevices, Register},
};

use snafu::ResultExt;
use std::str::FromStr;

#[async_trait::async_trait]
impl crate::controller::io_engine::HostApi for super::RpcClient {
    async fn liveness_probe(&self) -> Result<Register, SvcError> {
        // V1 liveness sends registration_info, which can be used to get the
        // actual state of dataplane
        let data = self
            .host()
            .get_mayastor_info(())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Node,
                request: "v1::get_mayastor_info",
            })?;

        let inner = data.into_inner();

        let registration_info = match inner.registration_info {
            Some(info) => info,
            None => {
                // For versions which support v1 but registration info is not available,
                // we should not enable v1 thus fail the call with unimplemented error.
                return Err(SvcError::Unimplemented {
                    resource: ResourceKind::Node,
                    request: "v1::get_mayastor_info".to_string(),
                    source: tonic::Status::unimplemented(format!(
                        "v1::get_mayastor_info is unimplemented for {}",
                        inner.version
                    )),
                });
            }
        };

        Ok(Register {
            id: registration_info.id.into(),
            grpc_endpoint: std::net::SocketAddr::from_str(&registration_info.grpc_endpoint)
                .map_err(|error| SvcError::NodeGrpcEndpoint {
                    node: self.context.node().clone(),
                    socket: registration_info.grpc_endpoint,
                    error,
                })?,
            api_versions: Some(
                registration_info
                    .api_version
                    .into_iter()
                    .map(|v| {
                        // Get the list of supported api versions from dataplane
                        registration::traits::ApiVersion(
                            rpc::v1::registration::ApiVersion::try_from(v).unwrap_or_default(),
                        )
                        .into()
                    })
                    .collect(),
            ),
            instance_uuid: registration_info
                .instance_uuid
                .and_then(|u| uuid::Uuid::parse_str(&u).ok()),
            node_nqn: registration_info.hostnqn.and_then(|h| h.try_into().ok()),
        })
    }

    async fn list_blockdevices(&self, request: &GetBlockDevices) -> Result<BlockDevices, SvcError> {
        let result = self
            .host()
            .list_block_devices(ListBlockDevicesRequest { all: request.all })
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
