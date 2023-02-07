use crate::controller::io_engine::translation::IoEngineToAgent;
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use common_lib::{
    transport_api::{v0::BlockDevices, ResourceKind},
    types::v0::transport::{GetBlockDevices, Register},
};
use grpc::operations::registration;
use rpc::v1::host::ListBlockDevicesRequest;

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

        let registration_info = match data.into_inner().registration_info {
            Some(info) => info,
            None => {
                // The dataplane did not send anything in registration info, which should
                // not happen.
                return Err(SvcError::NodeNotOnline {
                    node: self.context.node().clone(),
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
                            rpc::v1::registration::ApiVersion::from_i32(v).unwrap_or_default(),
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
