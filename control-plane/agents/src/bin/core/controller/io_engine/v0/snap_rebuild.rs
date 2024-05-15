use agents::errors::SvcError;
use stor_port::transport_api::ResourceKind;

use crate::controller::io_engine::types::{
    CreateSnapRebuild, DestroySnapRebuild, ListSnapRebuild, ListSnapRebuildRsp, SnapshotRebuild,
};

#[async_trait::async_trait]
impl crate::controller::io_engine::SnapshotRebuildApi for super::RpcClient {
    async fn create_snap_rebuild(
        &self,
        _request: &CreateSnapRebuild,
    ) -> Result<SnapshotRebuild, SvcError> {
        Err(SvcError::GrpcRequestError {
            resource: ResourceKind::ReplicaSnapshot,
            request: "create_snap_rebuild".to_string(),
            source: tonic::Status::unimplemented(""),
        })
    }

    async fn list_snap_rebuild(
        &self,
        _request: &ListSnapRebuild,
    ) -> Result<ListSnapRebuildRsp, SvcError> {
        Err(SvcError::GrpcRequestError {
            resource: ResourceKind::ReplicaSnapshot,
            request: "list_snap_rebuild".to_string(),
            source: tonic::Status::unimplemented(""),
        })
    }

    async fn destroy_snap_rebuild(&self, _request: &DestroySnapRebuild) -> Result<(), SvcError> {
        Err(SvcError::GrpcRequestError {
            resource: ResourceKind::ReplicaSnapshot,
            request: "destroy_snap_rebuild".to_string(),
            source: tonic::Status::unimplemented(""),
        })
    }
}
