use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use stor_port::transport_api::ResourceKind;

use crate::controller::io_engine::types::{
    CreateSnapRebuild, DestroySnapRebuild, ListSnapRebuild, ListSnapRebuildRsp, SnapshotRebuild,
};
use rpc::v1::snapshot_rebuild::{
    CreateSnapshotRebuildRequest, DestroySnapshotRebuildRequest, ListSnapshotRebuildRequest,
};
use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::SnapshotRebuildApi for super::RpcClient {
    async fn create_snap_rebuild(
        &self,
        request: &CreateSnapRebuild,
    ) -> Result<SnapshotRebuild, SvcError> {
        let response = self
            .snap_rebuild()
            .create_snapshot_rebuild(CreateSnapshotRebuildRequest {
                uuid: request.replica_uuid.to_string(),
                replica_uuid: request.replica_uuid.to_string(),
                snapshot_uuid: request.replica_uuid.to_string(),
                snapshot_uri: request.source_uri.to_string(),
                replica_uri: "".to_string(),
                resume: false,
                bitmap: None,
                use_bitmap: false,
                error_policy: None,
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::ReplicaSnapshot,
                request: "create_snapshot_rebuild",
            })?;
        Ok(response.into_inner().try_into()?)
    }

    async fn list_snap_rebuild(
        &self,
        request: &ListSnapRebuild,
    ) -> Result<ListSnapRebuildRsp, SvcError> {
        let response = self
            .snap_rebuild()
            .list_snapshot_rebuild(ListSnapshotRebuildRequest {
                uuid: None,
                replica_uuid: request.uuid.as_ref().map(ToString::to_string),
                snapshot_uuid: None,
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::ReplicaSnapshot,
                request: "list_snapshot_rebuild",
            })?;
        let rebuilds = response.into_inner().rebuilds;
        let rebuilds = rebuilds
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(rebuilds)
    }

    async fn destroy_snap_rebuild(&self, request: &DestroySnapRebuild) -> Result<(), SvcError> {
        let _ = self
            .snap_rebuild()
            .destroy_snapshot_rebuild(DestroySnapshotRebuildRequest {
                uuid: request.uuid.to_string(),
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::ReplicaSnapshot,
                request: "destroy_snapshot_rebuild",
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        controller::io_engine::{
            types::{CreateSnapRebuild, ListSnapRebuild, SnapshotRebuildStatus},
            GrpcClient, GrpcContext,
        },
        node::service::NodeCommsTimeout,
    };
    use deployer_cluster::ClusterBuilder;
    use std::{sync::Arc, time::Duration};
    use stor_port::types::v0::{
        openapi::models,
        transport::{ApiVersion, ReplicaId},
    };

    const VOLUME_UUID: &str = "ec4e66fd-3b33-4439-b504-d49aba53da26";
    const VOLUME_SIZE: u64 = 10u64 * 1024 * 1024;

    #[tokio::test]
    async fn snapshot_rebuild() {
        let cluster = ClusterBuilder::builder()
            .with_rest(true)
            .with_io_engines(1)
            .with_pools(1)
            .with_cache_period("100ms")
            .build()
            .await
            .expect("Failed to build cluster");

        let api_client = cluster.rest_v00();
        let volumes_api = api_client.volumes_api();

        let _volume = volumes_api
            .put_volume(
                &VOLUME_UUID.parse().unwrap(),
                models::CreateVolumeBody::new(
                    models::VolumePolicy::new(true),
                    1,
                    VOLUME_SIZE,
                    true,
                ),
            )
            .await
            .expect("Failed to create volume");
        let replica_api = api_client.replicas_api();
        let replicas = replica_api.get_replicas().await.unwrap();
        let replica = replicas.first().unwrap();
        let replica_uuid: ReplicaId = replica.uuid.try_into().unwrap();
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let one_s = std::time::Duration::from_secs(1);
        let ctx = GrpcContext::new(
            lock,
            &cluster.node(0),
            cluster.node_socket(0),
            &NodeCommsTimeout::new(one_s, one_s, false),
            None,
            ApiVersion::V1,
        )
        .unwrap();
        let io_engine = GrpcClient::new(&ctx).await.unwrap();

        let mut rebuild = io_engine
            .create_snap_rebuild(&CreateSnapRebuild {
                replica_uuid: replica_uuid.clone(),
                source_uri: "malloc:///d?size_mb=30".to_string(),
            })
            .await
            .unwrap();
        tracing::info!("Rebuild: {rebuild:?}");

        let rebuilds = io_engine
            .list_snap_rebuild(&ListSnapRebuild { uuid: None })
            .await
            .unwrap();
        assert_eq!(rebuilds.len(), 1);
        assert!(rebuild.start_timestamp.is_some());

        let now = std::time::Instant::now();
        while now.elapsed() < Duration::from_secs(2)
            && matches!(
                rebuild.status,
                SnapshotRebuildStatus::Created | SnapshotRebuildStatus::Running
            )
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut rebuilds = io_engine
                .list_snap_rebuild(&ListSnapRebuild {
                    uuid: Some(replica_uuid.clone()),
                })
                .await
                .unwrap();
            rebuild = rebuilds.pop().unwrap();
        }
        tracing::info!("Rebuild: {rebuild:?}");
        assert_eq!(rebuild.status, SnapshotRebuildStatus::Successful);
        assert!(rebuild.end_timestamp.is_some());
        assert_eq!(rebuild.persisted_checkpoint, 0);
        assert_eq!(rebuild.remaining, 0);
        assert_eq!(rebuild.rebuilt, rebuild.total);
    }
}
