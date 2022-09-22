#![cfg(test)]

use crate::volume::helpers::wait_till_volume_children;
use common_lib::types::v0::{openapi::models, transport::ReplicaId};
use deployer_cluster::{ClusterBuilder, FindVolumeRequest};
use std::{convert::TryFrom, time::Duration};

#[tokio::test]
async fn fault_enospc_child() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(2)
        .with_pools(1)
        .with_csi(false, true)
        .with_options(|o| o.with_isolated_io_engine(true))
        .with_cache_period("250ms")
        .with_reconcile_period(Duration::from_millis(250), Duration::from_millis(250))
        .build()
        .await
        .unwrap();

    let api_client = cluster.rest_v00();
    let volumes_api = api_client.volumes_api();
    let pools_api = api_client.pools_api();
    let replica_api = api_client.replicas_api();

    let volume_1_size = 10u64 * 1024 * 1024;
    let mut volume_1 = volumes_api
        .put_volume(
            &"ec4e66fd-3b33-4439-b504-d49aba53da26".parse().unwrap(),
            models::CreateVolumeBody::new(models::VolumePolicy::new(true), 2, volume_1_size, true),
        )
        .await
        .unwrap();
    volume_1 = volumes_api
        .put_volume_target(
            &volume_1.spec.uuid,
            models::VolumeShareProtocol::Nvmf,
            Some(cluster.node(0).as_str()),
            Some(false),
        )
        .await
        .unwrap();
    let uri = volume_1.state.target.as_ref().unwrap().device_uri.as_str();
    let _drop_target = DeviceDisconnect(nvmeadm::NvmeTarget::try_from(uri).unwrap());

    replica_api
        .put_pool_replica(
            cluster.pool(0, 0).as_str(),
            &ReplicaId::new(),
            models::CreateReplicaBody {
                share: None,
                size: 85u64 * 1024 * 1024,
                thin: false,
            },
        )
        .await
        .unwrap();
    let pool = pools_api
        .get_pool(cluster.pool(0, 0).as_str())
        .await
        .unwrap();
    let pool = pool.state.unwrap();
    tracing::info!(?pool, "Here's the pool");

    let replicas = replica_api.get_replicas().await.unwrap();
    tracing::info!(?replicas, "Here's the replicas");

    let mut node = cluster.csi_node_client(0).await.unwrap();
    node.node_stage_volume(&volume_1).await.unwrap();
    let response = node
        .internal()
        .find_volume(FindVolumeRequest {
            volume_id: volume_1.spec.uuid.to_string(),
        })
        .await
        .unwrap();
    tracing::info!(?response);

    let device_path = response.into_inner().device_path;
    let output = cluster
        .composer()
        .exec(
            cluster.csi_container(0).as_str(),
            vec!["dd", "if=/dev/zero", format!("of={}", device_path).as_str()],
        )
        .await;

    cluster
        .composer()
        .exec(
            cluster.csi_container(0).as_str(),
            vec!["nvme", "disconnect-all"],
        )
        .await
        .unwrap();

    tracing::info!("\n{}", output.unwrap());

    let volume_client = cluster.grpc_client().volume();
    let _ = wait_till_volume_children(&volume_1.spec.uuid.into(), 1, &volume_client).await;
}

struct DeviceDisconnect(nvmeadm::NvmeTarget);
impl Drop for DeviceDisconnect {
    fn drop(&mut self) {
        if self.0.disconnect().is_err() {
            std::process::Command::new("sudo")
                .args(&["nvme", "disconnect-all"])
                .status()
                .unwrap();
        }
    }
}
