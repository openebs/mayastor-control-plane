//! This file contains tests for rebuild scenarios, specifically around
//! partial(log-based) rebuild, but not limited to that.

use deployer_cluster::{Cluster, ClusterBuilder};

use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::{
    openapi::{
        models,
        models::{
            ChildState, CreateVolumeBody, Pool, PoolStatus, PublishVolumeBody, Replica,
            ReplicaState, VolumePolicy, VolumeStatus,
        },
    },
    transport::VolumeId,
};

async fn build_cluster(num_ioe: u32, pool_size: u64, _num_pools: u32) -> Cluster {
    let reconcile_period = Duration::from_secs(1);
    let child_twait = Duration::from_secs(15);
    ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(num_ioe)
        .with_faulted_child_wait_period(child_twait)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_cache_period("1s")
        .with_tmpfs_pool(pool_size)
        .build()
        .await
        .unwrap()
}

async fn wait_till_pool_online(
    cluster: &Cluster,
    replica: &Replica,
    timeout: Duration,
) -> Option<Pool> {
    let client = cluster.rest_v00();
    let pool_api = client.pools_api();
    let start = std::time::Instant::now();

    loop {
        let pool = pool_api.get_pool(replica.pool.as_str()).await.unwrap();
        tracing::info!("Waiting for pool to be Online - {:?}", pool);
        if pool.state.is_some() && (pool.state.clone().unwrap().status == PoolStatus::Online) {
            tracing::info!("Pool is Online now");
            return Some(pool);
        }

        if std::time::Instant::now() > (start + timeout)
            && (pool.state.is_none() || pool.state.as_ref().unwrap().status != PoolStatus::Online)
        {
            tracing::info!("Timeout waiting for pool to be Online - {:?}", pool);
            return None;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// This test
// 1. creates a volume with 3 replicas, faults one of them by restarting container.
// 2. Expects child's replica to be online again within 15sec period.
// 3. Attempts to online the child.
// 4. Child should get successfully onlined and partial rebuild happen.
// 5. If online call fails, full rebuild of child happens.
#[tokio::test]
async fn replica_wait_and_partial_rebuild() {
    let cluster = build_cluster(4, 52428800, 0).await; // 50MiB pool size

    let vol_target = cluster.node(0).to_string();
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();

    let body = CreateVolumeBody::new(VolumePolicy::new(true), 3, 10485760u64, false);
    let volid = VolumeId::new();
    let volume = volume_api.put_volume(&volid, body).await.unwrap();
    let volume = volume_api
        .put_volume_target(
            &volume.spec.uuid,
            PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                vol_target.clone().to_string(),
                models::VolumeShareProtocol::Nvmf,
                None,
                cluster.csi_node(0),
            ),
        )
        .await
        .unwrap();

    let volume_state = volume.state;
    assert!(volume_state.status == VolumeStatus::Online);
    let nexus = volume_state.target.unwrap();
    assert_eq!(nexus.children.len(), 3);
    nexus
        .children
        .iter()
        .for_each(|c| assert!(c.state == ChildState::Online));

    // Check where the replicas are, apart from vol target node.
    let replicas = api_client.replicas_api().get_replicas().await.unwrap();
    tracing::info!({?replicas}, "Available replicas - ");
    let testrep = replicas.iter().find(|r| r.node != vol_target).unwrap();
    let testrep_node = &testrep.node;
    tracing::info!(
        "Restarting node {testrep_node} having replica {}",
        testrep.uri.clone()
    );

    // Restart the container.
    cluster
        .composer()
        .restart(&testrep_node.to_string())
        .await
        .expect("container stop failure");

    // Check the pool on restarted node is imported back.
    wait_till_pool_online(&cluster, testrep, Duration::from_secs(20)).await;

    // Fetch replicas again.
    let replicas_now = api_client.replicas_api().get_replicas().await.unwrap();
    // The replica picked for test must be online now again.
    assert!(replicas_now.iter().any(|r| r.uri == testrep.uri));
    if let Some(rep) = replicas_now.iter().find(|r| r.uri == testrep.uri) {
        assert!(rep.state == ReplicaState::Online);
    }

    // Get the volume again for validations.
    let vol = volume_api.get_volume(volid.uuid()).await.unwrap();
    let volume_state = vol.state;
    let nexus = volume_state.target.unwrap();
    // The child must be still in the nexus.
    let is_removed = !nexus.children.iter().any(|c| c.uri == testrep.uri);
    assert!(!is_removed);
    // TODO: We can additionally check that the volume state eventually becomes Online,
    // and doesn't remain Degraded once the partial rebuild finishes. Currently no support for
    // getting rebuild info via nexus grpc client NexusOperations.
}

// This test
// 1. creates a volume with 3 replicas, faults one of them by stopping container.
// 2. Expects child's replica to stay offline for faulted child wait period.
// 3. Since replica doesn't come online within wait period, child gets removed from nexus.
// 4. Validate child should not be present in nexus.
#[tokio::test]
async fn replica_wait_and_full_rebuild() {
    let cluster = build_cluster(4, 52428800, 0).await; // 50MiB pool size

    let vol_target = cluster.node(0).to_string();
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();

    let body = CreateVolumeBody::new(VolumePolicy::new(true), 3, 10485760u64, false);
    let volid = VolumeId::new();
    let volume = volume_api.put_volume(&volid, body).await.unwrap();
    let volume = volume_api
        .put_volume_target(
            &volume.spec.uuid,
            PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                vol_target.clone().to_string(),
                models::VolumeShareProtocol::Nvmf,
                None,
                cluster.csi_node(0),
            ),
        )
        .await
        .unwrap();

    let volume_state = volume.state;
    assert!(volume_state.status == VolumeStatus::Online);
    let nexus = volume_state.target.unwrap();
    assert_eq!(nexus.children.len(), 3);
    nexus
        .children
        .iter()
        .for_each(|c| assert!(c.state == ChildState::Online));

    // Check where the replicas are, apart from vol target node.
    let replicas = api_client.replicas_api().get_replicas().await.unwrap();
    tracing::info!({?replicas}, "Available replicas - ");
    let testrep = replicas.iter().find(|r| r.node != vol_target).unwrap();
    let testrep_node = &testrep.node;
    tracing::info!(
        "Stopping node {testrep_node} having replica {}",
        testrep.uri.clone()
    );

    // Stop the container and don't restart.
    cluster
        .composer()
        .stop(&testrep_node.to_string())
        .await
        .expect("container stop failure");

    // Check the pool on restarted node shouldn't be online.
    // This must expire the wait timeout.
    wait_till_pool_online(&cluster, testrep, Duration::from_secs(20)).await;

    // Fetch replicas again.
    let replicas_now = api_client.replicas_api().get_replicas().await.unwrap();
    // The replica picked for test must not be online now again.
    assert!(!replicas_now.iter().any(|r| r.uri == testrep.uri));

    // Get the volume again for validations.
    let vol = volume_api.get_volume(volid.uuid()).await.unwrap();
    let volume_state = vol.state;
    let nexus = volume_state.target.unwrap();
    // The child must not be in the nexus anymore.
    let is_removed = !nexus.children.iter().any(|c| c.uri == testrep.uri);
    assert!(is_removed);
    // TODO: We can additionally check that the volume state eventually becomes Online,
    // and doesn't remain degraded once the full rebuild finishes. Currently no support for
    // getting rebuild info via nexus grpc client NexusOperations.
}
