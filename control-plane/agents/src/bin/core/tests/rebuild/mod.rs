//! This file contains tests for rebuild scenarios, specifically around
//! partial(log-based) rebuild, but not limited to that.
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::nexus::traits::NexusOperations;
use std::{collections::HashMap, thread::sleep, time::Duration};
use stor_port::types::v0::{
    openapi::{
        models,
        models::{
            ChildState, CreateVolumeBody, Pool, PoolStatus, PublishVolumeBody, Replica,
            ReplicaState, VolumePolicy, VolumeStatus,
        },
    },
    transport::{Filter, GetRebuildRecord, NexusId, NexusStatus, RebuildHistory, VolumeId},
};
// Wait time for the nexus to be online from Degraded state.
const REBUILD_WAIT_TIME: u64 = 12;
const CHILD_WAIT: u64 = 5;

async fn build_cluster(num_ioe: u32, pool_size: u64) -> Cluster {
    let reconcile_period = Duration::from_millis(300);
    let child_twait = Duration::from_secs(CHILD_WAIT);
    ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(num_ioe)
        .with_faulted_child_wait_period(child_twait)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_cache_period("250ms")
        .with_tmpfs_pool(pool_size)
        .with_options(|b| b.with_isolated_io_engine(true))
        .build()
        .await
        .unwrap()
}

// This test:
// Creates a 3 replica volume and publishes it,
// validates that rebuild record is empty for the published nexus,
// stops io engine hosting one of the child,
// polls for rebuild history and validates there is 1 record for the nexus,
// validates that rebuild type is Full rebuild,
// validates that faulted child is removed from nexus,
// gets and validates rebuild history response from rest api.
#[tokio::test]
async fn rebuild_history_for_full_rebuild() {
    let cluster = build_cluster(4, 52428800).await;

    let vol_target = cluster.node(0).to_string();
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();
    let nexus_client = cluster.grpc_client().nexus();

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
        .expect("Failed to publish volume");

    let volume_state = volume.state.clone();
    let nexus = volume_state.target.unwrap();
    let nexus_id = NexusId::from(nexus.uuid);
    sleep(Duration::from_millis(500));
    // Before triggering rebuild, we expect rebuild history record to be available for the nexus
    // without any history of rebuild.
    let history = nexus_client
        .get_rebuild_history(&GetRebuildRecord::new(nexus_id.clone()), None)
        .await
        .expect("Failed to get rebuild record");
    assert_eq!(
        nexus_id, history.uuid,
        "Cant match nexus id in rebuild history"
    );
    assert_eq!(
        0,
        history.records.len(),
        "Number of rebuild record should be 0"
    );

    let volume_state = volume.state.clone();
    assert_eq!(volume_state.status, VolumeStatus::Online);
    let nexus = volume_state.target.unwrap();
    assert_eq!(nexus.children.len(), 3);
    nexus
        .children
        .iter()
        .for_each(|c| assert_eq!(c.state, ChildState::Online));

    // Check where the replicas are, apart from vol target node.
    let replicas = api_client.replicas_api().get_replicas().await.unwrap();
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
    let nexus_client = cluster.grpc_client().nexus();
    let history = poll_rebuild_history(&nexus_client, nexus_id.clone())
        .await
        .expect("Failed to get rebuild record");
    assert_eq!(
        nexus_id, history.uuid,
        "Cant match nexus id in rebuild history"
    );
    assert_eq!(
        1,
        history.records.len(),
        "Number of rebuild history is not equal to 1"
    );

    assert!(
        !history.records.get(0).unwrap().is_partial,
        "Rebuild type is not Full rebuild"
    );
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
    let history = api_client
        .volumes_api()
        .get_rebuild_history(&vol.spec.uuid)
        .await
        .expect("could not find rebuild history in rest");
    assert_eq!(
        nexus_id.to_string(),
        history.target_uuid.to_string(),
        "Cant match nexus id in rebuild history in rest"
    );
    assert_eq!(
        1,
        history.records.len(),
        "Number of rebuild history is not equal to 1 in rest"
    );

    assert!(
        !history.records.get(0).unwrap().is_partial,
        "Rebuild type is not Full rebuild in rest"
    );
}

// This test:
// Creates a 3 replica volume and publishes it,
// validates that rebuild record is empty for the published nexus,
// restarts io engine hosting one of the child,
// polls for rebuild history and validates there is 1 record for the nexus,
// validates that rebuild type is Partial rebuild,
// validates that previously faulted child is not removed from nexus,
// gets and validates rebuild history response from rest api.

#[tokio::test]
async fn rebuild_history_for_partial_rebuild() {
    let cluster = build_cluster(4, 52428800).await;

    let vol_target = cluster.node(0).to_string();
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();
    let nexus_client = cluster.grpc_client().nexus();
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

    let volume_state = volume.state.clone();
    assert!(volume_state.status == VolumeStatus::Online);
    let nexus = volume_state.target.unwrap();
    let nexus_id = NexusId::from(nexus.uuid);
    sleep(Duration::from_millis(500));
    // Before triggering rebuild, we expect rebuild history record to be available for the nexus
    // without any history of rebuild.
    let history = nexus_client
        .get_rebuild_history(&GetRebuildRecord::new(nexus_id.clone()), None)
        .await
        .expect("Failed to get rebuild record");
    assert_eq!(
        nexus_id, history.uuid,
        "Can't match nexus id in rebuild history"
    );
    assert_eq!(
        0,
        history.records.len(),
        "Number of rebuild record should be 0"
    );
    assert_eq!(nexus.children.len(), 3);
    nexus
        .children
        .iter()
        .for_each(|c| assert!(c.state == ChildState::Online));

    // Check where the replicas are, apart from vol target node.
    let replicas = api_client.replicas_api().get_replicas().await.unwrap();
    let testrep = replicas.iter().find(|r| r.node != vol_target).unwrap();
    let testrep_node = &testrep.node;
    tracing::info!(
        "Restarting node {testrep_node} having replica {}",
        testrep.uri
    );

    // Restart the container.
    cluster
        .composer()
        .restart(testrep_node)
        .await
        .expect("container stop failure");

    // Check the pool on restarted node is imported back.
    wait_pool_online(&cluster, testrep, Duration::from_secs(6))
        .await
        .expect("Pool didn't get into online state");

    // Fetch replicas again.
    let replicas_now = api_client.replicas_api().get_replicas().await.unwrap();
    // The replica picked for test must be online now again.
    assert!(replicas_now.iter().any(|r| r.uri == testrep.uri));
    if let Some(rep) = replicas_now.iter().find(|r| r.uri == testrep.uri) {
        assert_eq!(rep.state, ReplicaState::Online);
    }

    // Get the volume again for validations.
    let vol = volume_api.get_volume(&volid).await.unwrap();
    let volume_state = vol.state;
    let nexus = volume_state.target.unwrap();
    // The child must be still in the nexus.
    let is_removed = !nexus.children.iter().any(|c| c.uri == testrep.uri);
    assert!(!is_removed);
    let nexus_id = NexusId::from(volume.state.target.unwrap().uuid);

    wait_nexus_online(&nexus_client, nexus_id.clone())
        .await
        .expect("Rebuild didnt finish in 30 secs");

    let history = nexus_client
        .get_rebuild_history(&GetRebuildRecord::new(nexus_id.clone()), None)
        .await
        .expect("Failed to get rebuild record");

    assert_eq!(
        nexus_id, history.uuid,
        "Cant match nexus id in rebuild history"
    );

    assert_eq!(
        1,
        history.records.len(),
        "Number of rebuild history is not equal to 1"
    );

    assert!(
        history.records.get(0).unwrap().is_partial,
        "Rebuild type is not Partial rebuild"
    );
    let history = api_client
        .volumes_api()
        .get_rebuild_history(&vol.spec.uuid)
        .await
        .expect("could not find rebuild history in rest");
    assert_eq!(
        nexus_id.to_string(),
        history.target_uuid.to_string(),
        "Cant match nexus id in rebuild history in rest"
    );
    assert_eq!(
        1,
        history.records.len(),
        "Number of rebuild history is not equal to 1 in rest"
    );

    assert!(
        history.records.get(0).unwrap().is_partial,
        "Rebuild type is not Partial rebuild in rest"
    );

    // Restart the container.
    cluster
        .composer()
        .restart(testrep_node)
        .await
        .expect("container stop failure");

    wait_pool_online(&cluster, testrep, Duration::from_secs(6))
        .await
        .expect("Pool didn't get into online state");
    wait_nexus_online(&nexus_client, nexus_id.clone())
        .await
        .expect("Rebuild didnt finish in 30 secs");

    let history = api_client
        .volumes_api()
        .get_rebuild_history(&vol.spec.uuid)
        .await
        .expect("could not find rebuild history in rest");
    assert_eq!(
        nexus_id.to_string(),
        history.target_uuid.to_string(),
        "Can't match nexus id in rebuild history in rest"
    );
    assert_eq!(2, history.records.len());
}

/// Checks if node is online, returns true if yes.
async fn wait_nexus_online(nexus_client: &impl NexusOperations, nexus: NexusId) -> Result<(), ()> {
    let timeout = Duration::from_secs(REBUILD_WAIT_TIME);
    let start = std::time::Instant::now();
    loop {
        let nexus = nexus_client
            .get(Filter::Nexus(nexus.clone()), None)
            .await
            .expect("Cant get Nexus object");
        if let Some(nexus) = nexus.0.get(0) {
            if nexus.status == NexusStatus::Online {
                return Ok(());
            }
        }
        if std::time::Instant::now() > (start + timeout) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(())
}

/// Checks if nexus has rebuild record for predefined interval.
async fn poll_rebuild_history(
    nexus_client: &impl NexusOperations,
    nexus_id: NexusId,
) -> Result<RebuildHistory, ()> {
    let timeout = Duration::from_secs(REBUILD_WAIT_TIME);
    let start = std::time::Instant::now();
    loop {
        if let Ok(record) = nexus_client
            .get_rebuild_history(&GetRebuildRecord::new(nexus_id.clone()), None)
            .await
        {
            if !record.records.is_empty() {
                return Ok(record);
            }
            if std::time::Instant::now() > (start + timeout) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    Err(())
}

/// Checks if pool is online. Returns pool if yes, None if not.
async fn wait_pool_online(cluster: &Cluster, replica: &Replica, timeout: Duration) -> Option<Pool> {
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
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
