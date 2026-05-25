#![cfg(test)]

use super::helpers::wait_till_volume_status;
use deployer_cluster::ClusterBuilder;
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::{
    openapi::models::{self, VolumePolicy, VolumeStatus},
    transport::VolumeId,
};
use uuid::Uuid;

const RECONCILE_PERIOD_MS: u64 = 250;
const GRACE_PERIOD_SECS: u64 = 2;
const REBUILD_TIMEOUT_SECS: u64 = 30;

/// Happy path: create a 2-replica volume, publish to establish health_info,
/// unpublish, stop one io-engine node → volume becomes Degraded.
/// With offline rebuild enabled, the reconciler creates a temp nexus,
/// HotSpare rebuilds the replica, then the reconciler tears down the nexus.
/// Final state: volume Online with no target (unpublished).
#[tokio::test]
async fn offline_rebuild_happy_path() {
    let reconcile = Duration::from_millis(RECONCILE_PERIOD_MS);
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(3)
        .with_tmpfs_pool(52428800)
        .with_cache_period("250ms")
        .with_reconcile_period(reconcile, reconcile)
        .with_options(|o| {
            o.with_isolated_io_engine(true)
                .with_agents_env("OFFLINE_REBUILD_ENABLED", "true")
                .with_agents_env(
                    "OFFLINE_REBUILD_GRACE_PERIOD",
                    &format!("{GRACE_PERIOD_SECS}s"),
                )
        })
        .build()
        .await
        .unwrap();

    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();

    let volid = VolumeId::new();
    let body = models::CreateVolumeBody::new(VolumePolicy::new(true), 2, 10485760u64, false, false);
    let volume = volume_api.put_volume(&volid, body).await.unwrap();
    let uid = volume.spec.uuid;

    // Also provision a second volume that is never published. With no
    // health_info_id the reconciler must never stand up a target for it, even
    // while the first volume is actively being rebuilt.
    let never_pub_id = VolumeId::new();
    let never_pub_body =
        models::CreateVolumeBody::new(VolumePolicy::new(true), 2, 10485760u64, false, false);
    let never_pub = volume_api
        .put_volume(&never_pub_id, never_pub_body)
        .await
        .unwrap();
    let never_pub_uid = never_pub.spec.uuid;

    // Publish to establish health_info_id (nexus info persisted to etcd).
    let volume = volume_api
        .put_volume_target(
            &uid,
            models::PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                cluster.node(0).to_string(),
                models::VolumeShareProtocol::Nvmf,
                None,
                cluster.csi_node(0),
                None,
            ),
        )
        .await
        .expect("Should publish volume");

    assert_eq!(volume.state.status, VolumeStatus::Online);

    // Unpublish — volume returns to no-target state.
    volume_api
        .del_volume_target(&uid, None, None)
        .await
        .expect("Should unpublish volume");

    let volume = volume_api.get_volume(&uid).await.unwrap();
    assert!(
        volume.state.target.is_none(),
        "Volume should have no target after unpublish"
    );

    // Stop a node hosting a replica — makes volume Degraded.
    let replicas = api_client.replicas_api().get_replicas().await.unwrap();
    let victim_replica = replicas
        .iter()
        .find(|r| r.node != cluster.node(0).to_string())
        .expect("Should have a replica on a non-target node");
    let victim_node = victim_replica.node.clone();

    cluster
        .composer()
        .stop(&victim_node)
        .await
        .expect("Should stop io-engine node");

    // Wait for Degraded status.
    wait_till_volume_status(
        &cluster,
        &uid,
        VolumeStatus::Degraded,
        Duration::from_secs(10),
    )
    .await
    .expect("Volume should become Degraded");

    // Now wait for grace period + rebuild time. The reconciler should:
    // 1. Wait for grace period (GRACE_PERIOD_SECS)
    // 2. Create temporary unshared nexus
    // 3. HotSpare rebuilds replica onto remaining healthy node's pool
    // 4. Volume goes Online
    // 5. Reconciler tears down temporary nexus
    //
    // We wait for the volume to return to Online with no target.
    let timeout = Duration::from_secs(GRACE_PERIOD_SECS + REBUILD_TIMEOUT_SECS);
    wait_till_volume_online_no_target(&cluster, &uid, timeout)
        .await
        .expect("Volume should be Online with no target after offline rebuild");

    // The never-published volume must still have no target — the reconciler
    // skips it for lack of health_info_id.
    let never_pub = volume_api.get_volume(&never_pub_uid).await.unwrap();
    assert!(
        never_pub.state.target.is_none(),
        "Never-published volume must not be touched by the offline rebuild reconciler; got {:?}",
        never_pub.state.target
    );
}

/// Wait for volume to reach Online status with no target (nexus torn down).
async fn wait_till_volume_online_no_target(
    cluster: &deployer_cluster::Cluster,
    volume: &Uuid,
    timeout: Duration,
) -> Result<(), String> {
    let start = std::time::Instant::now();
    loop {
        let vol = cluster.rest_v00().volumes_api().get_volume(volume).await;

        if let Ok(vol) = vol {
            if vol.state.status == VolumeStatus::Online && vol.state.target.is_none() {
                return Ok(());
            }
        }

        if std::time::Instant::now() > (start + timeout) {
            let vol = cluster.rest_v00().volumes_api().get_volume(volume).await;
            return Err(format!(
                "Timeout waiting for volume to be Online with no target. Current: {vol:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
