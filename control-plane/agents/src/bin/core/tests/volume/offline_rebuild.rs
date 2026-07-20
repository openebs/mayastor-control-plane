#![cfg(test)]

use super::helpers::{wait_node_online, wait_till_volume_status};
use deployer_cluster::ClusterBuilder;
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::{
    openapi::models::{self, VolumePolicy, VolumeStatus},
    transport::{NodeId, VolumeId},
};
use uuid::Uuid;

const RECONCILE_PERIOD_MS: u64 = 250;
const GRACE_PERIOD_SECS: u64 = 2;
const REBUILD_TIMEOUT_SECS: u64 = 30;

/// End-to-end coverage for the offline-rebuild reconciler. Drives four
/// scenarios sequentially on a single shared cluster (each scenario takes a
/// node down and brings it back up before handing off), which avoids the
/// per-test cluster spin-up cost while still exercising each path on a clean
/// pool layout:
///
/// 1. **Happy path**: degraded unpublished volume gets a temp nexus, the
///    rebuild runs, the temp nexus is torn down, the volume returns to
///    Online with no target. A second volume that was never published must
///    not be touched.
/// 2. **Promote-on-publish**: while the temp nexus exists, a CSI publish
///    promotes the existing unshared nexus instead of creating a new one.
///    The rebuild keeps going on the now-shared nexus and the volume reaches
///    Online still published.
/// 3. **GC safety**: during an active offline rebuild, the GarbageCollector
///    must not strip ownership from the surviving healthy replicas of the
///    volume being rebuilt.
/// 4. **No pool candidate defers rebuild**: when the volume is degraded but
///    no candidate pool can host the replacement replica, the reconciler
///    must not stand up a temp nexus. Returning the victim node brings the
///    volume back to Online without a rebuild ever firing.
#[tokio::test]
async fn offline_rebuild_e2e() {
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

    happy_path(&cluster).await;
    promote_on_publish(&cluster).await;
    gc_safety(&cluster).await;
    no_pool_candidate_defers_rebuild(&cluster).await;
}

/// Happy path scenario, see [`offline_rebuild_e2e`] doc.
async fn happy_path(cluster: &deployer_cluster::Cluster) {
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

    volume_api
        .del_volume_target(&uid, None, None)
        .await
        .expect("Should unpublish volume");

    let volume = volume_api.get_volume(&uid).await.unwrap();
    assert!(
        volume.state.target.is_none(),
        "Volume should have no target after unpublish"
    );

    // Stop a node hosting a replica, this makes the volume Degraded.
    let victim_node = pick_victim_node(&api_client, &cluster.node(0).to_string(), uid).await;
    cluster
        .composer()
        .stop(&victim_node)
        .await
        .expect("Should stop io-engine node");

    wait_till_volume_status(
        cluster,
        &uid,
        VolumeStatus::Degraded,
        Duration::from_secs(10),
    )
    .await
    .expect("Volume should become Degraded");

    // Wait for grace period + rebuild. Final state: Online, no target.
    let timeout = Duration::from_secs(GRACE_PERIOD_SECS + REBUILD_TIMEOUT_SECS);
    wait_till_volume_online_no_target(cluster, &uid, timeout)
        .await
        .expect("Volume should be Online with no target after offline rebuild");

    // The never-published volume must still have no target.
    let never_pub = volume_api.get_volume(&never_pub_uid).await.unwrap();
    assert!(
        never_pub.state.target.is_none(),
        "Never-published volume must not be touched by the offline rebuild reconciler; got {:?}",
        never_pub.state.target
    );

    // Cleanup so later scenarios start from a known volume set, and bring the
    // stopped node back so subsequent scenarios have the full 3-node pool.
    volume_api.del_volume(&uid).await.unwrap();
    volume_api.del_volume(&never_pub_uid).await.unwrap();
    restart_node(cluster, &victim_node).await;
}

/// Promote-on-publish scenario, see [`offline_rebuild_e2e`] doc.
async fn promote_on_publish(cluster: &deployer_cluster::Cluster) {
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();

    let volid = VolumeId::new();
    let body = models::CreateVolumeBody::new(VolumePolicy::new(true), 2, 10485760u64, false, false);
    let volume = volume_api.put_volume(&volid, body).await.unwrap();
    let uid = volume.spec.uuid;

    // Publish + unpublish to establish health_info.
    volume_api
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
        .unwrap();
    volume_api
        .del_volume_target(&uid, None, None)
        .await
        .unwrap();

    // Stop a replica node so the volume becomes Degraded.
    let victim_node = pick_victim_node(&api_client, &cluster.node(0).to_string(), uid).await;
    cluster.composer().stop(&victim_node).await.unwrap();

    // Wait for the offline-rebuild reconciler to create the unshared nexus.
    let unshared_target = wait_for_unshared_target(cluster, &uid, Duration::from_secs(15))
        .await
        .expect("Offline rebuild should create unshared target");
    let rebuild_node = unshared_target.node.clone();

    // CSI publish arrives mid-rebuild, should promote (share the existing nexus).
    let volume = volume_api
        .put_volume_target(
            &uid,
            models::PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                rebuild_node.clone(),
                models::VolumeShareProtocol::Nvmf,
                None,
                cluster.csi_node(0),
                None,
            ),
        )
        .await
        .expect("Publish should promote the offline-rebuild nexus");

    let target = volume.state.target.expect("target present after promote");
    assert_eq!(
        target.node, rebuild_node,
        "Promoted nexus should stay on same node"
    );
    assert!(
        !target.device_uri.is_empty(),
        "Promoted nexus should be shared (device_uri set)"
    );

    // Rebuild continues on the promoted (now shared) nexus, volume eventually Online.
    let volume = wait_for_volume_state(
        cluster,
        &uid,
        VolumeStatus::Online,
        Duration::from_secs(REBUILD_TIMEOUT_SECS),
    )
    .await
    .expect("Volume should reach Online after promoted rebuild");

    assert!(
        volume.state.target.is_some(),
        "Volume stays published after promotion"
    );

    volume_api
        .del_volume_target(&uid, None, None)
        .await
        .unwrap();
    volume_api.del_volume(&uid).await.unwrap();
    restart_node(cluster, &victim_node).await;
}

/// GC safety scenario, see [`offline_rebuild_e2e`] doc.
async fn gc_safety(cluster: &deployer_cluster::Cluster) {
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();

    let volid = VolumeId::new();
    let body = models::CreateVolumeBody::new(VolumePolicy::new(true), 2, 10485760u64, false, false);
    let volume = volume_api.put_volume(&volid, body).await.unwrap();
    let uid = volume.spec.uuid;

    volume_api
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
        .unwrap();
    volume_api
        .del_volume_target(&uid, None, None)
        .await
        .unwrap();

    // Snapshot the surviving replicas before triggering rebuild. Ownership
    // lives on the spec; runtime state has the node placement. We need both:
    // state to find a victim node, spec to identify the volume's own replicas
    // and verify ownership after the fact.
    let pre_state = api_client.replicas_api().get_replicas().await.unwrap();
    let pre_specs = api_client.specs_api().get_specs().await.unwrap();
    let owned_replica_uuids: Vec<uuid::Uuid> = pre_specs
        .replicas
        .iter()
        .filter(|r| r.owners.volume == Some(uid))
        .map(|r| r.uuid)
        .collect();
    let victim_node = pre_state
        .iter()
        .find(|r| owned_replica_uuids.contains(&r.uuid) && r.node != cluster.node(0).to_string())
        .expect("Should find a replica of the test volume on a non-target node")
        .node
        .clone();
    let live_replica_uuids: Vec<_> = owned_replica_uuids
        .iter()
        .filter(|u| {
            !pre_state
                .iter()
                .any(|r| r.uuid == **u && r.node == victim_node)
        })
        .copied()
        .collect();
    assert!(!live_replica_uuids.is_empty());

    cluster.composer().stop(&victim_node).await.unwrap();

    // Wait until offline rebuild creates the unshared nexus.
    wait_for_unshared_target(cluster, &uid, Duration::from_secs(15))
        .await
        .expect("Offline rebuild should start");

    // Sleep briefly to let GC poll at least once with the rebuild active.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The surviving replicas should still exist *and* still be owned by the
    // test volume. Without the ownership check, a disown-without-destroy would
    // slip past.
    let post_specs = api_client.specs_api().get_specs().await.unwrap();
    for live_uuid in &live_replica_uuids {
        let live = post_specs
            .replicas
            .iter()
            .find(|r| r.uuid == *live_uuid)
            .unwrap_or_else(|| {
                panic!("Live replica {live_uuid} was destroyed during offline rebuild")
            });
        assert_eq!(
            live.owners.volume,
            Some(uid),
            "Live replica {live_uuid} was disowned (owners.volume cleared) during offline rebuild"
        );
    }

    volume_api.del_volume(&uid).await.unwrap();
    restart_node(cluster, &victim_node).await;
}

/// No-pool-candidate scenario, see [`offline_rebuild_e2e`] doc.
///
/// Exercises the pre-flight viability check in `initiate_offline_rebuild`:
/// when no candidate pool exists for the replacement replica, the reconciler
/// must defer (no temp nexus created) rather than stand up a nexus that has
/// nowhere to rebuild to. A 3-replica volume on a 3-node cluster occupies all
/// pools, so a victim node going down leaves no spare pool that can host the
/// replacement.
///
/// The Faulted-teardown branch (source replica lost mid-rebuild while the
/// temp nexus host stays up) is not exercised here — auto-selected nexus
/// placement co-locates the nexus with the source on a 3-node cluster, so
/// killing the source also kills the host and the volume status falls to
/// Degraded/Unknown instead of Faulted. Coverage for that branch needs a
/// topology-controlled cluster and is left as a follow-up.
async fn no_pool_candidate_defers_rebuild(cluster: &deployer_cluster::Cluster) {
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();

    let volid = VolumeId::new();
    // 3 replicas on a 3-node cluster pins each replica to one pool, leaving
    // no spare pool for the offline-rebuild reconciler to place a
    // replacement on.
    let body = models::CreateVolumeBody::new(VolumePolicy::new(true), 3, 10485760u64, false, false);
    let volume = volume_api.put_volume(&volid, body).await.unwrap();
    let uid = volume.spec.uuid;

    // Publish + unpublish to establish health_info_id (so the volume is a
    // candidate for offline rebuild on the next degradation).
    volume_api
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
    volume_api
        .del_volume_target(&uid, None, None)
        .await
        .expect("Should unpublish volume");

    let victim_node = pick_victim_node(&api_client, &cluster.node(0).to_string(), uid).await;
    cluster
        .composer()
        .stop(&victim_node)
        .await
        .expect("Should stop io-engine node");

    wait_till_volume_status(
        cluster,
        &uid,
        VolumeStatus::Degraded,
        Duration::from_secs(10),
    )
    .await
    .expect("Volume should become Degraded");

    // Wait past the grace period plus a few reconcile cycles, then assert
    // that no temp nexus was created — the precondition check should have
    // deferred. Without the check, the reconciler would have created a
    // nexus and the underlying publish would fail at scheduling time, churning
    // each reconcile cycle.
    tokio::time::sleep(Duration::from_secs(GRACE_PERIOD_SECS + 3)).await;

    let vol = volume_api.get_volume(&uid).await.unwrap();
    assert!(
        vol.state.target.is_none(),
        "Temp nexus must not be created when no candidate pool exists for the \
        replacement replica; got target={:?}",
        vol.state.target
    );
    assert_eq!(
        vol.state.status,
        VolumeStatus::Degraded,
        "Volume should stay Degraded while no rebuild is viable"
    );

    // Bring the victim back. The original replica recovers and the volume
    // returns to Online without any temp nexus involvement.
    restart_node(cluster, &victim_node).await;
    wait_till_volume_status(cluster, &uid, VolumeStatus::Online, Duration::from_secs(30))
        .await
        .expect("Volume should return to Online after victim node restart");

    let vol = volume_api.get_volume(&uid).await.unwrap();
    assert!(
        vol.state.target.is_none(),
        "No temp nexus should have been created during the deferred window; \
        got target={:?}",
        vol.state.target
    );

    volume_api.del_volume(&uid).await.unwrap();
}

/// Pick a node that hosts a replica of the given volume, excluding the publish
/// target node. Filtering by the test volume's ownership matters because
/// leftover replicas from a prior scenario can otherwise be picked, and
/// stopping a node that doesn't host *this* volume's data leaves the volume
/// Online, the reconciler never sees a degraded state, and the test stalls.
async fn pick_victim_node(
    api_client: &stor_port::types::v0::openapi::tower::client::direct::ApiClient,
    target_node: &str,
    volume_uid: uuid::Uuid,
) -> String {
    let specs = api_client.specs_api().get_specs().await.unwrap();
    let owned_replica_uuids: Vec<uuid::Uuid> = specs
        .replicas
        .iter()
        .filter(|r| r.owners.volume == Some(volume_uid))
        .map(|r| r.uuid)
        .collect();
    let replicas = api_client.replicas_api().get_replicas().await.unwrap();
    replicas
        .iter()
        .find(|r| owned_replica_uuids.contains(&r.uuid) && r.node != target_node)
        .expect("Should have a replica of this volume on a non-target node")
        .node
        .clone()
}

/// Restart a node previously stopped by `composer().stop()` so the next
/// scenario starts from a healthy 3-node cluster. Waits for the node to
/// re-register as Online before returning, so the caller can assume a clean
/// cluster state.
async fn restart_node(cluster: &deployer_cluster::Cluster, node: &str) {
    cluster
        .composer()
        .start(node)
        .await
        .expect("Should restart io-engine node");
    let node_client = cluster.grpc_client().node();
    wait_node_online(&node_client, NodeId::from(node))
        .await
        .expect("Restarted node should come back Online");
}

/// Wait for the volume to have a target with no share protocol (offline-rebuild marker).
async fn wait_for_unshared_target(
    cluster: &deployer_cluster::Cluster,
    volume: &Uuid,
    timeout: Duration,
) -> Result<models::Nexus, String> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(vol) = cluster.rest_v00().volumes_api().get_volume(volume).await {
            if let Some(target) = vol.state.target {
                if target.protocol == models::Protocol::None {
                    return Ok(target);
                }
            }
        }
        if std::time::Instant::now() > (start + timeout) {
            return Err("Timeout waiting for unshared target".to_string());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait for the volume to reach a specific status, returning the full Volume.
async fn wait_for_volume_state(
    cluster: &deployer_cluster::Cluster,
    volume: &Uuid,
    status: VolumeStatus,
    timeout: Duration,
) -> Result<models::Volume, String> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(vol) = cluster.rest_v00().volumes_api().get_volume(volume).await {
            if vol.state.status == status {
                return Ok(vol);
            }
        }
        if std::time::Instant::now() > (start + timeout) {
            return Err(format!("Timeout waiting for volume status {status:?}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
