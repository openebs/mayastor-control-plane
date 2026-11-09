#![cfg(test)]

//! End-to-end (deployer) coverage for pool drain.
//!
//! Each test brings up its own cluster since a drained pool stays cordoned, which would otherwise
//! leak into the placement of the following scenario.

use deployer_cluster::{Cluster, ClusterBuilder, FindVolumeRequest};
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::openapi::models::{
    self, PoolDrainPhase, PoolDrainPhaseReason, VolumePolicy, VolumeStatus,
};
use uuid::Uuid;

const RECONCILE_PERIOD_MS: u64 = 250;
const POOL_SIZE: u64 = 100 * 1024 * 1024;
const VOLUME_SIZE: u64 = 10 * 1024 * 1024;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(100);

/// Builds a cluster with `nodes` io-engines, each with one tmpfs pool, tuned for drain tests.
async fn drain_cluster(
    nodes: u32,
    max_concurrent_drains: Option<u16>,
    replica_move_limit: Option<u32>,
) -> Cluster {
    drain_cluster_with_csi(nodes, max_concurrent_drains, replica_move_limit, false).await
}

/// Same as [`drain_cluster`], optionally with the csi node so that I/O can be issued to a
/// published volume.
async fn drain_cluster_with_csi(
    nodes: u32,
    max_concurrent_drains: Option<u16>,
    replica_move_limit: Option<u32>,
    csi: bool,
) -> Cluster {
    let reconcile = Duration::from_millis(RECONCILE_PERIOD_MS);
    ClusterBuilder::builder()
        .with_rest(true)
        .with_csi(csi, csi)
        .with_io_engines(nodes)
        .with_tmpfs_pool(POOL_SIZE)
        .with_cache_period("250ms")
        .with_reconcile_period(reconcile, reconcile)
        .with_options(|o| {
            // The csi node connects to the nexus from within its own container, which needs
            // isolated io-engines.
            o.with_isolated_io_engine(csi)
                .with_max_concurrent_pool_drain(max_concurrent_drains)
                .with_pool_replica_move_limit(replica_move_limit)
        })
        .build()
        .await
        .unwrap()
}

/// Creates a volume with `replicas` replicas, optionally publishing it on the first node.
async fn create_volume(cluster: &Cluster, replicas: u8, publish: bool) -> Uuid {
    let api_client = cluster.rest_v00();
    let volume_api = api_client.volumes_api();
    let id = Uuid::new_v4();
    let body =
        models::CreateVolumeBody::new(VolumePolicy::new(true), replicas, VOLUME_SIZE, false, false);
    volume_api.put_volume(&id, body).await.unwrap();
    if publish {
        volume_api
            .put_volume_target(
                &id,
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
    }
    id
}

/// The replicas owned by the volume.
async fn volume_replicas(cluster: &Cluster, volume: &Uuid) -> Vec<models::Replica> {
    let api = cluster.rest_v00();
    let specs = api.specs_api().get_specs().await.unwrap();
    let owned = specs
        .replicas
        .iter()
        .filter(|r| r.owners.volume == Some(*volume))
        .map(|r| r.uuid)
        .collect::<Vec<_>>();
    api.replicas_api()
        .get_replicas()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| owned.contains(&r.uuid))
        .collect()
}

/// The replica specs owned by the volume which are not (being) deleted. Unlike
/// [`volume_replicas`] these are also reported for replicas on a stopped node, which have no state.
async fn volume_replica_specs(cluster: &Cluster, volume: &Uuid) -> Vec<models::ReplicaSpec> {
    cluster
        .rest_v00()
        .specs_api()
        .get_specs()
        .await
        .unwrap()
        .replicas
        .into_iter()
        .filter(|r| r.owners.volume == Some(*volume) && r.status == models::SpecStatus::Created)
        .collect()
}

/// The ids of the pools hosting a replica of the volume.
async fn volume_pools(cluster: &Cluster, volume: &Uuid) -> Vec<String> {
    volume_replicas(cluster, volume)
        .await
        .into_iter()
        .map(|r| r.pool)
        .collect()
}

/// Starts draining the pool with the given request.
async fn start_drain(cluster: &Cluster, pool: &str, request: models::PoolDrainReq) {
    cluster
        .rest_v00()
        .pools_api()
        .put_pool_drain(pool, request)
        .await
        .expect("Should accept the drain request");
}

/// Cancels the drain of the pool.
async fn cancel_drain(cluster: &Cluster, pool: &str) {
    let request = models::PoolCordonReq {
        drain: Some(true),
        ..Default::default()
    };
    cluster
        .rest_v00()
        .pools_api()
        .del_pool_cordon(pool, request)
        .await
        .expect("Should accept the drain cancel request");
}

/// The current drain state of the pool, None if it has none.
async fn drain_state(cluster: &Cluster, pool: &str) -> Option<models::PoolDrainStateExt> {
    cluster
        .rest_v00()
        .pools_api()
        .get_pool_drain(pool)
        .await
        .ok()
}

/// Whether the pool still carries a drain spec.
async fn has_drain_spec(cluster: &Cluster, pool: &str) -> bool {
    let pool = cluster.rest_v00().pools_api().get_pool(pool).await.unwrap();
    matches!(
        pool.spec.and_then(|s| s.cordon_drain),
        Some(models::PoolCordonDrain::drain(_))
    )
}

/// Wait until the pool reaches the phase, and return its drain state.
async fn wait_for_phase(
    cluster: &Cluster,
    pool: &str,
    phase: PoolDrainPhase,
    timeout: Duration,
) -> models::PoolDrainStateExt {
    let start = std::time::Instant::now();
    loop {
        let state = drain_state(cluster, pool).await;
        if let Some(state) = state.as_ref().filter(|s| s.phase == phase) {
            return state.clone();
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for pool '{pool}' to reach {phase:?}, current: {state:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the pool is draining with at least one replica move in flight.
async fn wait_for_active_move(cluster: &Cluster, pool: &str) -> models::PoolDrainStateExt {
    let start = std::time::Instant::now();
    loop {
        let state = drain_state(cluster, pool).await;
        if let Some(state) = state
            .as_ref()
            .filter(|s| s.phase == PoolDrainPhase::Draining && !s.moving_replicas.is_empty())
        {
            return state.clone();
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timeout waiting for a replica move on pool '{pool}', current: {state:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the volume is Online.
async fn wait_volume_online(cluster: &Cluster, volume: &Uuid, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        let vol = cluster.rest_v00().volumes_api().get_volume(volume).await;
        if vol
            .as_ref()
            .is_ok_and(|v| v.state.status == VolumeStatus::Online)
        {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for volume '{volume}' to be Online, current: {vol:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the volume has exactly `count` replicas.
async fn wait_volume_replica_count(
    cluster: &Cluster,
    volume: &Uuid,
    count: usize,
    timeout: Duration,
) -> Vec<models::Replica> {
    let start = std::time::Instant::now();
    loop {
        let replicas = volume_replicas(cluster, volume).await;
        if replicas.len() == count {
            return replicas;
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for volume '{volume}' to have {count} replicas: {replicas:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// A safe drain of a pool hosting a replica of a 2-replica volume over-replicates (spare on the
/// third node), rebuilds the spare and then removes the replica from the drained pool.
#[tokio::test]
async fn drain_moves_replica_to_spare() {
    let cluster = drain_cluster(3, None, None).await;
    let volume = create_volume(&cluster, 2, true).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;

    let mut max_replicas = 0;
    let start = std::time::Instant::now();
    loop {
        max_replicas = max_replicas.max(volume_replicas(&cluster, &volume).await.len());
        let state = drain_state(&cluster, &source).await;
        if state.is_some_and(|s| s.phase == PoolDrainPhase::Drained) {
            break;
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for Drained"
        );
        tokio::time::sleep(POLL).await;
    }
    // The spare is added before the source replica is removed, so the volume is over-replicated
    // for a short while: 2 replicas + 1 spare, and never more than one spare.
    assert_eq!(
        max_replicas, 3,
        "Expected exactly one extra replica during the move, saw at most {max_replicas}"
    );

    let state = drain_state(&cluster, &source).await.unwrap();
    assert!(
        state.moving_replicas.is_empty(),
        "No move may be left behind: {state:?}"
    );

    let replicas = wait_volume_replica_count(&cluster, &volume, 2, DRAIN_TIMEOUT).await;
    assert!(
        replicas.iter().all(|r| r.pool != source),
        "No replica may stay on the drained pool: {replicas:?}"
    );
    wait_volume_online(&cluster, &volume, DRAIN_TIMEOUT).await;

    let vol = cluster
        .rest_v00()
        .volumes_api()
        .get_volume(&volume)
        .await
        .unwrap();
    assert_eq!(vol.spec.num_replicas, 2, "The replica count is unchanged");
    let target = vol.state.target.expect("Volume is still published");
    assert_eq!(target.children.len(), 2, "The nexus has 2 children");
    // Child uris carry the replica uuid, not the pool name, so match them against the replicas.
    let replica_ids = replicas
        .iter()
        .map(|r| r.uuid.to_string())
        .collect::<Vec<_>>();
    assert!(
        target
            .children
            .iter()
            .all(|c| replica_ids.iter().any(|id| c.uri.contains(id))),
        "The nexus must only use the volume's current replicas: {target:?}"
    );
}

/// A single replica volume is moved by a safe drain, but an unsafe drain refuses to evict it.
#[tokio::test]
async fn drain_single_replica_volume() {
    let cluster = drain_cluster(2, None, None).await;
    let volume = create_volume(&cluster, 1, false).await;

    // Safe drain: the single replica is moved through a spare.
    let first = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &first, models::PoolDrainReq::default()).await;
    wait_for_phase(&cluster, &first, PoolDrainPhase::Drained, DRAIN_TIMEOUT).await;

    let replicas = wait_volume_replica_count(&cluster, &volume, 1, DRAIN_TIMEOUT).await;
    let second = replicas[0].pool.clone();
    assert_ne!(second, first, "The replica must have left the drained pool");
    wait_volume_online(&cluster, &volume, DRAIN_TIMEOUT).await;

    // Unsafe drain of the pool which now holds the only copy: the drain must not make progress.
    let unsafe_drain = models::PoolDrainReq {
        unsafe_evict: Some(true),
        ..Default::default()
    };
    start_drain(&cluster, &second, unsafe_drain).await;
    let state = wait_for_phase(
        &cluster,
        &second,
        PoolDrainPhase::PartiallyDrained,
        DRAIN_TIMEOUT,
    )
    .await;
    assert_eq!(
        state.reason,
        Some(PoolDrainPhaseReason::SingleReplicaEviction)
    );
    let replicas = volume_replicas(&cluster, &volume).await;
    assert_eq!(replicas.len(), 1, "The only replica must be kept");
    assert_eq!(replicas[0].pool, second);
}

/// On a cluster with no pool to host the spare, a safe drain keeps the replica in place. Scaling
/// is refused while the move is pending, and deleting the volume lets the drain finish.
#[tokio::test]
async fn drain_without_spare_candidate_blocks() {
    let cluster = drain_cluster(2, None, None).await;
    let volume = create_volume(&cluster, 2, false).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;
    wait_for_active_move(&cluster, &source).await;

    // Nothing may be removed, as we could not over replicate somewhere.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let state = drain_state(&cluster, &source).await.unwrap();
    assert_eq!(state.phase, PoolDrainPhase::Draining);
    let replicas = volume_replicas(&cluster, &volume).await;
    assert_eq!(
        replicas.len(),
        2,
        "Both replicas must be kept: {replicas:?}"
    );
    assert!(replicas.iter().any(|r| r.pool == source));

    // The replica count can't be changed while a move is pending.
    let error = cluster
        .rest_v00()
        .volumes_api()
        .put_volume_replica_count(&volume, 3)
        .await
        .expect_err("Scale up must be refused during a replica move");
    assert!(
        format!("{error:?}").contains("waiting for one of its replicas to move"),
        "Unexpected error: {error:?}"
    );

    // The volume goes away mid-drain: the pool has nothing left, and finishes.
    cluster
        .rest_v00()
        .volumes_api()
        .del_volume(&volume)
        .await
        .unwrap();
    let state = wait_for_phase(&cluster, &source, PoolDrainPhase::Drained, DRAIN_TIMEOUT).await;
    assert!(state.moving_replicas.is_empty());
}

/// Cancelling a drain whose spare could not be placed drops the move and the drain, leaving the
/// volume untouched.
#[tokio::test]
async fn drain_cancel_without_spare() {
    let cluster = drain_cluster(2, None, None).await;
    let volume = create_volume(&cluster, 2, false).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;
    wait_for_active_move(&cluster, &source).await;

    cancel_drain(&cluster, &source).await;

    let start = std::time::Instant::now();
    while has_drain_spec(&cluster, &source).await {
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the cancelled drain to be finalised"
        );
        tokio::time::sleep(POLL).await;
    }

    let replicas = volume_replicas(&cluster, &volume).await;
    assert_eq!(replicas.len(), 2, "No replica may be touched: {replicas:?}");
    assert!(replicas.iter().any(|r| r.pool == source));
    let vol = cluster
        .rest_v00()
        .volumes_api()
        .get_volume(&volume)
        .await
        .unwrap();
    assert_eq!(
        vol.state.status,
        VolumeStatus::Online,
        "Volume must be untouched: {vol:?}"
    );

    // With the move gone, the replica count can be changed again.
    cluster
        .rest_v00()
        .volumes_api()
        .put_volume_replica_count(&volume, 1)
        .await
        .expect("Scale down is allowed once the move is gone");
}

/// Cancelling a drain whose spare is already placed lets the in-flight move complete, after which
/// the drain spec and state are removed.
#[tokio::test]
async fn drain_cancel_with_spare() {
    let cluster = drain_cluster(3, None, None).await;
    let volume = create_volume(&cluster, 2, true).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;

    // Cancel as soon as the spare is placed, while the volume is over-replicated.
    let start = std::time::Instant::now();
    while volume_replicas(&cluster, &volume).await.len() < 3 {
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the spare to be placed"
        );
        tokio::time::sleep(POLL).await;
    }
    cancel_drain(&cluster, &source).await;

    // The in-flight move completes, after which the drain spec and state are removed.
    wait_drain_finalised(&cluster, &source).await;
    assert!(
        drain_state(&cluster, &source).await.is_none(),
        "The drain state must be removed once the cancelled drain is finalised"
    );
}

/// A single replica volume on 3 nodes: draining the pool hosting the placed spare while the move
/// of the source pool is in flight. Both pools eventually get drained.
#[tokio::test]
async fn drain_respares_single_replica_volume() {
    let cluster = drain_cluster(3, None, None).await;
    // Published, so the spare has to be rebuilt through the nexus, which keeps the window in
    // which the spare exists open long enough to drain its pool.
    let volume = create_volume(&cluster, 1, true).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;

    // Wait for the spare to be placed, and find the pool hosting it.
    let start = std::time::Instant::now();
    let spare_pool = loop {
        let replicas = volume_replicas(&cluster, &volume).await;
        if let Some(spare) = replicas.iter().find(|r| r.pool != source) {
            break spare.pool.clone();
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the spare to be placed"
        );
        tokio::time::sleep(POLL).await;
    };

    start_drain(&cluster, &spare_pool, models::PoolDrainReq::default()).await;

    // Both pools end up drained.
    wait_for_phase(
        &cluster,
        &source,
        PoolDrainPhase::Drained,
        DRAIN_TIMEOUT * 2,
    )
    .await;
    wait_for_phase(
        &cluster,
        &spare_pool,
        PoolDrainPhase::Drained,
        DRAIN_TIMEOUT * 2,
    )
    .await;
}

/// With `unsafe_evict`, the replica is removed from the pool without placing a spare.
#[tokio::test]
async fn drain_unsafe_evict() {
    let cluster = drain_cluster(2, None, None).await;
    let volume = create_volume(&cluster, 2, false).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    let request = models::PoolDrainReq {
        unsafe_evict: Some(true),
        ..Default::default()
    };
    start_drain(&cluster, &source, request).await;
    wait_for_phase(&cluster, &source, PoolDrainPhase::Drained, DRAIN_TIMEOUT).await;

    let replicas = volume_replicas(&cluster, &volume).await;
    assert!(
        replicas.iter().all(|r| r.pool != source),
        "The replica must be evicted from the drained pool: {replicas:?}"
    );
}

/// With `unsafe_rebuild_otherwise_evict`, the replica is evicted only once the grace period for
/// placing a spare has elapsed.
#[tokio::test]
async fn drain_unsafe_rebuild_otherwise_evict() {
    const GRACE: Duration = Duration::from_secs(8);

    let cluster = drain_cluster(2, None, None).await;
    let volume = create_volume(&cluster, 2, false).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    let request = models::PoolDrainReq {
        unsafe_rebuild_otherwise_evict: Some(format!("{}s", GRACE.as_secs())),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    start_drain(&cluster, &source, request).await;
    wait_for_active_move(&cluster, &source).await;

    // No spare can be placed, so the replica stays until the grace period has passed.
    tokio::time::sleep(GRACE / 2).await;
    assert!(
        volume_replicas(&cluster, &volume)
            .await
            .iter()
            .any(|r| r.pool == source),
        "The replica may not be evicted before the grace period"
    );

    wait_for_phase(&cluster, &source, PoolDrainPhase::Drained, DRAIN_TIMEOUT).await;
    assert!(
        started.elapsed() >= GRACE,
        "Evicted before the grace period of {GRACE:?}: {:?}",
        started.elapsed()
    );
    let replicas = volume_replicas(&cluster, &volume).await;
    assert!(replicas.iter().all(|r| r.pool != source), "{replicas:?}");
}

/// With the concurrent drain cap at 1, the second pool waits in `Queued` until the first one is
/// done, and both eventually finish.
#[tokio::test]
async fn drain_respects_max_concurrent_drains() {
    let cluster = drain_cluster(4, Some(1), None).await;
    let first_volume = create_volume(&cluster, 2, false).await;
    let second_volume = create_volume(&cluster, 2, false).await;

    let pool_a = volume_pools(&cluster, &first_volume).await.remove(0);
    let pool_b = volume_pools(&cluster, &second_volume)
        .await
        .into_iter()
        .find(|p| *p != pool_a)
        .expect("The volume has replicas on 2 distinct pools");

    // Requested in order, so `pool_a` is the first in line.
    start_drain(&cluster, &pool_a, models::PoolDrainReq::default()).await;
    start_drain(&cluster, &pool_b, models::PoolDrainReq::default()).await;

    let mut saw_queued_behind_draining = false;
    let start = std::time::Instant::now();
    loop {
        let a = drain_state(&cluster, &pool_a).await.map(|s| s.phase);
        let b = drain_state(&cluster, &pool_b).await.map(|s| s.phase);
        assert!(
            !(a == Some(PoolDrainPhase::Draining) && b == Some(PoolDrainPhase::Draining)),
            "Both pools are draining despite the cap of 1"
        );
        assert!(
            !(b == Some(PoolDrainPhase::Draining) && a == Some(PoolDrainPhase::Queued)),
            "pool_b was promoted ahead of pool_a, violating FIFO order"
        );
        saw_queued_behind_draining |=
            a == Some(PoolDrainPhase::Draining) && b == Some(PoolDrainPhase::Queued);
        if a == Some(PoolDrainPhase::Drained) && b == Some(PoolDrainPhase::Drained) {
            break;
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT * 2,
            "Timeout waiting for both pools to drain: {a:?} {b:?}"
        );
        tokio::time::sleep(POLL).await;
    }
    assert!(
        saw_queued_behind_draining,
        "One pool must have waited in Queued while the other was Draining"
    );

    for volume in [first_volume, second_volume] {
        let replicas = wait_volume_replica_count(&cluster, &volume, 2, DRAIN_TIMEOUT).await;
        assert!(
            replicas
                .iter()
                .all(|r| r.pool != pool_a && r.pool != pool_b),
            "Replicas must leave both drained pools: {replicas:?}"
        );
        wait_volume_online(&cluster, &volume, DRAIN_TIMEOUT).await;
    }
}

/// With a replica move limit of 1, a pool never has more than one move in flight, even if
/// several volumes have a replica on it.
#[tokio::test]
async fn drain_respects_replica_move_limit() {
    let cluster = drain_cluster(3, None, Some(1)).await;
    let mut volumes = vec![];
    for _ in 0..3 {
        volumes.push(create_volume(&cluster, 2, false).await);
    }

    // 6 replicas over 3 pools: at least one pool hosts 2 or more of them.
    let mut per_pool: HashMap<String, usize> = HashMap::new();
    for volume in &volumes {
        for pool in volume_pools(&cluster, volume).await {
            *per_pool.entry(pool).or_default() += 1;
        }
    }
    let (source, hosted) = per_pool.into_iter().max_by_key(|(_, n)| *n).unwrap();
    assert!(hosted >= 2, "The test needs several replicas on one pool");

    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;

    let mut max_moves = 0;
    let start = std::time::Instant::now();
    loop {
        let state = drain_state(&cluster, &source).await;
        if let Some(state) = state {
            max_moves = max_moves.max(state.moving_replicas.len());
            assert!(
                state.moving_replicas.len() <= 1,
                "The move limit of 1 was exceeded: {state:?}"
            );
            if state.phase == PoolDrainPhase::Drained {
                break;
            }
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT * 2,
            "Timeout waiting for the pool to drain"
        );
        tokio::time::sleep(POLL).await;
    }
    assert_eq!(max_moves, 1, "At least one move must have been observed");

    for volume in volumes {
        let replicas = wait_volume_replica_count(&cluster, &volume, 2, DRAIN_TIMEOUT).await;
        assert!(replicas.iter().all(|r| r.pool != source), "{replicas:?}");
        wait_volume_online(&cluster, &volume, DRAIN_TIMEOUT).await;
    }
}

/// Stages the volume on csi node 0 and runs one small random-write fio pass against it.
/// Writes to a stopped replica fail right away, so the nexus faults that child quickly.
async fn run_small_fio(cluster: &Cluster, volume: &models::Volume) {
    let mut node = cluster.csi_node_client(0).await.unwrap();
    node.node_stage_volume(volume, HashMap::from([("ioTimeout".into(), "11".into())]))
        .await
        .unwrap();
    let device = node
        .internal()
        .find_volume(FindVolumeRequest {
            volume_id: volume.spec.uuid.to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .device_path;
    let cmd = [
        "taskset",
        "-c",
        cluster.fio_taskset().as_str(),
        "fio",
        "--direct=1",
        "--ioengine=libaio",
        "--bs=4k",
        "--iodepth=16",
        "--size=4m",
        "--loops=1",
        "--numjobs=1",
        "--name=fio",
        "--readwrite=randwrite",
    ]
    .into_iter()
    .map(ToString::to_string)
    .chain([format!("--filename={}", device.trim_end())])
    .collect::<Vec<_>>();
    let (code, out) = cluster
        .composer()
        .exec(&cluster.csi_container(0), cmd)
        .await
        .unwrap();
    assert_eq!(
        code,
        Some(0),
        "fio must pass on the remaining healthy children: {out}"
    );
}

/// Wait until the nexus child backed by `replica` is no longer Online.
async fn wait_child_unhealthy(cluster: &Cluster, volume: &Uuid, replica: &Uuid) {
    let start = std::time::Instant::now();
    loop {
        let vol = cluster
            .rest_v00()
            .volumes_api()
            .get_volume(volume)
            .await
            .unwrap();
        let unhealthy = vol.state.target.as_ref().is_some_and(|target| {
            target.children.iter().any(|c| {
                c.uri.contains(&replica.to_string()) && c.state != models::ChildState::Online
            })
        });
        if unhealthy {
            return;
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the child of replica '{replica}' to fault"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the drain spec of the pool is gone.
async fn wait_drain_finalised(cluster: &Cluster, pool: &str) {
    let start = std::time::Instant::now();
    while has_drain_spec(cluster, pool).await {
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the drain of '{pool}' to be finalised"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// A published `replicas` replica volume (on a cluster with at least `replicas` + 1 nodes), drain
/// started and the spare placed. The source is the replica on the nexus node, if any. Then an
/// unrelated replica (not on the source pool, nor on the nexus node) is Faulted by stopping its io-engine
/// and issuing a small fio. The move can't complete while that replica is unhealthy.
/// Returns (volume, original replicas, source pool, faulted replica).
async fn drain_with_unrelated_faulted(
    cluster: &Cluster,
    replicas: u8,
    snapshot: bool,
) -> (Uuid, Vec<models::Replica>, String, models::Replica) {
    let volume_id = create_volume(cluster, replicas, true).await;
    if snapshot {
        cluster
            .rest_v00()
            .snapshots_api()
            .put_volume_snapshot(&volume_id, &Uuid::new_v4())
            .await
            .expect("Should create the snapshot");
    }

    let originals = volume_replicas(cluster, &volume_id).await;
    let nexus_node = cluster.node(0).to_string();
    let source = originals
        .iter()
        .find(|r| r.node == nexus_node)
        .unwrap_or(&originals[0])
        .pool
        .clone();
    start_drain(cluster, &source, models::PoolDrainReq::default()).await;

    // Wait for the spare to be placed.
    let start = std::time::Instant::now();
    while volume_replicas(cluster, &volume_id).await.len() <= replicas as usize {
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the spare to be placed"
        );
        tokio::time::sleep(POLL).await;
    }

    let victim = originals
        .iter()
        .find(|r| r.pool != source && r.node != nexus_node)
        .expect("An unrelated replica which is not on the nexus node")
        .clone();
    cluster.composer().stop(&victim.node).await.unwrap();

    let volume = cluster
        .rest_v00()
        .volumes_api()
        .get_volume(&volume_id)
        .await
        .unwrap();
    run_small_fio(cluster, &volume).await;
    wait_child_unhealthy(cluster, &volume_id, &victim.uuid).await;

    // If the move already completed we lost the race against the rebuild: fail clearly.
    assert!(
        has_drain_spec(cluster, &source).await,
        "The move completed before the unrelated replica faulted, the test lost the race"
    );
    (volume_id, originals, source, victim)
}

/// Cancelling a drain while the spare is online and an unrelated replica is Faulted: the move
/// can't complete while that replica is unhealthy. Once its node is back and it rebuilds, the move
/// completes and the drain spec and state are removed.
#[tokio::test]
async fn drain_cancel_with_unrelated_faulted() {
    let cluster = drain_cluster_with_csi(3, None, None, true).await;
    let (_volume, _originals, source, victim) =
        drain_with_unrelated_faulted(&cluster, 2, false).await;

    cancel_drain(&cluster, &source).await;

    cluster.composer().start(&victim.node).await.unwrap();
    wait_drain_finalised(&cluster, &source).await;
    assert!(
        drain_state(&cluster, &source).await.is_none(),
        "The drain state must be removed once the cancelled drain is finalised"
    );
}

/// Same as above but with a snapshot: the faulted replica holds snapshot data, so it is not
/// removed and the move stays in place. Once the replica's node is back and it rebuilds, the move
/// completes and the drain spec and state are removed.
#[tokio::test]
async fn drain_cancel_with_unrelated_faulted_and_snapshot() {
    let cluster = drain_cluster_with_csi(3, None, None, true).await;
    let (volume, _originals, source, victim) =
        drain_with_unrelated_faulted(&cluster, 2, true).await;

    cancel_drain(&cluster, &source).await;

    // Hold for ~10 reconcile periods, checking on every poll that nothing was removed.
    let polls = (10 * RECONCILE_PERIOD_MS / POLL.as_millis() as u64).max(1);
    for _ in 0..polls {
        let replicas = volume_replica_specs(&cluster, &volume).await;
        assert!(
            replicas.iter().any(|r| r.uuid == victim.uuid),
            "A replica of a volume with snapshots must not be removed: {replicas:?}"
        );
        assert!(
            has_drain_spec(&cluster, &source).await,
            "The move must stay while an unrelated replica is unhealthy"
        );
        tokio::time::sleep(POLL).await;
    }

    // The unrelated replica's node comes back: the child rebuilds (partially) and the move
    // can now complete.
    cluster.composer().start(&victim.node).await.unwrap();
    wait_drain_finalised(&cluster, &source).await;
    assert!(
        drain_state(&cluster, &source).await.is_none(),
        "The drain state must be removed once the cancelled drain is finalised"
    );
}
