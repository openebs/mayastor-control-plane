#![cfg(test)]

//! End-to-end (deployer) coverage for pool drain.
//!
//! Each test brings up its own cluster since a drained pool stays cordoned, which would otherwise
//! leak into the placement of the following scenario.

use deployer_cluster::{Cluster, ClusterBuilder};
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
    let reconcile = Duration::from_millis(RECONCILE_PERIOD_MS);
    ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(nodes)
        .with_tmpfs_pool(POOL_SIZE)
        .with_cache_period("250ms")
        .with_reconcile_period(reconcile, reconcile)
        .with_options(|o| {
            o.with_max_concurrent_pool_drain(max_concurrent_drains)
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

/// Cancelling a drain whose spare is already placed destroys the spare and ends the move, keeping
/// the moving replica: the volume ends up on its original replicas.
#[tokio::test]
async fn drain_cancel_with_spare() {
    let cluster = drain_cluster(3, None, None).await;
    let volume = create_volume(&cluster, 2, true).await;

    let mut original = volume_pools(&cluster, &volume).await;
    original.sort();
    let source = original[0].clone();
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

    // The move reports the spare is being unwound as part of the cancel, until it is finalised.
    let mut saw_cancelled = false;
    let start = std::time::Instant::now();
    while has_drain_spec(&cluster, &source).await {
        saw_cancelled |= drain_state(&cluster, &source).await.is_some_and(|state| {
            state
                .moving_replicas
                .iter()
                .any(|m| m.unwind_spare == Some(models::UnwindSpare::Cancelled))
        });
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the cancelled drain to be finalised"
        );
        tokio::time::sleep(POLL).await;
    }
    assert!(
        saw_cancelled,
        "The move of the cancelled pool must have been marked as `Cancelled`"
    );

    // The spare is gone and both original replicas are kept.
    let replicas = wait_volume_replica_count(&cluster, &volume, 2, DRAIN_TIMEOUT).await;
    let mut pools = replicas.iter().map(|r| r.pool.clone()).collect::<Vec<_>>();
    pools.sort();
    assert_eq!(
        pools, original,
        "The volume must stay on its original replicas: {replicas:?}"
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

    // With the move gone, the replica count can be changed again.
    cluster
        .rest_v00()
        .volumes_api()
        .put_volume_replica_count(&volume, 1)
        .await
        .expect("Scale down is allowed once the move is gone");
}

/// A single replica volume on 3 nodes: draining the pool hosting the placed spare unwinds that
/// spare (`Respare`) and the move places a fresh one on the last free pool. The only replica is
/// kept until its replacement exists, so the volume is never left without a copy.
#[tokio::test]
async fn drain_respares_single_replica_volume() {
    let cluster = drain_cluster(3, None, None).await;
    // Published, so the spare has to be rebuilt through the nexus, which keeps the window in
    // which the spare exists open long enough to drain its pool.
    let volume = create_volume(&cluster, 1, true).await;

    let source = volume_pools(&cluster, &volume).await.remove(0);
    start_drain(&cluster, &source, models::PoolDrainReq::default()).await;

    // Wait for the spare to be placed, and find the replica and pool hosting it.
    let start = std::time::Instant::now();
    let (spare_pool, spare_replica) = loop {
        let replicas = volume_replicas(&cluster, &volume).await;
        if let Some(spare) = replicas.iter().find(|r| r.pool != source) {
            break (spare.pool.clone(), spare.uuid);
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the spare to be placed"
        );
        tokio::time::sleep(POLL).await;
    };

    // Draining the spare's pool must unwind the spare and place a new one on the last free pool.
    start_drain(&cluster, &spare_pool, models::PoolDrainReq::default()).await;

    // The move is marked to unwind its spare, and the initial spare is removed while the source
    // replica, the only complete copy, is still in place.
    let mut saw_respare = false;
    let start = std::time::Instant::now();
    loop {
        saw_respare |= drain_state(&cluster, &source).await.is_some_and(|state| {
            state
                .moving_replicas
                .iter()
                .any(|m| m.unwind_spare == Some(models::UnwindSpare::Respare))
        });
        let replicas = volume_replicas(&cluster, &volume).await;
        assert!(
            !replicas.is_empty(),
            "The volume must never be left without a replica"
        );
        if !replicas.iter().any(|r| r.uuid == spare_replica) {
            assert!(
                replicas.iter().any(|r| r.pool == source),
                "The spare was removed before its replacement took over: {replicas:?}"
            );
            break;
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT,
            "Timeout waiting for the spare '{spare_replica}' to be unwound: {replicas:?}"
        );
        tokio::time::sleep(POLL).await;
    }
    assert!(
        saw_respare,
        "The move of the source pool must have been marked as `Respare`"
    );

    // Both pools end up drained. At most one spare exists at any time: 1 replica + 1 spare.
    let mut max_replicas = 0;
    let start = std::time::Instant::now();
    loop {
        let replicas = volume_replicas(&cluster, &volume).await;
        assert!(!replicas.is_empty(), "The volume lost all its replicas");
        max_replicas = max_replicas.max(replicas.len());
        let source_state = drain_state(&cluster, &source).await.map(|s| s.phase);
        let spare_state = drain_state(&cluster, &spare_pool).await.map(|s| s.phase);
        if source_state == Some(PoolDrainPhase::Drained)
            && spare_state == Some(PoolDrainPhase::Drained)
        {
            break;
        }
        assert!(
            start.elapsed() < DRAIN_TIMEOUT * 2,
            "Timeout waiting for both pools to drain: {source_state:?} {spare_state:?}"
        );
        tokio::time::sleep(POLL).await;
    }
    assert!(
        max_replicas <= 2,
        "There must never be more than one spare, saw {max_replicas} replicas"
    );

    for pool in [&source, &spare_pool] {
        let state = drain_state(&cluster, pool).await.unwrap();
        assert!(
            state.moving_replicas.is_empty(),
            "No move may be left behind on '{pool}': {state:?}"
        );
    }

    // The volume ends up on the third pool, with its replica count unchanged.
    let replicas = wait_volume_replica_count(&cluster, &volume, 1, DRAIN_TIMEOUT).await;
    assert!(
        replicas
            .iter()
            .all(|r| r.pool != source && r.pool != spare_pool),
        "The replica must leave both drained pools: {replicas:?}"
    );
    wait_volume_online(&cluster, &volume, DRAIN_TIMEOUT).await;

    let vol = cluster
        .rest_v00()
        .volumes_api()
        .get_volume(&volume)
        .await
        .unwrap();
    assert_eq!(vol.spec.num_replicas, 1, "The replica count is unchanged");
    let target = vol.state.target.expect("Volume is still published");
    assert_eq!(target.children.len(), 1, "The nexus has 1 child");
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
