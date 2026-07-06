use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    pool::traits::PoolOperations, replica::traits::ReplicaOperations,
    volume::traits::VolumeOperations,
};
use std::{collections::HashMap, time::Duration};
use stor_port::{
    pstor::{etcd::Etcd, StoreObj},
    transport_api::{ReplyErrorKind, ResourceKind},
    types::v0::{
        store::{
            pool::{PoolOperation, PoolOperationState, PoolSpec, PoolSpecKey},
            SpecStatus,
        },
        transport::{
            CreatePool, CreateVolume, DestroyPool, DestroyVolume, Filter, GetVolumes, NodeStatus,
            Pool, PoolId, PublishVolume, VolumeAccessMode, VolumeId, VolumeStatus,
        },
    },
};

use grpc::operations::pool::traits::PoolCordonRequest;

const NODE_OFFLINE_TIMEOUT: Duration = Duration::from_secs(10);
const VOLUME_SIZE: u64 = 5242880;

/// All pool purge tests share a single cluster to avoid repeated spinup costs.
#[tokio::test]
async fn pool_purge() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        // Engine 1 is intentionally idle for phases 1-3 (no pools on it). The existing
        // phases assume the volume replica goes on the engine being stopped, which is
        // guaranteed when engine 1 has no pool to land on. Phase 4 then provisions a
        // pool on engine 1 to act as the surviving online destination that the bug
        // would write a stray replica to.
        .with_io_engines(2)
        .with_pool(0, "malloc:///disk1?size_mb=100")
        .with_pool(0, "malloc:///disk2?size_mb=100")
        .with_cache_period("100ms")
        .with_node_deadline("100ms")
        .with_reconcile_period(Duration::from_millis(100), Duration::from_millis(100))
        .build()
        .await
        .unwrap();

    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();

    let pools = pool_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pools.len(), 2, "Expected 2 pools");

    // Create a volume with 1 replica so one pool has data.
    let vol_id = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: vol_id.clone(),
                size: VOLUME_SIZE,
                replicas: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Figure out which pool got the replica.
    let replicas = replica_client
        .get(Filter::Volume(vol_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert!(!replicas.is_empty(), "Volume should have a replica");
    let replica_pool_id = replicas[0].pool_id.clone();

    let (purge_pool, delete_pool) = if replica_pool_id == *pools[0].id() {
        (pools[0].clone(), pools[1].clone())
    } else {
        (pools[1].clone(), pools[0].clone())
    };

    // --- Phase 1: io-engine online ---
    purge_rejected_pool_state_online(&pool_client, &purge_pool).await;
    normal_delete_still_works(&pool_client, &delete_pool).await;

    // --- Phase 2: io-engine offline ---
    cluster.composer().stop(&cluster.node(0)).await.unwrap();
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Offline, NODE_OFFLINE_TIMEOUT)
        .await
        .expect("Node should go offline");

    purge_rejected_not_cordoned(&pool_client, &purge_pool).await;
    purge_rejected_insufficient_cordon(&pool_client, &purge_pool).await;

    // Cordon properly for remaining tests.
    pool_client
        .cordon(PoolCordonRequest {
            node_id: None,
            pool_id: purge_pool.id().clone(),
            replicas: true,
            snapshots: true,
            restores: false,
            import: false,
        })
        .await
        .unwrap();

    purge_rejected_without_accept(&pool_client, &purge_pool).await;
    purge_rejected_volume_loss_without_accept(&pool_client, &purge_pool).await;
    purge_succeeds_with_volume_loss(&pool_client, &purge_pool, &vol_id).await;

    // --- Phase 3: reconciler resume after simulated crash ---
    purge_reconciler_resumes_interrupted(&cluster).await;

    // --- Phase 4: regression — purging the last healthy replica must not silently
    //     create a fresh empty replica on a surviving node.
    purge_skips_replica_creation_on_last_healthy_loss(&cluster).await;
}

/// Purge must be rejected when the pool's node is online (state is not Unknown).
async fn purge_rejected_pool_state_online(pool_client: &dyn PoolOperations, pool: &Pool) {
    let destroy = DestroyPool::purge(pool.node(), pool.id().clone()).with_accept(true);
    let err = pool_client
        .destroy(&destroy, None)
        .await
        .expect_err("Purge should be rejected for online pool");

    assert_eq!(err.kind, ReplyErrorKind::PoolNotPurgeable);
    assert_eq!(err.resource, ResourceKind::Pool);
}

/// Normal (non-purge) delete should work on an empty pool.
async fn normal_delete_still_works(pool_client: &dyn PoolOperations, pool: &Pool) {
    let pool_id = pool.id().clone();
    let destroy = DestroyPool::new(pool.node(), pool_id.clone());

    let result = pool_client
        .destroy(&destroy, None)
        .await
        .expect("Normal delete should succeed");

    assert!(
        result.is_none(),
        "Normal delete should not return a PoolDeleteResult"
    );

    // Verify pool is gone.
    let pools = pool_client.get(Filter::Pool(pool_id), None).await;
    assert!(
        pools.is_err() || pools.unwrap().into_inner().is_empty(),
        "Pool should be gone after delete"
    );
}

/// Purge must be rejected when the pool is not cordoned.
async fn purge_rejected_not_cordoned(pool_client: &dyn PoolOperations, pool: &Pool) {
    let destroy = DestroyPool::purge(pool.node(), pool.id().clone()).with_accept(true);
    let err = pool_client
        .destroy(&destroy, None)
        .await
        .expect_err("Purge should be rejected for uncordoned pool");

    assert_eq!(err.kind, ReplyErrorKind::PoolNotCordoned);
    assert_eq!(err.resource, ResourceKind::Pool);
}

/// Purge must be rejected when cordon doesn't block both replicas and snapshots.
async fn purge_rejected_insufficient_cordon(pool_client: &dyn PoolOperations, pool: &Pool) {
    // Cordon with only replicas (not snapshots).
    pool_client
        .cordon(PoolCordonRequest {
            node_id: None,
            pool_id: pool.id().clone(),
            replicas: true,
            snapshots: false,
            restores: false,
            import: false,
        })
        .await
        .unwrap();

    let destroy = DestroyPool::purge(pool.node(), pool.id().clone()).with_accept(true);
    let err = pool_client
        .destroy(&destroy, None)
        .await
        .expect_err("Purge should be rejected for insufficiently cordoned pool");

    assert_eq!(err.kind, ReplyErrorKind::PoolCordonInsufficient);
    assert_eq!(err.resource, ResourceKind::Pool);

    // Clean up: uncordon so the main test can cordon properly.
    pool_client
        .uncordon(PoolCordonRequest {
            node_id: None,
            pool_id: pool.id().clone(),
            replicas: true,
            snapshots: false,
            restores: false,
            import: false,
        })
        .await
        .unwrap();
}

/// Purge must be rejected when pool has replicas but --accept is not set.
async fn purge_rejected_without_accept(pool_client: &dyn PoolOperations, pool: &Pool) {
    // accept defaults to false.
    let destroy = DestroyPool::purge(pool.node(), pool.id().clone());
    let err = pool_client
        .destroy(&destroy, None)
        .await
        .expect_err("Purge should be rejected without accept");

    assert_eq!(err.kind, ReplyErrorKind::PoolPurgeAcceptRequired);
    assert_eq!(err.resource, ResourceKind::Pool);
}

/// Purge must be rejected when volume loss would occur but --accept-volume-loss is not set.
async fn purge_rejected_volume_loss_without_accept(pool_client: &dyn PoolOperations, pool: &Pool) {
    let destroy = DestroyPool::purge(pool.node(), pool.id().clone()).with_accept(true);
    // accept_volume_loss defaults to false.
    let err = pool_client
        .destroy(&destroy, None)
        .await
        .expect_err("Purge should be rejected without volume loss accept");

    assert_eq!(err.kind, ReplyErrorKind::PoolPurgeVolumeLossAcceptRequired);
    assert_eq!(err.resource, ResourceKind::Pool);
    assert!(
        !err.volume_loss.volumes.is_empty(),
        "Error should carry the volume loss details"
    );
}

/// Purge succeeds when all confirmations are provided, reports data loss.
async fn purge_succeeds_with_volume_loss(
    pool_client: &dyn PoolOperations,
    pool: &Pool,
    volume_id: &VolumeId,
) {
    let pool_id = pool.id().clone();
    let destroy = DestroyPool::purge(pool.node(), pool_id.clone())
        .with_accept(true)
        .with_accept_volume_loss(true)
        .with_accept_snapshot_loss(true);

    let result = pool_client
        .destroy(&destroy, None)
        .await
        .expect("Purge should succeed with all confirmations")
        .expect("Purge should return a PoolDeleteResult");

    assert_eq!(result.pool_id, pool_id);
    assert!(
        !result.volume_loss.volumes.is_empty(),
        "Should report data loss"
    );
    assert_eq!(result.volume_loss.volumes.len(), 1);
    assert_eq!(result.volume_loss.volumes[0].volume_id, *volume_id);
    assert_eq!(result.volume_loss.volumes[0].healthy_after, 0);

    // Verify pool is gone.
    let pools = pool_client.get(Filter::Pool(pool_id), None).await;
    assert!(
        pools.is_err() || pools.unwrap().into_inner().is_empty(),
        "Pool should be gone after purge"
    );
}

/// Simulate a crash after the pool is marked Purging (step 8) but before replicas
/// are deleted. Write the Purging state directly to etcd, restart core, and verify
/// the reconciler resumes and completes the purge.
async fn purge_reconciler_resumes_interrupted(cluster: &Cluster) {
    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();

    // 1. Bring io-engine back online and create a fresh pool + volume.
    cluster.composer().start("io-engine-1").await.unwrap();
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Online, NODE_OFFLINE_TIMEOUT)
        .await
        .expect("Node should come online");

    let pool_id: PoolId = "resume-purge-pool".into();
    pool_client
        .create(
            &CreatePool {
                node: cluster.node(0),
                id: pool_id.clone(),
                disks: vec!["malloc:///resume_disk?size_mb=100".into()],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Pool creation should succeed");

    let vol_id = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: vol_id.clone(),
                size: VOLUME_SIZE,
                replicas: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Volume creation should succeed");

    // Verify replica landed on our pool.
    let replicas = replica_client
        .get(Filter::Volume(vol_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert!(!replicas.is_empty(), "Volume should have a replica");
    assert_eq!(
        replicas[0].pool_id, pool_id,
        "Replica should be on the new pool"
    );

    // 2. Stop node and cordon the pool.
    cluster.composer().stop("io-engine-1").await.unwrap();
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Offline, NODE_OFFLINE_TIMEOUT)
        .await
        .expect("Node should go offline");

    pool_client
        .cordon(PoolCordonRequest {
            node_id: None,
            pool_id: pool_id.clone(),
            replicas: true,
            snapshots: true,
            restores: false,
            import: false,
        })
        .await
        .expect("Cordon should succeed");

    // 3. Simulate crash after step 8: write Purging state + Destroy op to etcd.
    let mut etcd = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd");
    let (mut pool_spec, _): (PoolSpec, i64) = etcd
        .get_obj(&PoolSpecKey::from(&pool_id))
        .await
        .expect("Pool spec should exist in etcd");

    pool_spec.status = SpecStatus::Purging;
    pool_spec.operation = Some(PoolOperationState {
        operation: PoolOperation::Destroy,
        result: None,
    });
    etcd.put_obj(&pool_spec)
        .await
        .expect("Failed to write Purging pool spec to etcd");

    // 4. Restart core agent — reconciler should detect Purging and resume the purge.
    cluster
        .restart_core_with_liveness(None)
        .await
        .expect("Core agent should restart and become live");

    // Wait for the reconciler to complete the purge.
    // Filter::Pool returns Err(NotFound) when the pool is gone, not Ok(empty vec).
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();
    loop {
        match pool_client.get(Filter::Pool(pool_id.clone()), None).await {
            Err(e) if e.kind == ReplyErrorKind::NotFound => break,
            Ok(pools) if pools.clone().into_inner().is_empty() => break,
            _ => {}
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!("Timed out waiting for reconciler to complete pool purge");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5. Verify replicas are gone (retry briefly in case reconciler is still finishing).
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        match replica_client
            .get(Filter::Volume(vol_id.clone()), None)
            .await
        {
            Err(e) if e.kind == ReplyErrorKind::NotFound => break,
            Ok(replicas) if replicas.clone().into_inner().is_empty() => break,
            _ => {}
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!("Timed out waiting for replicas to be cleaned up after pool purge");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Setup: starts the engine that was left stopped by Phase 3 and creates a fresh
/// pool on each io-engine. A 1-replica volume is published on the node holding its
/// replica, so:
///   - the volume has `health_info_id` set (NexusInfo persisted to etcd),
///   - both the replica and the nexus live on node-A,
///   - node-B has a pool with free space — the candidate destination if the bug fires.
///
/// Sequence:
///   1. Stop node-A. The nexus state can no longer be fetched, so `volume_state`
///      (which `hot_spare_reconcile` calls without health info) returns `Unknown`.
///   2. Cordon and purge the pool on node-A. The replica is destroyed (spec-only).
///   3. Without the guard in `volume_replica_count_reconciler_traced`, the dispatch
///      `Unknown -> hot_spare_nexus_reconcile -> volume_replica_count_reconciler`
///      sees `0 < 1` replicas and creates a brand new empty replica on node-B,
///      silently masking the data loss.
///
/// Expected with the fix:
///   - No new replica appears on node-B.
///   - The volume settles into a state without a healthy replica (Faulted/Unknown).
async fn purge_skips_replica_creation_on_last_healthy_loss(cluster: &Cluster) {
    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();

    // Phase 3 ends with io-engine-1 stopped and no pools anywhere. Bring it back so
    // we have two engines to play with, then create a fresh pool on each.
    cluster.composer().start("io-engine-1").await.unwrap();
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Online, NODE_OFFLINE_TIMEOUT)
        .await
        .expect("io-engine-1 should come online");

    // Phases 1 and 3 leave behind volume specs with zero replicas (their pools were
    // purged). Before we provision new pools, destroy them — otherwise the hot-spare
    // reconciler will repopulate them on whichever pool we create first, which both
    // pollutes the test and breaks the volume_loss accounting in the purge below
    // (the analyzer would report multiple affected volumes).
    let leftover = volume_client
        .get(Filter::None, false, None, None)
        .await
        .expect("Should be able to list volumes")
        .entries;
    for vol in leftover {
        let _ = volume_client
            .destroy(&DestroyVolume::new(vol.uuid()), None)
            .await;
    }

    let pool_a: PoolId = "data-loss-pool-a".into();
    let pool_b: PoolId = "data-loss-pool-b".into();
    pool_client
        .create(
            &CreatePool {
                node: cluster.node(0),
                id: pool_a.clone(),
                disks: vec!["malloc:///data_loss_a?size_mb=100".into()],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Pool A creation on io-engine-1 should succeed");
    pool_client
        .create(
            &CreatePool {
                node: cluster.node(1),
                id: pool_b.clone(),
                disks: vec!["malloc:///data_loss_b?size_mb=100".into()],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Pool B creation on io-engine-2 should succeed");

    // 1-replica volume; the replica lands on whichever pool the scheduler picks.
    let vol_id = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: vol_id.clone(),
                size: VOLUME_SIZE,
                replicas: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Volume creation should succeed");

    // Identify the replica's node — that's the pool we'll purge.
    let replicas = replica_client
        .get(Filter::Volume(vol_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replicas.len(), 1, "Volume should have a single replica");
    let replica_node = replicas[0].node.clone();
    let replica_pool: PoolId = replicas[0].pool_id.clone();
    let surviving_node = if replica_node == cluster.node(0) {
        cluster.node(1)
    } else {
        cluster.node(0)
    };

    // Publish on the replica's node so the nexus shares its fate when we stop the engine.
    // This forces `volume_state` to see `target.is_some()` but fail to fetch nexus state,
    // landing the volume in `Unknown` — the exact path the bug rides on.
    volume_client
        .publish(
            &PublishVolume::new(
                vol_id.clone(),
                Some(replica_node.clone()),
                None,
                HashMap::new(),
                vec![],
                VolumeAccessMode::SingleNodeWriter,
            ),
            None,
        )
        .await
        .expect("Volume should publish on the replica's node");

    // Stop the io-engine holding the replica + nexus. NodeId stringifies to the same
    // name as the composer container ("io-engine-N").
    let engine_name = replica_node.to_string();
    cluster
        .composer()
        .stop(&engine_name)
        .await
        .expect("Should stop io-engine");
    cluster
        .wait_node_status_tmo(
            replica_node.clone(),
            NodeStatus::Offline,
            NODE_OFFLINE_TIMEOUT,
        )
        .await
        .expect("Node should go offline");

    // Cordon and purge.
    pool_client
        .cordon(PoolCordonRequest {
            node_id: None,
            pool_id: replica_pool.clone(),
            replicas: true,
            snapshots: true,
            restores: false,
            import: false,
        })
        .await
        .expect("Cordon should succeed");

    let destroy = DestroyPool::purge(replica_node.clone(), replica_pool.clone())
        .with_accept(true)
        .with_accept_volume_loss(true)
        .with_accept_snapshot_loss(true);
    let result = pool_client
        .destroy(&destroy, None)
        .await
        .expect("Purge should succeed")
        .expect("Purge should return a PoolDeleteResult");
    assert_eq!(result.volume_loss.volumes.len(), 1);
    assert_eq!(result.volume_loss.volumes[0].volume_id, vol_id);
    assert_eq!(result.volume_loss.volumes[0].healthy_after, 0);

    // Give the reconciler a generous window to misbehave. With reconcile_period at
    // 100ms, this is ~30 ticks — plenty of opportunity for the buggy code path to
    // kick in if the guard isn't there.
    let observation_window = Duration::from_secs(3);
    let start = std::time::Instant::now();
    while std::time::Instant::now() < start + observation_window {
        // Re-check on every tick: any replica appearing is an immediate failure,
        // regardless of which pool it landed on. Catching it as soon as it appears
        // keeps the failure log close to the cause.
        let replicas = match replica_client
            .get(Filter::Volume(vol_id.clone()), None)
            .await
        {
            Ok(r) => r.into_inner(),
            Err(e) if e.kind == ReplyErrorKind::NotFound => Vec::new(),
            Err(e) => panic!("unexpected error querying replicas: {e:?}"),
        };
        assert!(
            replicas.is_empty(),
            "Reconciler created a new replica after the last healthy one was purged \
             (replicas: {:?}). This is the data-loss-masking bug the guard prevents.",
            replicas
                .iter()
                .map(|r| (r.node.clone(), r.pool_id.clone()))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Sanity: the candidate destination pool is still present and could have received
    // a replica — so the absence above isn't a placement failure, it's the guard
    // correctly refusing to create.
    let surviving_pools = pool_client
        .get(Filter::Node(surviving_node.clone()), None)
        .await
        .expect("Should be able to list pools on the surviving node")
        .into_inner();
    assert!(
        !surviving_pools.is_empty(),
        "Surviving node should still have its pool — otherwise the no-replica result \
         doesn't prove anything"
    );

    // The volume should report no healthy replica. Status check is a soft confirmation:
    // depending on whether the nexus state is reachable through any path it may end up
    // Faulted or Unknown, but never Online/Degraded.
    let volume = volume_client
        .get(GetVolumes::new(&vol_id).filter, false, None, None)
        .await
        .expect("Volume should still exist")
        .entries
        .into_iter()
        .next()
        .expect("Volume should be returned");
    let status = volume.state().status;
    assert!(
        matches!(status, VolumeStatus::Faulted | VolumeStatus::Unknown),
        "Volume should not appear healthy after losing its only replica, got: {status:?}"
    );

    // Replica spec count should be zero.
    let final_replicas = match replica_client
        .get(Filter::Volume(vol_id.clone()), None)
        .await
    {
        Ok(r) => r.into_inner().len(),
        Err(e) if e.kind == ReplyErrorKind::NotFound => 0,
        Err(e) => panic!("unexpected error: {e:?}"),
    };
    assert_eq!(final_replicas, 0, "Volume should have no replicas");
}
