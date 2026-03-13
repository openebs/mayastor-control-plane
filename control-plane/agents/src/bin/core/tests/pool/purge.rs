use deployer_cluster::ClusterBuilder;
use grpc::operations::{
    pool::traits::PoolOperations, replica::traits::ReplicaOperations,
    volume::traits::VolumeOperations,
};
use std::time::Duration;
use stor_port::{
    transport_api::{ReplyErrorKind, ResourceKind},
    types::v0::transport::{CreateVolume, DestroyPool, Filter, NodeStatus, Pool, VolumeId},
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
        .with_io_engines(1)
        .with_pools(2) // pool-1 for purge tests, pool-2 for normal delete
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
    cluster.composer().stop("io-engine-1").await.unwrap();
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
