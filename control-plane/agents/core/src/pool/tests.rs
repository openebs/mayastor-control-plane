#![cfg(test)]

use common_lib::{
    mbus_api::{Message, ReplyError, ReplyErrorKind, ResourceKind, TimeoutOptions},
    types::v0::{
        message_bus::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, Filter, GetNodes, GetSpecs,
            Protocol, Replica, ReplicaId, ReplicaName, ReplicaShareProtocol, ReplicaStatus,
            ShareReplica, UnshareReplica, VolumeId,
        },
        openapi::{
            apis::StatusCode,
            clients::tower::Error,
            models::{CreateVolumeBody, Pool, PoolState, VolumePolicy},
        },
        store::replica::ReplicaSpec,
    },
};
use grpc::{
    grpc_opts::Context,
    operations::{pool::traits::PoolOperations, replica::traits::ReplicaOperations},
};
use itertools::Itertools;
use std::{convert::TryFrom, time::Duration};
use testlib::{Cluster, ClusterBuilder};

#[tokio::test]
async fn pool() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .build()
        .await
        .unwrap();

    let mayastor = cluster.node(0);
    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let pool_client = cluster.grpc_client().pool();
    let rep_client = cluster.grpc_client().replica();

    let pool = pool_client
        .create(
            &CreatePool {
                node: mayastor.clone(),
                id: "pooloop".into(),
                disks: vec!["malloc:///disk0?size_mb=100".into()],
                labels: None,
            },
            None,
        )
        .await
        .unwrap();
    tracing::info!("Pools: {:?}", pool);

    let pools = pool_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Pools: {:?}", pools);

    let replica = rep_client
        .create(
            &CreateReplica {
                node: mayastor.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool: "pooloop".into(),
                size: 12582912, /* actual size will be a multiple of 4MB so just
                                 * create it like so */
                thin: true,
                share: Protocol::None,
                name: None,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    tracing::info!("Replicas: {:?}", replica);

    let replicas = rep_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Replicas: {:?}", replicas);

    let uri = replica.uri.clone();
    assert_eq!(
        replica,
        Replica {
            node: mayastor.clone(),
            name: ReplicaName::from("cf36a440-74c6-4042-b16c-4f7eddfc24da"),
            uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
            pool: "pooloop".into(),
            thin: false,
            size: 12582912,
            share: Protocol::None,
            uri,
            status: ReplicaStatus::Online
        }
    );

    let uri = rep_client
        .share(
            &ShareReplica {
                node: mayastor.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool: "pooloop".into(),
                protocol: ReplicaShareProtocol::Nvmf,
                name: None,
            },
            None,
        )
        .await
        .unwrap();

    let mut replica_updated = replica;
    replica_updated.uri = uri;
    replica_updated.share = Protocol::Nvmf;
    let replica = rep_client.get(Filter::None, None).await.unwrap();
    let replica = replica.0.first().unwrap();
    assert_eq!(replica, &replica_updated);

    let error = pool_client
        .destroy(
            &DestroyPool {
                node: mayastor.clone(),
                id: "pooloop".into(),
            },
            None,
        )
        .await
        .expect_err("Should fail to destroy a pool that is in use.");

    assert!(matches!(
        error,
        ReplyError {
            kind: ReplyErrorKind::InUse,
            resource: ResourceKind::Pool,
            ..
        }
    ));
    rep_client
        .destroy(
            &DestroyReplica {
                node: mayastor.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool: "pooloop".into(),
                name: None,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    assert!(rep_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .0
        .is_empty());

    pool_client
        .destroy(
            &DestroyPool {
                node: mayastor.clone(),
                id: "pooloop".into(),
            },
            None,
        )
        .await
        .unwrap();

    assert!(pool_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .0
        .is_empty());
}

/// The tests below revolve around transactions and are dependent on the core agent's command line
/// arguments for timeouts.
/// This is required because as of now, we don't have a good mocking strategy

/// default timeout options for every bus request
fn bus_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::default()
        .with_max_retries(0)
        .with_timeout(Duration::from_millis(250))
}

/// Get the replica spec
async fn replica_spec(replica: &Replica) -> Option<ReplicaSpec> {
    GetSpecs {}
        .request()
        .await
        .unwrap()
        .replicas
        .iter()
        .find(|r| r.uuid == replica.uuid)
        .cloned()
}

/// Tests replica share and unshare operations as a transaction
#[tokio::test]
async fn replica_transaction() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        .with_agents(vec!["core"])
        .with_req_timeouts(Duration::from_millis(250), Duration::from_millis(500))
        .with_bus_timeouts(bus_timeout_opts())
        .build()
        .await
        .unwrap();
    let mayastor = cluster.node(0);

    let pool_client = cluster.grpc_client().pool();
    let rep_client = cluster.grpc_client().replica();

    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let pools = pool_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Pools: {:?}", pools);

    let replica = rep_client
        .create(
            &CreateReplica {
                node: mayastor.clone(),
                uuid: ReplicaId::new(),
                pool: cluster.pool(0, 0),
                size: 12582912,
                thin: false,
                share: Protocol::None,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    async fn check_operation(replica: &Replica, protocol: Protocol) {
        // operation in progress
        assert!(replica_spec(replica).await.unwrap().operation.is_some());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // operation is completed
        assert!(replica_spec(replica).await.unwrap().operation.is_none());
        assert_eq!(replica_spec(replica).await.unwrap().share, protocol);
    }

    // pause mayastor
    cluster.composer().pause(mayastor.as_str()).await.unwrap();

    let _ = rep_client
        .share(
            &ShareReplica::from(&replica),
            Some(Context::new(bus_timeout_opts())),
        )
        .await
        .expect_err("mayastor down");

    check_operation(&replica, Protocol::None).await;

    // unpause mayastor
    cluster.composer().thaw(mayastor.as_str()).await.unwrap();

    // now it should be shared successfully
    let uri = rep_client
        .share(&ShareReplica::from(&replica), None)
        .await
        .unwrap();
    println!("Share uri: {}", uri);

    cluster.composer().pause(mayastor.as_str()).await.unwrap();

    let _ = rep_client
        .unshare(
            &UnshareReplica::from(&replica),
            Some(Context::new(bus_timeout_opts())),
        )
        .await
        .expect_err("mayastor down");

    check_operation(&replica, Protocol::Nvmf).await;

    cluster.composer().thaw(mayastor.as_str()).await.unwrap();

    let _ = rep_client
        .unshare(&UnshareReplica::from(&replica), None)
        .await
        .unwrap();

    assert_eq!(replica_spec(&replica).await.unwrap().share, Protocol::None);
}

/// Tests Store Write Failures for Replica Operations
/// As it stands, the tests expects the operation to not be undone, and
/// a reconcile thread should eventually sync the specs when the store reappears
async fn replica_op_transaction_store(
    replica: &Replica,
    cluster: &Cluster,
    (store_timeout, reconcile_period, grpc_timeout): (Duration, Duration, Duration),
    protocol: Protocol,
    share: Option<ShareReplica>,
    unshare: Option<UnshareReplica>,
) {
    let mayastor = cluster.node(0);

    // pause mayastor
    cluster.composer().pause(mayastor.as_str()).await.unwrap();

    let rep_client = cluster.grpc_client().replica();

    if share.clone().is_some() {
        rep_client
            .share(&share.as_ref().unwrap().clone(), None)
            .await
            .expect_err("mayastor down");
    }
    if unshare.clone().is_some() {
        rep_client
            .unshare(&unshare.as_ref().unwrap().clone(), None)
            .await
            .expect_err("mayastor down");
    }

    // ensure the share will succeed but etcd store will fail
    // by pausing etcd and releasing mayastor
    cluster.composer().pause("etcd").await.unwrap();
    cluster.composer().thaw(mayastor.as_str()).await.unwrap();

    // hopefully we have enough time before the store times out
    let spec = replica_spec(replica).await.unwrap();
    assert!(spec.operation.unwrap().result.is_none());

    // let the store write time out
    tokio::time::sleep(grpc_timeout + store_timeout).await;

    // and now we have a result but the operation is still pending until
    // we can sync the spec
    let spec = replica_spec(replica).await.unwrap();
    assert!(spec.operation.unwrap().result.is_some());

    // thaw etcd allowing the worker thread to sync the "dirty" spec
    cluster.composer().thaw("etcd").await.unwrap();

    // wait for the reconciler to do its thing
    tokio::time::sleep(reconcile_period * 2).await;

    // and now we've sync and the pending operation is no more
    let spec = replica_spec(replica).await.unwrap();
    assert!(spec.operation.is_none() && spec.share == protocol);

    if share.clone().is_some() {
        rep_client
            .share(&share.as_ref().unwrap().clone(), None)
            .await
            .expect_err("already done");
    }
    if unshare.clone().is_some() {
        rep_client
            .unshare(&unshare.as_ref().unwrap().clone(), None)
            .await
            .expect_err("already done");
    }
}

/// Tests replica share and unshare operations when the store is temporarily down
#[tokio::test]
async fn replica_transaction_store() {
    let store_timeout = Duration::from_millis(250);
    let reconcile_period = Duration::from_millis(250);
    let grpc_timeout = Duration::from_millis(350);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_bus_timeouts(bus_timeout_opts())
        .build()
        .await
        .unwrap();
    let rep_client = cluster.grpc_client().replica();
    let mayastor = cluster.node(0);

    let replica = rep_client
        .create(
            &CreateReplica {
                node: mayastor.clone(),
                uuid: ReplicaId::new(),
                pool: cluster.pool(0, 0),
                size: 12582912,
                thin: false,
                share: Protocol::None,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    replica_op_transaction_store(
        &replica,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        Protocol::Nvmf,
        Some(ShareReplica::from(&replica)),
        None,
    )
    .await;

    replica_op_transaction_store(
        &replica,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        Protocol::None,
        None,
        Some(UnshareReplica::from(&replica)),
    )
    .await;
}

const RECONCILE_TIMEOUT_SECS: u64 = 7;
const POOL_FILE_NAME: &str = "disk1.img";
const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Creates a pool on a mayastor instance, which will have both spec and state.
/// Stops/Kills the mayastor container. At some point we will have no pool state, because the node
/// is gone. We then restart the node and the pool reconciler will then recreate the pool! At this
/// point, we'll have a state again.
#[tokio::test]
async fn reconciler_missing_pool_state() {
    let disk = testlib::TmpDiskFile::new(POOL_FILE_NAME, POOL_SIZE_BYTES);

    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(1)
        .with_pool(0, disk.uri())
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let client = cluster.rest_v00();
    let pools_api = client.pools_api();
    let volumes_api = client.volumes_api();

    // create volume to fill up some of the pool space
    for _ in 0 .. 10 {
        let body = CreateVolumeBody::new(VolumePolicy::default(), 1, 8388608u64);
        let volume = VolumeId::new();
        volumes_api.put_volume(&volume, body).await.unwrap();
    }
    let replicas = client.replicas_api().get_replicas().await.unwrap();

    let pool = pools_api
        .get_pool(cluster.pool(0, 0).as_str())
        .await
        .unwrap();
    tracing::info!("Pool: {:#?}", pool);

    assert!(pool.spec.is_some());
    assert!(pool.state.is_some());

    let maya = cluster.node(0);
    async fn pool_checker(cluster: &Cluster, state: Option<&PoolState>) {
        let maya = cluster.node(0);

        let pool = wait_till_pool_state(cluster, (0, 0), false).await;
        assert!(pool.state.is_none());

        cluster.composer().start(maya.as_str()).await.unwrap();
        let pool = wait_till_pool_state(cluster, (0, 0), true).await;
        // the state should be the same as it was before
        assert_eq!(pool.state.as_ref(), state);
    }

    // let's stop the mayastor container, gracefully
    cluster.composer().stop(maya.as_str()).await.unwrap();
    pool_checker(&cluster, pool.state.as_ref()).await;

    // now kill it, so there's no deregistration message
    cluster.composer().kill(maya.as_str()).await.unwrap();
    pool_checker(&cluster, pool.state.as_ref()).await;

    // we should have also "imported" the same replicas, perhaps in a different order...
    let current_replicas = client.replicas_api().get_replicas().await.unwrap();
    assert_eq!(
        replicas
            .iter()
            .sorted_by(|a, b| a.uuid.cmp(&b.uuid))
            .collect::<Vec<_>>(),
        current_replicas
            .iter()
            .sorted_by(|a, b| a.uuid.cmp(&b.uuid))
            .collect::<Vec<_>>()
    );
}

/// Wait until the specified pool state option presence matches the `has_state` flag
async fn wait_till_pool_state(cluster: &Cluster, pool: (u32, u32), has_state: bool) -> Pool {
    let pool_id = cluster.pool(pool.0, pool.1);
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let client = cluster.rest_v00();
    let pools_api = client.pools_api();
    let start = std::time::Instant::now();
    loop {
        let pool = pools_api.get_pool(pool_id.as_str()).await.unwrap();

        if pool.state.is_some() == has_state {
            return pool;
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the pool to have 'has_state': '{}'. Pool: '{:#?}'",
                has_state, pool
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Creates a cluster with two nodes, two pools, core and rest with reconciler period 1 for both
/// busy and idle. Kills node 1 and deletes the pool on it. Somehow the pool is struck in Deleting
/// state. Now the node 1 is brought back and the reconciler Deletes the pool in Deleting state so
/// that pools with same spec can be created.
#[tokio::test]
async fn reconciler_deleting_pool_on_node_down() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(2)
        .with_pools(2)
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let pool_1_id = cluster.pool(0, 0);
    let pool_2_id = cluster.pool(1, 1);
    let node_1_id = cluster.node(0);
    let client = cluster.rest_v00();
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let pools_api = client.pools_api();
    let start = std::time::Instant::now();

    // Kill the mayastor node 1
    cluster.composer().kill(node_1_id.as_str()).await.unwrap();

    // Delete the pool on the mayastor node 1
    let _ = cluster
        .rest_v00()
        .pools_api()
        .del_node_pool(node_1_id.as_str(), pool_1_id.as_str())
        .await;

    let pool_1_status_after_delete = pools_api
        .get_pool(pool_1_id.as_str())
        .await
        .unwrap()
        .spec
        .unwrap()
        .status
        .to_string();

    let pool_2_status = pools_api
        .get_pool(pool_2_id.as_str())
        .await
        .unwrap()
        .spec
        .unwrap()
        .status
        .to_string();

    // The below infers only one node is down and one pool is in Deleting state
    // and the other pools are unaffected.
    assert_eq!(pool_1_status_after_delete, "Deleting");
    assert_eq!(pool_2_status, "Created");

    // Start the node once again
    cluster.composer().start(node_1_id.as_str()).await.unwrap();

    // The reconciler would delete the pool in Deleting state.
    loop {
        match pools_api.get_pool(pool_1_id.as_str()).await {
            Ok(_) => {}
            Err(err) => {
                if let Error::Response(err) = err {
                    if err.status() == StatusCode::NOT_FOUND {
                        break;
                    }
                }
            }
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!("Timeout waiting for the pool delete");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The other pools are unaffected by reconciler action
    let pool_2_status_after_reconciler_action = pools_api
        .get_pool(pool_2_id.as_str())
        .await
        .unwrap()
        .spec
        .unwrap()
        .status
        .to_string();
    assert_eq!(pool_2_status_after_reconciler_action, "Created");
}
