mod purge;

use anyhow::{anyhow, Result};
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::{
    context::Context,
    operations::{
        node::traits::NodeOperations, pool::traits::PoolOperations,
        registry::traits::RegistryOperations, replica::traits::ReplicaOperations,
        volume::traits::VolumeOperations,
    },
};
use itertools::Itertools;
use std::{collections::HashMap, convert::TryFrom, thread::sleep, time::Duration};
use stor_port::{
    pstor::{etcd::Etcd, StoreObj},
    transport_api::{ReplyError, ReplyErrorKind, ResourceKind, TimeoutOptions},
    types::v0::{
        openapi::{
            apis::StatusCode,
            clients::tower::Error,
            models::{
                self, CreateReplicaBody, CreateVolumeBody, Pool, PoolState, PublishVolumeBody,
                VolumePolicy,
            },
        },
        store::{
            pool::{Encryption, EncryptionSecret, PoolLabel},
            replica::{ReplicaSpec, ReplicaSpecKey},
        },
        transport::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, ExpandPool, Filter,
            GetBlockDevices, GetSpecs, NexusId, NodeId, NodeRscCounts, NodeStatus, PoolErrorCode,
            Protocol, Replica, ReplicaId, ReplicaName, ReplicaOwners, ReplicaShareProtocol,
            ReplicaStatus, ShareReplica, UnshareReplica, Volume, VolumeId,
        },
    },
};

#[tokio::test]
async fn pool() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let pool_client = cluster.grpc_client().pool();
    let rep_client = cluster.grpc_client().replica();

    let io_engine = cluster.node(0);
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    let node = nodes.0.first().unwrap();
    assert_eq!(
        node.tallies(),
        Some(&NodeRscCounts {
            pool_count: 0,
            replica_count: 0,
            snapshot_count: 0,
        })
    );

    let pool = pool_client
        .create(
            &CreatePool {
                node: io_engine.clone(),
                id: "pooloop".into(),
                disks: vec!["malloc:///disk0?size_mb=100".into()],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(0));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(0));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(0)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(0)
    );
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    let node = nodes.0.first().unwrap();
    assert_eq!(
        node.tallies(),
        Some(&NodeRscCounts {
            pool_count: 1,
            replica_count: 0,
            snapshot_count: 0,
        })
    );

    let _pool2 = pool_client
        .create(
            &CreatePool {
                node: io_engine.clone(),
                id: "pooloop2".into(),
                disks: vec!["malloc:///disk1?size_mb=100".into()],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: None,
            },
            None,
        )
        .await
        .unwrap();

    tracing::info!("Pools: {:?}", pool);
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    let node = nodes.0.first().unwrap();
    assert_eq!(
        node.tallies(),
        Some(&NodeRscCounts {
            pool_count: 2,
            replica_count: 0,
            snapshot_count: 0,
        })
    );

    let pools = pool_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Pools: {:?}", pools);

    let replica = rep_client
        .create(
            &CreateReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                entity_id: None,
                pool_id: "pooloop".into(),
                pool_uuid: None,
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
    let pools = pool_client
        .get(Filter::Pool(replica.pool_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(1));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(0));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(1)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(0)
    );
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    let node = nodes.0.first().unwrap();
    assert_eq!(
        node.tallies(),
        Some(&NodeRscCounts {
            pool_count: 2,
            replica_count: 1,
            snapshot_count: 0,
        })
    );

    let replicas = rep_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Replicas: {:?}", replicas);

    let uri = replica.uri.clone();
    assert_eq!(
        replica,
        Replica {
            node: io_engine.clone(),
            name: ReplicaName::from("cf36a440-74c6-4042-b16c-4f7eddfc24da"),
            uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
            entity_id: None,
            pool_id: "pooloop".into(),
            pool_uuid: replica.pool_uuid.clone(),
            thin: true,
            size: 12582912,
            space: replica.space.clone(),
            share: Protocol::None,
            uri,
            status: ReplicaStatus::Online,
            allowed_hosts: vec![],
            kind: Default::default(),
            encrypted: Some(false),
        }
    );

    let uri = rep_client
        .share(
            &ShareReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool_id: "pooloop".into(),
                pool_uuid: None,
                protocol: ReplicaShareProtocol::Nvmf,
                name: None,
                allowed_hosts: vec![],
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
                node: io_engine.clone(),
                id: "pooloop".into(),
                ..Default::default()
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

    let error = rep_client
        .destroy(
            &DestroyReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool_id: "pooloop2".into(),
                pool_uuid: None,
                name: None,
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("wrong pool");
    assert_eq!(error.kind, ReplyErrorKind::Aborted);

    rep_client
        .destroy(
            &DestroyReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool_id: "pooloop".into(),
                pool_uuid: None,
                name: None,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let pools = pool_client
        .get(Filter::Pool(replica.pool_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    let pool = pools.first().unwrap();
    assert_eq!(pool.state.as_ref().unwrap().repl_count, Some(0));
    assert_eq!(pool.state.as_ref().unwrap().snap_count, Some(0));
    assert_eq!(
        pool.config.as_ref().unwrap().definition.replica_count,
        Some(0)
    );
    assert_eq!(
        pool.config.as_ref().unwrap().definition.snapshot_count,
        Some(0)
    );
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    let node = nodes.0.first().unwrap();
    assert_eq!(
        node.tallies(),
        Some(&NodeRscCounts {
            pool_count: 2,
            replica_count: 0,
            snapshot_count: 0,
        })
    );

    let error = rep_client
        .destroy(
            &DestroyReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool_id: "pooloop".into(),
                pool_uuid: None,
                name: None,
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("already deleted");
    assert_eq!(error.kind, ReplyErrorKind::NotFound);

    assert!(rep_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .0
        .is_empty());

    pool_client
        .destroy(
            &DestroyPool {
                node: io_engine.clone(),
                id: "pooloop".into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    pool_client
        .destroy(
            &DestroyPool {
                node: io_engine.clone(),
                id: "pooloop2".into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    let node = nodes.0.first().unwrap();
    assert_eq!(
        node.tallies(),
        Some(&NodeRscCounts {
            pool_count: 0,
            replica_count: 0,
            snapshot_count: 0,
        })
    );

    let error = rep_client
        .destroy(
            &DestroyReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::try_from("cf36a440-74c6-4042-b16c-4f7eddfc24da").unwrap(),
                pool_id: "pooloop".into(),
                pool_uuid: None,
                name: None,
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("pool not loaded");
    assert_eq!(error.kind, ReplyErrorKind::FailedPrecondition);

    assert!(pool_client
        .get(Filter::None, None)
        .await
        .unwrap()
        .0
        .is_empty());
}

/// Creates two pool on a temps fs files using max_expansion arg.
/// Validates that disk_capacity is equal to the underlying disk capacity.
/// TODO: Add logic to assert max_expandable_size and grow functionality once changes are in.
#[tokio::test]
async fn pool_expansion() {
    let pool_disk_one = deployer_cluster::TmpDiskFile::new(POOL_FILE_NAME, TEN_GIB_BYTES);
    let pool_disk_two = deployer_cluster::TmpDiskFile::new(POOL_FILE_NAME_2, TEN_GIB_BYTES);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_options(|p| {
            p.with_io_engine_devices(vec![pool_disk_one.path(), pool_disk_two.path()])
        })
        .build()
        .await
        .unwrap();
    let pool_client = cluster.grpc_client().pool();
    let pool_one = pool_client
        .create(
            &CreatePool {
                node: cluster.node(0),
                id: "pool-1".into(),
                disks: vec![pool_disk_one.path().into()],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: Some("30x".to_string()),
            },
            None,
        )
        .await
        .unwrap();
    let disk_capacity_one = pool_one.state().unwrap().disk_capacity.unwrap();
    let pool_two = pool_client
        .create(
            &CreatePool {
                node: cluster.node(0),
                id: "pool-2".into(),
                disks: vec![pool_disk_two.path().into()],
                labels: None,
                encryption: None,
                cluster_size: None,
                max_expansion: Some("300GiB".to_string()),
            },
            None,
        )
        .await
        .unwrap();
    let disk_capacity_two = pool_two.state().unwrap().disk_capacity.unwrap();
    assert_eq!(
        TEN_GIB_BYTES, disk_capacity_one,
        "disk capacity doesnt match with underlying disk capacity"
    );
    assert_eq!(
        TEN_GIB_BYTES, disk_capacity_two,
        "disk capacity doesnt match with underlying disk capacity"
    );
    let capacity_before_pool_one = pool_one.state().unwrap().capacity;
    let capacity_before_pool_two = pool_two.state().unwrap().capacity;
    // TODO: expand till maximum expandable size when mayastor cherry-pick merges.
    // Here we extend device by cluster size.
    let max_expandable_pool_two = pool_two.state().unwrap().max_expandable_size.unwrap();
    let expand_by_two = max_expandable_pool_two.saturating_sub(disk_capacity_two);
    let max_expandable_pool_one = pool_one.state().unwrap().max_expandable_size.unwrap();
    let expand_by_one = max_expandable_pool_one.saturating_sub(disk_capacity_one);
    let _ = pool_disk_one.clone().expand(expand_by_one);
    let _ = pool_disk_two.clone().expand(expand_by_two);
    let expand_request_one = ExpandPool {
        id: "pool-1".into(),
    };
    let expand_request_two = ExpandPool {
        id: "pool-2".into(),
    };
    let pool_one = pool_client.expand(&expand_request_one).await.unwrap();
    let pool_two = pool_client.expand(&expand_request_two).await.unwrap();
    let capacity_after_pool_one = pool_one.state().unwrap().capacity;
    let capacity_after_pool_two = pool_two.state().unwrap().capacity;
    assert!(
        capacity_after_pool_one > capacity_before_pool_one,
        "pool-1 capacity did not increase as expected"
    );
    assert!(
        capacity_after_pool_two > capacity_before_pool_two,
        "pool-2 capacity did not increase as expected"
    );
    // Attempt expand without extending the underlying device.
    // Should result in FailedPrecondition.
    if let Err(e) = pool_client.expand(&expand_request_one).await {
        assert_eq!(e.kind, ReplyErrorKind::DiskNotExtended);
    }
    if let Err(e) = pool_client.expand(&expand_request_two).await {
        assert_eq!(e.kind, ReplyErrorKind::DiskNotExtended);
    }
    // Attempt expand after extending the device 1 cluster more then max_expandable_size.
    // Should result in OutOfRange. max_expandable_size is absolute limit.
    let _ = pool_disk_one.expand(4194304);
    let _ = pool_disk_two.expand(4194304);
    if let Err(e) = pool_client.expand(&expand_request_one).await {
        assert_eq!(e.kind, ReplyErrorKind::OutOfRange);
    }
    if let Err(e) = pool_client.expand(&expand_request_two).await {
        assert_eq!(e.kind, ReplyErrorKind::OutOfRange);
    }
}

// Tests creation and import of pool with larger blobstore cluster size.
// Create a few replicas, restart the node. Pool should import again without issue.
#[tokio::test]
async fn pool_larger_cluster_size() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_options(|o| o.with_pool_cluster_size(Some(POOL_BS_CLUSTER_SIZE as u32)))
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let pool_client = cluster.grpc_client().pool();
    let rep_client = cluster.grpc_client().replica();

    let io_engine = cluster.node(0);
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);
    let pools = pool_client.get(Filter::None, None).await.unwrap();
    let poolid = pools.0[0].id();

    for repl_idx in 1..=2 {
        let _ = rep_client
            .create(
                &CreateReplica {
                    node: io_engine.clone(),
                    uuid: ReplicaId::new(),
                    entity_id: None,
                    pool_id: poolid.clone(),
                    pool_uuid: None,
                    size: 52428800, // 50MiB. Actual will be 64MiB(2 clusters)
                    thin: repl_idx % 2 == 0,
                    share: Protocol::None,
                    name: None,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
    }

    cluster.composer().stop("io-engine-1").await.unwrap();
    cluster
        .wait_node_status(NodeId::from("io-engine-1"), NodeStatus::Offline)
        .await
        .unwrap();
    cluster.composer().start("io-engine-1").await.unwrap();
    cluster.wait_pool_online(poolid.clone()).await.unwrap();
    let replicas = rep_client.get(Filter::None, None).await.unwrap();
    replicas.into_inner().iter().for_each(|r| {
        assert!(r.online());
        assert_eq!(r.space.as_ref().unwrap().cluster_size, POOL_BS_CLUSTER_SIZE);
    });
}

// Create multiple pools, having different cluster sizes. Create volumes with varying cluster size
// requirements. Assert that the replicas are placed as expected on matching pools.
#[tokio::test]
async fn volume_repl_placement_with_cluster_size() {
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_reconcile_period(Duration::from_secs(10), Duration::from_secs(10))
        .build()
        .await
        .unwrap();

    let client = cluster.grpc_client();
    let rest_client = cluster.rest_v00();
    let volume_client = cluster.grpc_client().volume();
    let volumes_api = rest_client.volumes_api();

    let pool_4m_1 = CreatePool {
        node: cluster.node(0),
        id: "pool_cs_4m_1".into(),
        disks: vec!["malloc:///disk0?size_mb=200".into()],
        labels: None,
        encryption: None,
        cluster_size: None,
        max_expansion: None,
    };

    let pool_32m_1 = CreatePool {
        node: cluster.node(1),
        id: "pool_cs_32m_1".into(),
        disks: vec!["malloc:///disk1?size_mb=200".into()],
        labels: None,
        encryption: None,
        cluster_size: Some(33554432),
        max_expansion: None,
    };

    let pool_32m_2 = CreatePool {
        node: cluster.node(2),
        id: "pool_cs_32m_2".into(),
        disks: vec!["malloc:///disk2?size_mb=200".into()],
        labels: None,
        encryption: None,
        cluster_size: Some(33554432),
        max_expansion: None,
    };

    let _ = client.pool().create(&pool_4m_1, None).await.unwrap();
    let _ = client.pool().create(&pool_32m_1, None).await.unwrap();
    let _ = client.pool().create(&pool_32m_2, None).await.unwrap();
    // Create two volumes. Expect replica placement on 32MiB cluster sized pools.
    // For each volume, validate the cluster size of chosen pool by looking into replica.
    for _ in 0..2 {
        let body = CreateVolumeBody::new_all(
            VolumePolicy::default(),
            1,
            62914560u64,
            false,
            models::Topology::new(),
            HashMap::new(),
            None,
            Some(10),
            false,
            Some(33554432),
        );
        let volume = VolumeId::new();
        volumes_api.put_volume(&volume, body).await.unwrap();
        let vol = volume_client
            .get(Filter::Volume(volume), false, None, None)
            .await
            .unwrap();
        assert_eq!(vol.entries.len(), 1);
        validate_vol_repl_cluster_size(&cluster, &vol.entries[0], 33554432)
            .await
            .unwrap();
    }

    // Create one more volume. Expect replica placement on 4MiB cluster sized pool.
    // For this volume also, validate the cluster size of chosen pool by looking into replica.
    let body = CreateVolumeBody::new(VolumePolicy::default(), 1, 62914560u64, false, false);
    let volume = VolumeId::new();
    volumes_api.put_volume(&volume, body).await.unwrap();
    let vol = volume_client
        .get(Filter::Volume(volume), false, None, None)
        .await
        .unwrap();
    assert_eq!(vol.entries.len(), 1);
    validate_vol_repl_cluster_size(&cluster, &vol.entries[0], 4194304)
        .await
        .unwrap();
}

pub(crate) async fn validate_vol_repl_cluster_size(
    cluster: &Cluster,
    volume: &Volume,
    expected_cluster_size: u64,
) -> Result<()> {
    let vstate: Vec<_> = volume.state().replica_topology.keys().cloned().collect();
    for r in vstate {
        let repl_client = cluster.grpc_client().replica();
        let hdl = tokio::spawn(async move {
            println!("Validate replica {r} cluster size to be {expected_cluster_size}");
            let repl = repl_client
                .get(Filter::Replica(r.clone()), None)
                .await
                .unwrap();
            assert_eq!(repl.0.len(), 1);
            let repl_cluster_size = repl.0[0].space.as_ref().unwrap().cluster_size;
            if repl_cluster_size != expected_cluster_size {
                return Err(anyhow!(
                    "Replica {r} has cluster_size {}, expected {}",
                    repl_cluster_size,
                    expected_cluster_size
                ));
            }
            Ok(())
        });
        hdl.await.unwrap()?
    }
    Ok(())
}
/// The tests below revolve around transactions and are dependent on the core agent's command line
/// arguments for timeouts.
/// This is required because as of now, we don't have a good mocking strategy
///
/// default timeout options for every rpc request
fn grpc_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::default()
        .with_max_retries(0)
        .with_req_timeout(Duration::from_millis(250))
}

/// Get the replica spec
async fn replica_spec(replica: &Replica, client: &dyn RegistryOperations) -> Option<ReplicaSpec> {
    client
        .get_specs(&GetSpecs {}, None)
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
        .with_req_timeouts(Duration::from_millis(250), Duration::from_millis(500))
        .with_grpc_timeouts(grpc_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);

    let registry_client = cluster.grpc_client().registry();
    let node_client = cluster.grpc_client().node();
    let pool_client = cluster.grpc_client().pool();
    let rep_client = cluster.grpc_client().replica();

    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let pools = pool_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Pools: {:?}", pools);

    let replica = rep_client
        .create(
            &CreateReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::new(),
                pool_id: cluster.pool(0, 0),
                pool_uuid: None,
                size: 12582912,
                thin: false,
                share: Protocol::None,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    async fn check_operation(
        replica: &Replica,
        protocol: Protocol,
        registry_client: &dyn RegistryOperations,
    ) {
        // operation in progress
        assert!(replica_spec(replica, registry_client)
            .await
            .unwrap()
            .operation
            .is_some());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // operation is completed
        assert!(replica_spec(replica, registry_client)
            .await
            .unwrap()
            .operation
            .is_none());
        assert_eq!(
            replica_spec(replica, registry_client).await.unwrap().share,
            protocol
        );
    }

    // pause io_engine
    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    let _ = rep_client
        .share(
            &ShareReplica::from(&replica),
            Some(Context::new(grpc_timeout_opts())),
        )
        .await
        .expect_err("io_engine down");

    check_operation(&replica, Protocol::None, &registry_client).await;

    // unpause io_engine
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    // now it should be shared successfully
    let uri = rep_client
        .share(&ShareReplica::from(&replica), None)
        .await
        .unwrap();
    println!("Share uri: {uri}");

    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    let _ = rep_client
        .unshare(
            &UnshareReplica::from(&replica),
            Some(Context::new(grpc_timeout_opts())),
        )
        .await
        .expect_err("io_engine down");

    check_operation(&replica, Protocol::Nvmf, &registry_client).await;

    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    rep_client
        .unshare(&UnshareReplica::from(&replica), None)
        .await
        .unwrap();

    assert_eq!(
        replica_spec(&replica, &registry_client)
            .await
            .unwrap()
            .share,
        Protocol::None
    );
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
    let io_engine = cluster.node(0);

    // pause io_engine
    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    let rep_client = cluster.grpc_client().replica();
    let registry_client = cluster.grpc_client().registry();

    if share.clone().is_some() {
        rep_client
            .share(&share.as_ref().unwrap().clone(), None)
            .await
            .expect_err("io_engine down");
    }
    if unshare.clone().is_some() {
        rep_client
            .unshare(&unshare.as_ref().unwrap().clone(), None)
            .await
            .expect_err("io_engine down");
    }

    // ensure the share will succeed but etcd store will fail
    // by pausing etcd and releasing the io_engine
    cluster.composer().pause("etcd").await.unwrap();
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    // hopefully we have enough time before the store times out
    let spec = replica_spec(replica, &registry_client).await.unwrap();
    assert!(spec.operation.unwrap().result.is_none());

    // let the store write time out
    tokio::time::sleep(grpc_timeout + store_timeout).await;

    // and now we have a result but the operation is still pending until
    // we can sync the spec
    let spec = replica_spec(replica, &registry_client).await.unwrap();
    assert!(spec.operation.unwrap().result.is_some());

    // thaw etcd allowing the worker thread to sync the "dirty" spec
    cluster.composer().thaw("etcd").await.unwrap();

    // wait for the reconciler to do its thing
    tokio::time::sleep(reconcile_period * 2).await;

    // and now we've sync and the pending operation is no more
    let spec = replica_spec(replica, &registry_client).await.unwrap();
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
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_grpc_timeouts(grpc_timeout_opts())
        .build()
        .await
        .unwrap();
    let rep_client = cluster.grpc_client().replica();
    let io_engine = cluster.node(0);

    let replica = rep_client
        .create(
            &CreateReplica {
                node: io_engine.clone(),
                uuid: ReplicaId::new(),
                pool_id: cluster.pool(0, 0),
                pool_uuid: None,
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
const POOL_FILE_NAME_2: &str = "disk2.img";
const POOL_SIZE_BYTES: u64 = 200 * 1024 * 1024;
const POOL_BS_CLUSTER_SIZE: u64 = 33554432;
const TEN_GIB_BYTES: u64 = 10737418240;

/// Creates a pool on a io_engine instance, which will have both spec and state.
/// Stops/Kills the io_engine container. At some point we will have no pool state, because the node
/// is gone. We then restart the node and the pool reconciler will then recreate the pool! At this
/// point, we'll have a state again.
#[tokio::test]
async fn reconciler_missing_pool_state() {
    let disk = deployer_cluster::TmpDiskFile::new(POOL_FILE_NAME, POOL_SIZE_BYTES);

    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_pool(0, disk.uri())
        .with_cache_period("100ms")
        .with_node_deadline("100ms")
        .with_reconcile_period(Duration::from_millis(200), Duration::from_millis(1))
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();

    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let client = cluster.rest_v00();
    let pools_api = client.pools_api();
    let volumes_api = client.volumes_api();

    // create volume to fill up some of the pool space
    for _ in 0..10 {
        let body = CreateVolumeBody::new(VolumePolicy::default(), 1, 8388608u64, false, false);
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
        let tm = Duration::from_secs(1);

        let pool = wait_till_pool_state(cluster, (0, 0), false, tm).await;
        assert!(pool.state.is_none());

        cluster.composer().restart(maya.as_str()).await.unwrap();
        let pool = wait_till_pool_state(cluster, (0, 0), state.is_some(), tm).await;
        // the state should be the same as it was before
        assert_eq!(pool.state.as_ref(), state);
    }

    // let's stop the io_engine container, gracefully
    cluster.composer().stop(maya.as_str()).await.unwrap();
    pool_checker(&cluster, pool.state.as_ref()).await;

    // now kill it, so there's no deregistration message
    cluster.composer().kill(maya.as_str()).await.unwrap();

    // move pool disk to another location and replace it with another disk.
    // this means import should fail as we cannot import from that disk!
    assert_ne!(POOL_FILE_NAME, POOL_FILE_NAME_2);
    let mut disk = disk.into_inner().unwrap();
    disk.rename(POOL_FILE_NAME_2).unwrap();

    let new_disk = deployer_cluster::TmpDiskFile::new(POOL_FILE_NAME, POOL_SIZE_BYTES);
    pool_checker(&cluster, None).await;

    let pool_client = cluster.grpc_client().pool();
    let pools = pool_client
        .get(Filter::Pool(pool.id.clone().into()), None)
        .await
        .unwrap();
    tracing::info!("Pools: {:?}", pools);

    let hpool = pools.0.first().unwrap();
    let pool_diag = hpool.diag().unwrap();
    assert_eq!(pool_diag.import_errors.len(), 1);
    let error = pool_diag.import_errors.first().unwrap();
    assert_eq!(error.error.code, PoolErrorCode::InvalidSuperBlock);

    cluster.composer().kill(maya.as_str()).await.unwrap();

    // move original disk back and now import should succeed!
    drop(new_disk);
    disk.rename(POOL_FILE_NAME).unwrap();

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

    cluster.composer().kill(maya.as_str()).await.unwrap();

    // move pool disk to another location
    // this means import should fail with not found error
    assert_ne!(POOL_FILE_NAME, POOL_FILE_NAME_2);
    disk.rename(POOL_FILE_NAME_2).unwrap();

    pool_checker(&cluster, None).await;

    let pool_client = cluster.grpc_client().pool();
    let pools = pool_client
        .get(Filter::Pool(pool.id.clone().into()), None)
        .await
        .unwrap();
    tracing::info!("Pools: {:?}", pools);

    let hpool = pools.0.first().unwrap();
    let pool_diag = hpool.diag().unwrap();
    assert_eq!(pool_diag.import_errors.len(), 1);
    let error = pool_diag.import_errors.first().unwrap();
    assert_eq!(error.error.code, PoolErrorCode::DiskNotFound);
}

/// Wait until the specified pool state option presence matches the `has_state` flag
async fn wait_till_pool_state(
    cluster: &Cluster,
    pool: (u32, u32),
    has_state: bool,
    timeout: Duration,
) -> Pool {
    let pool_id = cluster.pool(pool.0, pool.1);
    let client = cluster.rest_v00();
    let pools_api = client.pools_api();
    let start = std::time::Instant::now();
    loop {
        let pool = pools_api.get_pool(pool_id.as_str()).await.unwrap();

        if has_state && pool.state.is_some() {
            return pool;
        }

        if std::time::Instant::now() > (start + timeout) {
            if !has_state && pool.state.is_none() {
                return pool;
            }
            panic!(
                "Timeout waiting for the pool to have 'has_state': '{has_state}'. Pool: '{pool:#?}'"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Creates a cluster with two nodes, two pools, core and rest with reconciler period 1 for both
/// busy and idle. Kills node 1 and deletes the pool on it. Somehow the pool is struck in Deleting
/// state. Now the node 1 is brought back and the reconciler Deletes the pool in Deleting state so
/// that pools with same spec can be created.
#[tokio::test]
async fn reconciler_deleting_pool_on_node_down() {
    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;

    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(2)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let pool_1_id = cluster.pool(0, 0);
    let pool_2_id = cluster.pool(1, 1);
    let node_1_id = cluster.node(0);
    let client = cluster.rest_v00();
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let pools_api = client.pools_api();
    let start = std::time::Instant::now();

    // Kill the io_engine node 1
    cluster.composer().kill(node_1_id.as_str()).await.unwrap();

    // Delete the pool on the io_engine node 1
    let _ = cluster
        .rest_v00()
        .pools_api()
        .del_node_pool(node_1_id.as_str(), pool_1_id.as_str(), None)
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

/// Tests that resources in the deleting state are eventually deleted
#[tokio::test]
async fn reconciler_deleting_dirty_pool() {
    let reconcile_period = Duration::from_millis(250);
    let grpc_timeout = TimeoutOptions::default()
        .with_max_retries(0)
        .with_req_timeout(Duration::from_millis(250))
        .with_min_req_timeout(None);
    let req_timeout = grpc_timeout.base_timeout() * 2;
    let store_timeout = Duration::from_millis(300);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        .with_req_timeouts(req_timeout, req_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_grpc_timeouts(grpc_timeout.clone())
        .build()
        .await
        .unwrap();
    let node = cluster.node(0);
    let pool = cluster.pool(0, 0);

    let node_client = cluster.grpc_client().node();
    let pool_client = cluster.grpc_client().pool();

    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let pools = pool_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Pools: {:?}", pools);

    // 1. Pause the node, so the destroy call will timeout
    cluster.composer().pause(node.as_str()).await.unwrap();

    let _ = pool_client
        .destroy(
            &DestroyPool {
                node: node.clone(),
                id: pool.clone(),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("timeout since the node is down");

    // 2. Pause ETCD so we fail to undo the operation
    cluster.composer().pause("etcd").await.unwrap();

    tokio::time::sleep(req_timeout - grpc_timeout.base_timeout()).await;

    // 3. Bring the node back so we can delete the pool
    cluster.composer().thaw(node.as_str()).await.unwrap();

    // 4. allow for the store write to time out (plus some slack)
    tokio::time::sleep(store_timeout * 2).await;

    // 5. Bring ETCD back up so we can resume operations
    cluster.composer().thaw("etcd").await.unwrap();

    // 6. The pool should "eventually" be deleted
    wait_pool_deleted(&cluster, node, reconcile_period * 4).await;

    async fn wait_pool_deleted(cluster: &Cluster, node: NodeId, timeout: Duration) {
        let pool_client = cluster.grpc_client().pool();
        let start = std::time::Instant::now();
        loop {
            let pools = pool_client
                .get(Filter::Node(node.clone()), None)
                .await
                .unwrap();
            let pools = pools.into_inner();

            if pools.is_empty() {
                return;
            }

            if std::time::Instant::now() > (start + timeout) {
                panic!("Timeout waiting for the pool to be deleted. Actual: '{pools:#?}'");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[tokio::test]
async fn disown_unused_replicas() {
    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
    let reconcile_period = Duration::from_millis(200);
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("1s")
        .with_reconcile_period(reconcile_period, reconcile_period)
        .build()
        .await
        .unwrap();

    let rest_api = cluster.rest_v00();
    let volumes_api = rest_api.volumes_api();
    let node = cluster.node(0).to_string();

    let volume = volumes_api
        .put_volume(
            &"1e3cf927-80c2-47a8-adf0-95c481bdd7b7".parse().unwrap(),
            models::CreateVolumeBody::new(
                models::VolumePolicy::default(),
                1,
                5242880u64,
                false,
                false,
            ),
        )
        .await
        .unwrap();

    let volume = volumes_api
        .put_volume_target(
            &volume.spec.uuid,
            PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                node.clone().to_string(),
                models::VolumeShareProtocol::Nvmf,
                None,
                cluster.csi_node(0),
                None,
            ),
        )
        .await
        .unwrap();

    cluster.composer().pause(&node).await.unwrap();
    volumes_api
        .del_volume_target(
            &volume.spec.uuid,
            Some(false),
            Some(cluster.csi_node(0).as_str()),
        )
        .await
        .expect_err("io-engine is down");
    cluster.composer().kill(&node).await.unwrap();

    let volume = volumes_api.get_volume(&volume.spec.uuid).await.unwrap();
    tracing::info!("Volume: {:?}", volume);

    assert!(volume.spec.target.is_some(), "Unpublish failed");

    let specs = cluster.rest_v00().specs_api().get_specs().await.unwrap();
    let replica = specs.replicas.first().cloned().unwrap();
    assert!(replica.owners.volume.is_some());
    assert!(replica.owners.nexuses.is_empty());

    // allow the reconcile to run - it should not disown the replica
    tokio::time::sleep(reconcile_period * 12).await;

    let specs = cluster.rest_v00().specs_api().get_specs().await.unwrap();
    let replica = specs.replicas.first().cloned().unwrap();
    // we should still be part of the volume
    assert!(replica.owners.volume.is_some());
    assert!(replica.owners.nexuses.is_empty());
}

#[tokio::test]
async fn test_disown_missing_replica_owners() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    // Create a replica. This will save the replica spec to the persistent store.
    let replica_id = ReplicaId::new();
    cluster
        .rest_v00()
        .replicas_api()
        .put_pool_replica(
            "io-engine-1-pool-1",
            &replica_id,
            CreateReplicaBody {
                share: None,
                size: 5242880,
                thin: false,
                encrypted: false,
                allowed_hosts: None,
            },
        )
        .await
        .expect("Failed to create replica.");

    // Check the replica exists.
    let num_replicas = cluster
        .rest_v00()
        .replicas_api()
        .get_replicas()
        .await
        .expect("Failed to get replicas.")
        .len();
    assert_eq!(num_replicas, 1);

    // Modify the replica spec in the store so that the replica has a volume and nexus owner;
    // neither of which exist.
    let mut etcd = Etcd::new("0.0.0.0:2379").await.unwrap();
    let (mut replica, _mod_rev): (ReplicaSpec, i64) = etcd
        .get_obj(&ReplicaSpecKey::from(&replica_id))
        .await
        .unwrap();
    replica.managed = true;
    replica.owners = ReplicaOwners::new(Some(VolumeId::new()), vec![NexusId::new()]);

    // Persist the modified replica spec to the store
    etcd.put_obj(&replica)
        .await
        .expect("Failed to store modified replica.");

    // Restart the core agent so that it reloads the modified replica spec from the persistent
    // store.
    cluster.restart_core().await;

    // Allow time for the core agent to restart.
    sleep(Duration::from_secs(2));

    // The replica should be removed because none of its parents exist.
    let num_replicas = cluster
        .rest_v00()
        .replicas_api()
        .get_replicas()
        .await
        .expect("Failed to get replicas.")
        .len();
    assert_eq!(num_replicas, 0);
}

#[tokio::test]
async fn destroy_after_restart() {
    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;

    let cluster = ClusterBuilder::builder()
        .with_io_engines(1)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_reconcile_period(Duration::from_secs(10), Duration::from_secs(10))
        .build()
        .await
        .unwrap();

    let client = cluster.grpc_client();

    let pools = client
        .pool()
        .get(Filter::Pool(cluster.pool(0, 0)), None)
        .await
        .unwrap();
    let pool = pools.into_inner().first().cloned().unwrap();

    cluster
        .composer()
        .restart(cluster.node(0).as_str())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let destroy = DestroyPool {
        node: cluster.node(0),
        id: cluster.pool(0, 0),
        ..Default::default()
    };
    let create = CreatePool {
        node: cluster.node(0),
        id: "bob".into(),
        disks: pool.state().cloned().unwrap().disks,
        labels: None,
        encryption: None,
        cluster_size: None,
        max_expansion: None,
    };

    client.pool().destroy(&destroy, None).await.unwrap();
    let pool = client.pool().create(&create, None).await.unwrap();

    assert_eq!(pool.state().unwrap().id, create.id);
}

#[tokio::test]
async fn slow_create() {
    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;

    let vg = deployer_cluster::lvm::VolGroup::new("slow-pooly", POOL_SIZE_BYTES).unwrap();
    let lvol = vg.create_lvol("lvol0", POOL_SIZE_BYTES / 2).unwrap();
    lvol.suspend_await().await.unwrap();
    {
        let cluster = ClusterBuilder::builder()
            .with_io_engines(1)
            .with_allow_non_persistent_devlink(true)
            .with_reconcile_period(Duration::from_millis(250), Duration::from_millis(250))
            .with_cache_period("200ms")
            .with_options(|o| o.with_io_engine_devices(vec![lvol.path()]))
            .with_req_timeouts(Duration::from_millis(500), Duration::from_millis(500))
            .compose_build(|b| b.with_clean(true))
            .await
            .unwrap();

        let client = cluster.grpc_client();

        let create = CreatePool {
            node: cluster.node(0),
            id: "bob".into(),
            disks: vec![lvol.path().into()],
            labels: Some(PoolLabel::from([("a".into(), "b".into())])),
            encryption: None,
            cluster_size: None,
            max_expansion: None,
        };

        let result = client.pool().create(&create, None).await;
        match result {
            Err(error) => assert_eq!(error.kind, ReplyErrorKind::Cancelled),
            Ok(_) => {
                let info = lvol.dm_info().unwrap();
                tracing::error!("Log DMSetup info:\n{info}");
                panic!("Should have failed!");
            }
        }

        let result = client.pool().create(&create, None).await;
        match result {
            Err(error) => assert_eq!(error.kind, ReplyErrorKind::Aborted),
            Ok(_) => {
                let info = lvol.dm_info().unwrap();
                tracing::error!("Log DMSetup info:\n{info}");
                panic!("Should have failed!");
            }
        }

        lvol.resume().unwrap();

        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30);
        loop {
            if std::time::Instant::now() > (start + timeout) {
                panic!("Timeout waiting for the pool");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;

            let pools = client
                .pool()
                .get(Filter::Pool(create.id.clone()), None)
                .await
                .unwrap();

            let Some(pool) = pools.0.first() else {
                continue;
            };
            let Some(pool_spec) = pool.spec() else {
                continue;
            };
            if !pool_spec.status.created() {
                continue;
            }
            break;
        }

        let result = client.pool().create(&create, None).await;
        match result {
            Err(error) => assert_eq!(error.kind, ReplyErrorKind::AlreadyExists),
            Ok(_) => {
                let info = lvol.dm_info().unwrap();
                tracing::error!("Log DMSetup info:\n{info}");
                panic!("Should have failed!");
            }
        }

        let destroy = DestroyPool::from(create.clone());
        client.pool().destroy(&destroy, None).await.unwrap();

        // Now we try to recreate using an API call, rather than using the reconciler
        lvol.suspend_await().await.unwrap();

        let error = client
            .pool()
            .create(&create, None)
            .await
            .expect_err("device suspended");
        assert_eq!(error.kind, ReplyErrorKind::Cancelled);

        lvol.resume().unwrap();

        let pool = client.pool().create(&create, None).await.unwrap();
        assert!(pool.spec().unwrap().status.created());
    }
}

/// Tests that a different devlink for same device cannot be used across multiple storage pools.
///
/// The test performs the following steps:
/// 1. Writes a secret file containing encryption config.
/// 2. Creates a volume group and a logical volume so that we have devlinks.
/// 3. Verifies that the block device has at least two devlinks.
/// 4. Creates "pool-1" using the first devlink with encryption enabled (expected to succeed).
/// 5. Attempts to create "pool-2" using a second devlink from the same device with encryption enabled (expected to fail).
/// 6. Destroys "pool-1" and attempts to destroy "pool-2" (expected to fail since it wasn't created).
/// 7. Recreates "pool-2" after releasing the devlink(by destroying pool-1) (expected to succeed).
/// 8. Restarts the cluster and verifies that creating "pool-3" without encryption using an already used devlink(since pool-2 is imported) with aio prefix (expected to fail).
/// 9. Finally, attempts to destroy "pool-3" (expected to fail as it was never created).
#[tokio::test]
async fn reject_devlink_reuse() {
    use serde_json::json;
    use std::{env, path::PathBuf};
    use tokio::{fs::File, io::AsyncWriteExt};

    const SECRETFILE: &str = "secretfile";

    let _file = SecretFileCreator::new()
        .await
        .expect("Failed to create secret file");

    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
    let vg = deployer_cluster::lvm::VolGroup::new("reject-devlink-vg", POOL_SIZE_BYTES)
        .expect("Failed to create volume group");
    let lvol = vg
        .create_lvol("reject-devlink-lv", POOL_SIZE_BYTES / 2)
        .expect("Failed to create logical volume");

    {
        let cluster = ClusterBuilder::builder()
            .with_io_engines(1)
            .with_mount_host_dev_udev(true)
            .with_options(|o| o.with_io_engine_devices(vec![lvol.path()]))
            .compose_build(|b| b.with_clean(true))
            .await
            .expect("Failed to build cluster");

        let dev_path = lvol.path();
        let client = cluster.grpc_client();

        let bds = client
            .node()
            .get_block_devices(
                &GetBlockDevices {
                    node: cluster.node(0),
                    all: false,
                },
                None,
            )
            .await
            .expect("Failed to get block devices")
            .into_inner();

        let matched_device = bds
            .iter()
            .find(|bd| {
                let dev_path_str = dev_path.to_string();
                bd.devname == dev_path_str
                    || bd.devpath == dev_path_str
                    || bd.devlinks.iter().any(|link| link == &dev_path_str)
            })
            .expect("Block device should exist");

        let device_devlinks: Vec<&str> = matched_device
            .devlinks
            .iter()
            .filter(|link| link.starts_with("/dev/disk/by-id"))
            .map(|s| s.as_str())
            .collect();

        assert!(
            device_devlinks.len() >= 2,
            "Expected at least 2 devlinks, found {}",
            device_devlinks.len()
        );

        let create_pool_1_request = CreatePool {
            node: cluster.node(0),
            id: "pool-1".into(),
            disks: vec![device_devlinks
                .first()
                .expect("At least one devlink should be present")
                .to_string()
                .into()],
            labels: None,
            encryption: Some(Encryption::Secret(EncryptionSecret {
                name: SECRETFILE.to_string(),
            })),
            cluster_size: None,
            max_expansion: None,
        };

        client
            .pool()
            .create(&create_pool_1_request, None)
            .await
            .expect("Pool-1 creation should succeed");

        let create_pool_2_request = CreatePool {
            node: cluster.node(0),
            id: "pool-2".into(),
            disks: vec![device_devlinks
                .get(1)
                .expect("Another devlink should be present")
                .to_string()
                .into()],
            labels: None,
            encryption: Some(Encryption::Secret(EncryptionSecret {
                name: SECRETFILE.to_string(),
            })),
            cluster_size: None,
            max_expansion: None,
        };

        client
            .pool()
            .create(&create_pool_2_request, None)
            .await
            .expect_err("Pool-2 creation should fail");

        let destroy = DestroyPool::from(create_pool_1_request.clone());
        client
            .pool()
            .destroy(&destroy, None)
            .await
            .expect("Pool-1 destruction should succeed");

        let destroy = DestroyPool::from(create_pool_2_request.clone());
        client
            .pool()
            .destroy(&destroy, None)
            .await
            .expect_err("Pool-2 destruction should fail");

        client
            .pool()
            .create(&create_pool_2_request, None)
            .await
            .expect("Pool-2 creation should succeed now");

        cluster
            .composer()
            .restart(&cluster.node(0))
            .await
            .expect("Cluster restart failed");
        cluster
            .wait_node_status(cluster.node(0), NodeStatus::Online)
            .await
            .expect("Node service liveness check failed");

        let create_pool_3_request = CreatePool {
            node: cluster.node(0),
            id: "pool-3".into(),
            disks: vec![format!(
                "aio://{}",
                device_devlinks
                    .first()
                    .expect("At least one devlink should be present")
            )
            .into()],
            labels: None,
            encryption: None,
            cluster_size: None,
            max_expansion: None,
        };

        client
            .pool()
            .create(&create_pool_3_request, None)
            .await
            .expect_err("Creating pool-3 with pool-1 devlink should fail");

        client
            .pool()
            .destroy(&DestroyPool::from(create_pool_2_request.clone()), None)
            .await
            .expect("Destroying pool-2 should succeed");

        client
            .pool()
            .destroy(&DestroyPool::from(create_pool_3_request.clone()), None)
            .await
            .expect_err("Destroying pool-3 should fail");
    }

    pub struct SecretFileCreator {
        file_path: Option<PathBuf>,
    }

    impl SecretFileCreator {
        pub async fn new() -> std::io::Result<Self> {
            let root = env::var("WORKSPACE_ROOT").expect("WORKSPACE_ROOT is not set");
            let path = PathBuf::from(&root).join(".tmp");

            let data = json!({
                "cipher": "AesXts",
                "key": "2b7e151628aed2a6abf7158809cf4f3c",
                "key_len": 128,
                "key2": "2b7e151628aed2a6abf7158809cf4f3d",
                "key2_len": 128
            });

            let file_path = path.join(SECRETFILE);
            let mut file = File::create(&file_path).await?;
            file.write_all(data.to_string().as_bytes()).await?;

            Ok(Self {
                file_path: Some(file_path),
            })
        }
    }

    impl Drop for SecretFileCreator {
        fn drop(&mut self) {
            if let Some(ref path) = self.file_path {
                let path = path.clone();
                std::thread::spawn(move || {
                    let _ = std::fs::remove_file(path);
                    println!("Removed secretfile");
                });
            }
        }
    }
}
