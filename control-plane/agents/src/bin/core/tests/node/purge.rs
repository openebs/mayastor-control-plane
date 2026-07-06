use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    node::traits::NodeOperations, pool::traits::PoolOperations, replica::traits::ReplicaOperations,
    volume::traits::VolumeOperations,
};
use std::{collections::HashMap, time::Duration};
use stor_port::{
    pstor::{etcd::Etcd, StoreObj},
    transport_api::{ReplyErrorKind, ResourceKind},
    types::v0::{
        openapi::models,
        store::{
            node::{NodeOperation, NodeOperationState, NodeSpec, NodeSpecKey},
            SpecStatus,
        },
        transport::{
            CreatePool, CreateVolume, DestroyNode, DestroyVolume, Filter, NodeId, NodeStatus,
            PoolId, PublishVolume, Topology, VolumeAccessMode, VolumeId,
        },
    },
};

const NODE_OFFLINE_TIMEOUT: Duration = Duration::from_secs(10);
const VOLUME_SIZE: u64 = 5242880;

/// All node purge tests share a single cluster to avoid repeated spinup costs.
#[tokio::test]
async fn node_purge() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_pools(1)
        .with_cache_period("100ms")
        .with_node_deadline("100ms")
        .with_reconcile_period(Duration::from_millis(100), Duration::from_millis(100))
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();
    let node_id = cluster.node(0);
    // Create a volume with 1 replica pinned to node 0 so that Phase 4's
    // purge_succeeds_with_volume_loss reliably reports healthy_after=0
    let vol_id = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: vol_id.clone(),
                size: VOLUME_SIZE,
                replicas: 1,
                topology: Some(Topology::from(models::Topology::new_all(
                    Some(models::NodeTopology::explicit(
                        models::ExplicitNodeTopology::new(
                            vec![node_id.to_string()],
                            Vec::<String>::new(),
                        ),
                    )),
                    None,
                ))),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    volume_client
        .publish(
            &PublishVolume {
                uuid: vol_id.clone(),
                target_node: Some(node_id.clone()),
                share: None,
                publish_context: HashMap::new(),
                frontend_nodes: vec![],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("Initial volume should publish on node 0");

    // Verify the replica exists.
    let replicas = replica_client
        .get(Filter::Volume(vol_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert!(!replicas.is_empty(), "Volume should have a replica");

    // --- Phase 1: Node online ---
    purge_rejected_node_online(&node_client, &node_id).await;

    // --- Phase 2: Node offline, not cordoned ---
    cluster.composer().stop("io-engine-1").await.unwrap();
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Offline, NODE_OFFLINE_TIMEOUT)
        .await
        .expect("Node should go offline");

    purge_rejected_not_cordoned(&node_client, &node_id).await;

    // Cordon node for remaining tests.
    node_client
        .cordon(node_id.clone(), "offline-delete".to_string())
        .await
        .unwrap();

    // --- Phase 3: Node offline, cordoned — missing flags ---
    purge_rejected_no_purge_flag(&node_client, &node_id).await;
    purge_rejected_without_accept(&node_client, &node_id).await;
    purge_rejected_volume_loss_without_accept(&node_client, &node_id).await;

    // --- Phase 4: Purge succeeds with all flags ---
    // Node delete will cordon pools automatically on behalf of the user.
    purge_succeeds_with_volume_loss(&node_client, &pool_client, &node_id, &vol_id).await;

    // --- Phase 5: Reconciler resumes interrupted purge ---
    purge_reconciler_resumes_interrupted(&cluster).await;
}

/// Purge must be rejected when the node is online.
async fn purge_rejected_node_online(node_client: &dyn NodeOperations, node_id: &NodeId) {
    let request = DestroyNode::purge(node_id.clone())
        .with_accept(true)
        .with_accept_volume_loss(true)
        .with_accept_snapshot_loss(true);
    let err = node_client
        .delete(&request)
        .await
        .expect_err("Purge should be rejected for online node");

    assert_eq!(err.kind, ReplyErrorKind::NodeIsOnline);
    assert_eq!(err.resource, ResourceKind::Node);
}

/// Purge must be rejected when the node is not cordoned.
async fn purge_rejected_not_cordoned(node_client: &dyn NodeOperations, node_id: &NodeId) {
    let request = DestroyNode::purge(node_id.clone())
        .with_accept(true)
        .with_accept_volume_loss(true)
        .with_accept_snapshot_loss(true);
    let err = node_client
        .delete(&request)
        .await
        .expect_err("Purge should be rejected for uncordoned node");

    assert_eq!(err.kind, ReplyErrorKind::NodeNotCordoned);
    assert_eq!(err.resource, ResourceKind::Node);
}

/// Purge must be rejected when node has resources but purge=false.
async fn purge_rejected_no_purge_flag(node_client: &dyn NodeOperations, node_id: &NodeId) {
    // purge defaults to false.
    let request = DestroyNode::new(node_id.clone()).with_accept(true);
    let err = node_client
        .delete(&request)
        .await
        .expect_err("Delete should be rejected without purge flag");

    assert_eq!(err.kind, ReplyErrorKind::NodeHasResources);
    assert_eq!(err.resource, ResourceKind::Node);
}

/// Purge must be rejected when node has pools but accept=false.
async fn purge_rejected_without_accept(node_client: &dyn NodeOperations, node_id: &NodeId) {
    // accept defaults to false.
    let request = DestroyNode::purge(node_id.clone());
    let err = node_client
        .delete(&request)
        .await
        .expect_err("Purge should be rejected without accept");

    assert_eq!(err.kind, ReplyErrorKind::NodePurgeAcceptRequired);
    assert_eq!(err.resource, ResourceKind::Node);
}

/// Purge must be rejected when volume loss would occur but accept_volume_loss=false.
/// Node delete pre-flight scans all pools and reports aggregate volume loss.
async fn purge_rejected_volume_loss_without_accept(
    node_client: &dyn NodeOperations,
    node_id: &NodeId,
) {
    let request = DestroyNode::purge(node_id.clone()).with_accept(true);
    // accept_volume_loss defaults to false.
    let err = node_client
        .delete(&request)
        .await
        .expect_err("Purge should be rejected without volume loss accept");

    assert_eq!(err.kind, ReplyErrorKind::NodePurgeVolumeLossAcceptRequired);
    assert_eq!(err.resource, ResourceKind::Node);
    assert!(
        !err.volume_loss.volumes.is_empty(),
        "Error should carry the volume loss details"
    );
}

/// Purge succeeds when all flags are provided, reports volume loss.
async fn purge_succeeds_with_volume_loss(
    node_client: &dyn NodeOperations,
    pool_client: &dyn PoolOperations,
    node_id: &NodeId,
    volume_id: &VolumeId,
) {
    let request = DestroyNode::purge(node_id.clone())
        .with_accept(true)
        .with_accept_volume_loss(true)
        .with_accept_snapshot_loss(true);

    let result = node_client
        .delete(&request)
        .await
        .expect("Purge should succeed with all flags");

    assert_eq!(&result.node_id, node_id);
    assert!(
        !result.volume_loss.volumes.is_empty(),
        "Should report volume loss"
    );
    assert_eq!(result.volume_loss.volumes.len(), 1);
    assert_eq!(result.volume_loss.volumes[0].volume_id, *volume_id);
    assert_eq!(result.volume_loss.volumes[0].healthy_after, 0);

    // Verify node spec is gone.
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let nodes = node_client
            .get(Filter::Node(node_id.clone()), false, None)
            .await;
        match nodes {
            Err(e) if e.kind == ReplyErrorKind::NotFound => break,
            _ => {}
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for node spec to be removed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify the purged node's pool is gone. Node 1's pool is still alive
    // (it was never part of the purge), so we filter by node rather than
    // expecting an empty global list.
    let pools = pool_client
        .get(Filter::Node(node_id.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert!(
        pools.is_empty(),
        "All pools on the purged node should be gone"
    );
}

/// Simulate a crash after the node is marked Purging but before pools are
/// deleted. Write the Purging state directly to etcd, restart core, and verify
/// the reconciler resumes and completes the purge.
async fn purge_reconciler_resumes_interrupted(cluster: &Cluster) {
    let node_client = cluster.grpc_client().node();
    let pool_client = cluster.grpc_client().pool();
    let volume_client = cluster.grpc_client().volume();
    let replica_client = cluster.grpc_client().replica();
    let node_id = cluster.node(0);
    // Node 1 remains online throughout all phases and retains its auto-created
    // pool from cluster startup; it is the survivor node for the 2-replica volume.
    let node1_id = cluster.node(1);

    // 1. Bring io-engine back online.
    //    After Phase 4 purged the node, the spec is gone. The io-engine must
    //    re-register to create a new NodeSpec, so we poll for it instead of
    //    using wait_node_status_tmo (which panics on NotFound).
    cluster.composer().start("io-engine-1").await.unwrap();
    let timeout = NODE_OFFLINE_TIMEOUT;
    let start = std::time::Instant::now();
    loop {
        if let Ok(nodes) = node_client
            .get(Filter::Node(node_id.clone()), false, None)
            .await
        {
            if nodes
                .0
                .first()
                .and_then(|n| n.state())
                .map(|s| s.status == NodeStatus::Online)
                .unwrap_or(false)
            {
                break;
            }
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for node to re-register and come online"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 2. Create a pool and volume.
    let pool_id: PoolId = "resume-purge-pool".into();
    pool_client
        .create(
            &CreatePool {
                node: node_id.clone(),
                id: pool_id.clone(),
                disks: vec!["malloc:///resume_disk?size_mb=100".into()],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Pool creation should succeed");

    // 1-replica volume pinned to node 0 — fully lost when node 0 is purged.
    let vol_id = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: vol_id.clone(),
                size: VOLUME_SIZE,
                replicas: 1,
                topology: Some(Topology::from(models::Topology::new_all(
                    Some(models::NodeTopology::explicit(
                        models::ExplicitNodeTopology::new(
                            vec![node_id.to_string()],
                            Vec::<String>::new(),
                        ),
                    )),
                    None,
                ))),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("Volume creation should succeed");

    volume_client
        .publish(
            &PublishVolume {
                uuid: vol_id.clone(),
                target_node: Some(node_id.clone()),
                share: None,
                publish_context: HashMap::new(),
                frontend_nodes: vec![],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("1-replica volume should publish on node 0");

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

    // 2-replica volume spanning both nodes — one replica survives on node 1
    // after the purge, so the volume spec persists in a degraded state.
    let vol_id_2r = VolumeId::new();
    volume_client
        .create(
            &CreateVolume {
                uuid: vol_id_2r.clone(),
                size: VOLUME_SIZE,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("2-replica volume creation should succeed");

    volume_client
        .publish(
            &PublishVolume {
                uuid: vol_id_2r.clone(),
                target_node: Some(node_id.clone()),
                share: None,
                publish_context: HashMap::new(),
                frontend_nodes: vec![],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("2-replica volume should publish on node 0");

    let replicas_2r = replica_client
        .get(Filter::Volume(vol_id_2r.clone()), None)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        replicas_2r.len(),
        2,
        "2-replica volume should have 2 replicas"
    );
    assert!(
        replicas_2r.iter().any(|r| r.node == node_id),
        "One replica should be on node 0 (the node being purged)"
    );
    assert!(
        replicas_2r.iter().any(|r| r.node == node1_id),
        "One replica should be on node 1 (the survivor node)"
    );

    // 3. Stop node so it goes offline.
    cluster.composer().stop("io-engine-1").await.unwrap();
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Offline, NODE_OFFLINE_TIMEOUT)
        .await
        .expect("Node should go offline");

    // Cordon the node.
    node_client
        .cordon(node_id.clone(), "reconciler-test".to_string())
        .await
        .unwrap();

    // 4. Simulate crash after start_destroy_for_purge: write Purging state + Destroy op to etcd.
    //    The reconciler will cordon pools automatically when resuming.
    let mut etcd = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd");
    let (mut node_spec, _): (NodeSpec, i64) = etcd
        .get_obj(&NodeSpecKey::from(&node_id))
        .await
        .expect("Node spec should exist in etcd");

    node_spec.status = SpecStatus::Purging;
    node_spec.operation = Some(NodeOperationState {
        operation: NodeOperation::Destroy,
        result: None,
    });
    etcd.put_obj(&node_spec)
        .await
        .expect("Failed to write Purging node spec to etcd");

    // 5. Restart core agent — reconciler should detect Purging and resume the purge.
    cluster
        .restart_core_with_liveness(None)
        .await
        .expect("Core agent should restart and become live");

    // 6. Wait for the reconciler to complete the purge (node spec gone).
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let nodes = node_client
            .get(Filter::Node(node_id.clone()), false, None)
            .await;
        match nodes {
            Err(e) if e.kind == ReplyErrorKind::NotFound => break,
            _ => {}
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for reconciler to complete node purge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 7. Pool is gone — either NotFound or an empty list; anything else is a bug.
    match pool_client.get(Filter::Pool(pool_id.clone()), None).await {
        Ok(p) => assert!(
            p.into_inner().is_empty(),
            "Pool should be gone after node purge"
        ),
        Err(e) if e.kind == ReplyErrorKind::NotFound => {}
        Err(e) => panic!("Unexpected error checking pool after purge: {e:?}"),
    }

    // 8. Replicas of the 1-replica volume are gone — same contract.
    match replica_client
        .get(Filter::Volume(vol_id.clone()), None)
        .await
    {
        Ok(r) => assert!(
            r.into_inner().is_empty(),
            "Replicas should be gone after node purge"
        ),
        Err(e) if e.kind == ReplyErrorKind::NotFound => {}
        Err(e) => panic!("Unexpected error checking replicas after purge: {e:?}"),
    }

    // 9. Verify the 2-replica volume still exists and has exactly one surviving
    //    replica on node 1. The volume spec is NOT deleted by the purge because
    //    healthy_after=1 — it stays degraded until the user removes it.
    let surviving = replica_client
        .get(Filter::Volume(vol_id_2r.clone()), None)
        .await
        .expect("2-replica volume replicas should be queryable after purge")
        .into_inner();
    assert_eq!(
        surviving.len(),
        1,
        "Exactly one replica should survive on node 1"
    );
    assert_eq!(
        surviving[0].node, node1_id,
        "Surviving replica must be on node 1, not the purged node"
    );

    volume_client
        .get(Filter::Volume(vol_id_2r.clone()), false, None, None)
        .await
        .expect("2-replica volume spec should still exist after partial purge");

    // 10. Clean up: destroy both volumes.
    //     vol_id has no replicas left — pure spec deletion, no io-engine RPC.
    //     vol_id_2r has one replica on node 1 — the destroy will tear it down.
    volume_client
        .destroy(
            &DestroyVolume {
                uuid: vol_id.clone(),
            },
            None,
        )
        .await
        .expect("1-replica volume destroy should succeed after all replicas are gone");

    let err = volume_client
        .get(Filter::Volume(vol_id.clone()), false, None, None)
        .await
        .expect_err("1-replica volume should be NotFound after destroy");
    assert_eq!(err.kind, ReplyErrorKind::NotFound);
    assert_eq!(err.resource, ResourceKind::Volume);

    volume_client
        .destroy(
            &DestroyVolume {
                uuid: vol_id_2r.clone(),
            },
            None,
        )
        .await
        .expect("2-replica volume destroy should succeed");

    let err = volume_client
        .get(Filter::Volume(vol_id_2r.clone()), false, None, None)
        .await
        .expect_err("2-replica volume should be NotFound after destroy");
    assert_eq!(err.kind, ReplyErrorKind::NotFound);
    assert_eq!(err.resource, ResourceKind::Volume);
}
