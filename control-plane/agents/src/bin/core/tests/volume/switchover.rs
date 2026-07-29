#![cfg(test)]
use crate::volume::helpers::wait_node_online;
use deployer_cluster::{Cluster, ClusterBuilder, FindVolumeRequest};
use grpc::{
    common::MapWrapper,
    csi_node_nvme::{nvme_operations_client, NvmeConnectRequest},
    operations::{
        nexus::traits::NexusOperations, pool::traits::PoolOperations,
        registry::traits::RegistryOperations, replica::traits::ReplicaOperations,
        volume::traits::VolumeOperations,
    },
};
use http::Uri;
use std::{collections::HashMap, time::Duration};
use stor_port::{
    transport_api::{ReplyErrorKind, ResourceKind},
    types::v0::{
        openapi::{
            apis::specs_api::tower::client::direct::Specs,
            models::{self, SpecStatus},
        },
        store::nexus::NexusSpec,
        transport::{
            CreateReplica, CreateVolume, DestroyReplica, DestroyShutdownTargets, DestroyVolume,
            Filter, GetSpecs, Nexus, NodeStatus, PublishVolume, ReplicaId, RepublishVolume,
            VolumeAccessMode, VolumeShareProtocol,
        },
    },
};
use tokio::time::sleep;
use tower::service_fn;

// This test: Creates a three io engine cluster
// Creates volume with 2 replicas and publishes it to create nexus
// Republishes nexus to a new node
// Stops node housing a previous nexus
// Destroys volume
// Starts node which was stopped and expects old nexus cleanup.
#[tokio::test]
async fn old_nexus_delete_after_vol_destroy() {
    let reconcile_period = Duration::from_millis(200);
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(3)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("100ms")
        .with_reconcile_period(reconcile_period, reconcile_period)
        .build()
        .await
        .unwrap();

    let vol_client = cluster.grpc_client().volume();
    let nexus_client = cluster.grpc_client().nexus();
    let rest_client = cluster.rest_v00();
    let spec_client = rest_client.specs_api();
    let node_client = cluster.grpc_client().node();

    let vol = vol_client
        .create(
            &CreateVolume {
                uuid: "ec4e66fd-3b33-4439-b504-d49aba53da26".try_into().unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("unable to create volume");

    let vol = vol_client
        .publish(
            &PublishVolume {
                uuid: vol.uuid().clone(),
                share: None,
                target_node: Some(cluster.node(0)),
                publish_context: HashMap::new(),
                frontend_nodes: vec![cluster.node(1).to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("failed to publish volume");

    let target_node = vol.state().target.unwrap().node;
    assert_eq!(target_node, cluster.node(0));

    let vol = vol_client
        .republish(
            &RepublishVolume {
                uuid: vol.uuid().clone(),
                target_node: Some(cluster.node(1)),
                share: VolumeShareProtocol::Nvmf,
                reuse_existing: false,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect("unable to republish volume");

    let republished_node = vol.state().target.unwrap().node;
    assert_ne!(
        republished_node, target_node,
        "expected nexus node to change post republish"
    );

    let nexus_api = rest_client.nexuses_api();
    let nexuses = nexus_api.get_nexuses().await.unwrap();
    tracing::info!("Nexuses after republish: {nexuses:?}");

    assert!(nexuses
        .iter()
        .any(|n| n.state == models::NexusState::Shutdown));

    let nexuses = nexus_client
        .get(Filter::None, None)
        .await
        .expect("Could not list nexuses");
    assert_eq!(nexuses.0.len(), 2, "expected two nexuses after republish");

    cluster
        .composer()
        .stop(cluster.node(0).as_str())
        .await
        .expect("failed to stop container");

    let nexuses_after_pause = nexus_client
        .get(Filter::None, None)
        .await
        .expect("could not list nexuses");
    assert_eq!(
        nexuses_after_pause.0.len(),
        1,
        "expected 1 nexus after stop"
    );

    let spec = spec_client
        .get_specs()
        .await
        .expect("expected to retrieve specs");

    let nexus_spec = spec.nexuses;
    assert_eq!(nexus_spec.len(), 2, "spec should contain 2 nexuses");

    vol_client
        .destroy(
            &DestroyVolume {
                uuid: vol.uuid().clone(),
            },
            None,
        )
        .await
        .expect("failed to delete volume");

    let spec = spec_client
        .get_specs()
        .await
        .expect("expected to retrieve specs");

    let nexus_spec = spec.nexuses;
    assert_eq!(
        nexus_spec.len(),
        1,
        "spec should contain one nexus after vol destroy with old target node unreachable"
    );

    let n = nexus_spec.first().unwrap();
    assert_eq!(
        n.status,
        SpecStatus::Deleting,
        "failed to mark nexus spec as Deleting"
    );

    cluster
        .composer()
        .start(cluster.node(0).as_str())
        .await
        .expect("failed to start container");

    tracing::info!("Waiting for node online");

    wait_node_online(&node_client, cluster.node(0))
        .await
        .expect("Node to be online");
    wait_nexus_spec_empty(spec_client)
        .await
        .expect("No nexus specs");
}

async fn wait_nexus_spec_empty(spec_client: &dyn Specs) -> Result<(), ()> {
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    loop {
        let specs = spec_client
            .get_specs()
            .await
            .expect("expected to retrieve specs");
        if specs.nexuses.is_empty() {
            return Ok(());
        }
        if std::time::Instant::now() > (start + timeout) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(())
}

#[tokio::test]
async fn lazy_delete_shutdown_targets() {
    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(2)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .with_req_timeouts(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();
    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let volume = vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                share: None,
                target_node: Some(cluster.node(0)),
                publish_context: HashMap::new(),
                frontend_nodes: vec![cluster.node(1).to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .unwrap();
    let first_target = volume.state().target.unwrap();

    cluster
        .composer()
        .kill(cluster.node(0).as_str())
        .await
        .unwrap();

    vol_cli
        .republish(
            &RepublishVolume {
                uuid: volume.uuid().clone(),
                target_node: Some(cluster.node(1)),
                share: VolumeShareProtocol::Nvmf,
                reuse_existing: true,
                frontend_node: cluster.node(0),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect_err("Wrong frontend node");

    vol_cli
        .republish(
            &RepublishVolume {
                uuid: volume.uuid().clone(),
                target_node: Some(cluster.node(1)),
                share: VolumeShareProtocol::Nvmf,
                reuse_existing: true,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .unwrap();

    cluster.restart_core().await;
    // Wait for core service to restart.
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");
    let request = DestroyShutdownTargets::new(volume.uuid().clone(), None);
    vol_cli
        .destroy_shutdown_target(&request, None)
        .await
        .expect("Should destroy old target even though the node is offline!");

    let nx_cli = cluster.grpc_client().registry();

    let target = find_target(&nx_cli, &first_target).await;
    assert!(target.unwrap().spec_status.deleting());

    cluster
        .composer()
        .restart(cluster.node(0).as_str())
        .await
        .unwrap();

    wait_till_target_deleted(&nx_cli, &first_target).await;

    vol_cli
        .destroy(
            &DestroyVolume {
                uuid: volume.uuid().clone(),
            },
            None,
        )
        .await
        .expect("failed to delete volume");

    reshutdown(&cluster).await;
}

async fn find_target(client: &impl RegistryOperations, target: &Nexus) -> Option<NexusSpec> {
    let response = client.get_specs(&GetSpecs {}, None).await.unwrap().nexuses;
    response.into_iter().find(|n| n.uuid == target.uuid)
}

/// Wait for the unpublished volume to have the specified replica count
pub(crate) async fn wait_till_target_deleted(client: &impl RegistryOperations, target: &Nexus) {
    let timeout = Duration::from_secs(11);
    let start = std::time::Instant::now();
    loop {
        let target = find_target(client, target).await;
        if target.is_none() {
            return;
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!("Timeout waiting for the target to be deleted");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

const VOLUME_UUID: &str = "1e3cf927-80c2-47a8-adf0-95c486bdd7b7";
const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;

#[tokio::test]
async fn volume_republish_nexus_recreation() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_io_engines(2)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .build()
        .await
        .unwrap();

    let client = cluster.grpc_client().volume();

    assert!(client
        .create(
            &CreateVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                size: 5242880,
                replicas: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .is_ok());

    let replicas = cluster
        .grpc_client()
        .replica()
        .get(Filter::None, None)
        .await
        .expect("error getting replicas")
        .into_inner();

    let replica_node = replicas
        .first()
        .expect("Should have one replica")
        .node
        .as_str();

    let volume = client
        .publish(
            &PublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: Some(VolumeShareProtocol::Nvmf),
                target_node: Some(replica_node.into()),
                publish_context: HashMap::new(),
                frontend_nodes: vec![cluster.node(1).to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("Volume publish should have succeeded.");

    let older_nexus = volume
        .state()
        .target
        .expect("Target should be present as publish succceded");

    // Stop the node that hosts the nexus and only replica.
    cluster
        .composer()
        .stop(replica_node)
        .await
        .expect("Node should have been killed");

    // Start the node that hosts the nexus and only replica.
    cluster
        .composer()
        .start(replica_node)
        .await
        .expect("Node should have been started");

    // Wait for control plane refresh.
    cluster
        .node_service_liveness(None)
        .await
        .expect("Service should have been live by now");

    assert!(pool_recreated(&cluster, 10).await);

    // Republishing volume after node restart.
    let volume = client
        .republish(
            &RepublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: VolumeShareProtocol::Nvmf,
                target_node: None,
                reuse_existing: true,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect("Volume republish should have succeeded.");

    let newer_nexus = volume
        .state()
        .target
        .expect("Target should be present as republish succeeded");

    assert_eq!(older_nexus, newer_nexus);
}

async fn pool_recreated(cluster: &Cluster, max_tries: i32) -> bool {
    for _ in 1..max_tries {
        if let Ok(pools) = cluster.grpc_client().pool().get(Filter::None, None).await {
            if pools
                .into_inner()
                .into_iter()
                .filter(|p| p.state().is_some())
                .count()
                == 2
            {
                return true;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    false
}

#[tokio::test]
async fn node_exhaustion() {
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .build()
        .await
        .unwrap();

    let client = cluster.grpc_client().volume();

    client
        .create(
            &CreateVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                size: 5242880,
                replicas: 3,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let _volume = client
        .publish(
            &PublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: Some(VolumeShareProtocol::Nvmf),
                target_node: None,
                publish_context: HashMap::new(),
                frontend_nodes: vec![cluster.node(1).to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("Volume publish should have succeeded.");

    // Republishing volume after node restart.
    let _volume = client
        .republish(
            &RepublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: VolumeShareProtocol::Nvmf,
                target_node: None,
                reuse_existing: false,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect("Volume republish should have succeeded.");

    // Republishing volume after node restart.
    let _volume = client
        .republish(
            &RepublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: VolumeShareProtocol::Nvmf,
                target_node: None,
                reuse_existing: false,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect("Volume republish should have succeeded.");

    // Republishing volume after node restart.
    let error = client
        .republish(
            &RepublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: VolumeShareProtocol::Nvmf,
                target_node: None,
                reuse_existing: false,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect_err("Exhausted all nodes");

    assert_eq!(error.kind, ReplyErrorKind::ResourceExhausted);
    assert_eq!(error.resource, ResourceKind::Node);
}

#[tokio::test]
async fn shutdown_failed_replica_owner() {
    let grpc_timeout = Duration::from_millis(512);

    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("100ms")
        .with_reconcile_period(Duration::from_millis(100), Duration::from_millis(100))
        .with_node_deadline("2s")
        .with_options(|o| {
            o.with_io_engine_env("MAYASTOR_HB_INTERVAL_SEC", "1")
                .with_isolated_io_engine(true)
        })
        .with_req_timeouts_min(true, grpc_timeout, grpc_timeout)
        .build()
        .await
        .unwrap();

    let vol_cli = cluster.grpc_client().volume();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let volume = vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                share: None,
                target_node: Some(cluster.node(0)),
                publish_context: HashMap::new(),
                frontend_nodes: vec![cluster.node(1).to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .unwrap();

    cluster
        .composer()
        .pause(cluster.node(0).as_str())
        .await
        .unwrap();

    cluster
        .wait_node_status(cluster.node(0), NodeStatus::Unknown)
        .await
        .unwrap();

    vol_cli
        .republish(
            &RepublishVolume {
                uuid: volume.uuid().clone(),
                target_node: Some(cluster.node(1)),
                share: VolumeShareProtocol::Nvmf,
                reuse_existing: true,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .unwrap();

    cluster
        .composer()
        .thaw(cluster.node(0).as_str())
        .await
        .unwrap();

    cluster
        .wait_node_status(cluster.node(0), NodeStatus::Online)
        .await
        .unwrap();

    assert!(!wait_till_pool_locked(&cluster).await);

    let nexus_cli = cluster.grpc_client().nexus();
    let nexuses = nexus_cli
        .get(Filter::Node(cluster.node(0)), None)
        .await
        .unwrap();
    assert_eq!(nexuses.into_inner().len(), 1);

    let request = DestroyShutdownTargets::new(volume.uuid().clone(), None);
    vol_cli
        .destroy_shutdown_target(&request, None)
        .await
        .unwrap();

    let nexuses = nexus_cli
        .get(Filter::Node(cluster.node(0)), None)
        .await
        .unwrap();
    assert!(nexuses.into_inner().is_empty());
}

async fn poll_pool_locked(cluster: &Cluster, is_locked: &mut bool) {
    if *is_locked {
        return;
    }

    let replica_cli = cluster.grpc_client().replica();
    let req = CreateReplica {
        node: cluster.node(0),
        uuid: ReplicaId::new(),
        pool_id: cluster.pool(0, 0),
        size: 4 * 1024 * 1024,
        thin: true,
        ..Default::default()
    };

    let Ok(replica) = replica_cli.create(&req, None).await else {
        *is_locked = true;
        return;
    };

    let req = DestroyReplica::from(replica);
    *is_locked = replica_cli.destroy(&req, None).await.is_err();
}

async fn wait_till_pool_locked(cluster: &Cluster) -> bool {
    let timeout = Duration::from_secs(2);
    let start = std::time::Instant::now();
    let mut is_locked = false;
    loop {
        poll_pool_locked(cluster, &mut is_locked).await;
        if is_locked {
            return true;
        }

        if std::time::Instant::now() > (start + timeout) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn reshutdown(cluster: &Cluster) {
    let client = cluster.grpc_client().volume();

    client
        .create(
            &CreateVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let _volume = client
        .publish(
            &PublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: Some(VolumeShareProtocol::Nvmf),
                target_node: Some(cluster.node(1)),
                publish_context: HashMap::new(),
                frontend_nodes: vec![cluster.node(1).to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("Volume publish should have succeeded.");

    cluster.composer().pause(&cluster.node(1)).await.unwrap();
    cluster.composer().pause(&cluster.node(0)).await.unwrap();

    // Republishing volume after node restart.
    let _volume = client
        .republish(
            &RepublishVolume {
                uuid: VOLUME_UUID.try_into().unwrap(),
                share: VolumeShareProtocol::Nvmf,
                target_node: None,
                reuse_existing: false,
                frontend_node: cluster.node(1),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .expect_err("Volume republish should have not succeeded.");

    cluster.composer().restart(&cluster.node(0)).await.unwrap();
    cluster.composer().thaw(&cluster.node(1)).await.unwrap();

    cluster
        .wait_node_status(cluster.node(0), NodeStatus::Online)
        .await
        .unwrap();
    cluster
        .wait_node_status(cluster.node(1), NodeStatus::Online)
        .await
        .unwrap();

    client
        .destroy_shutdown_target(
            &DestroyShutdownTargets::new(VOLUME_UUID.try_into().unwrap(), None),
            None,
        )
        .await
        .expect("Volume Destroy should succeed.");

    let start = std::time::Instant::now();
    let mut result = Ok(());
    while start.elapsed() < std::time::Duration::from_secs(3) {
        result = client
            .republish(
                &RepublishVolume {
                    uuid: VOLUME_UUID.try_into().unwrap(),
                    share: VolumeShareProtocol::Nvmf,
                    target_node: None,
                    reuse_existing: true,
                    frontend_node: cluster.node(1),
                    reuse_existing_fallback: true,
                },
                None,
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(result.is_ok(), "Volume republish should have succeeded");
}

/// ### Test details
/// Avoid data corruption in a case when a new nexus picks up a replica deemed healthy(and is local to new nexus),
/// while the same replica is being marked by an older nexus as unhealthy.
/// 1. Create a 3-repl volume and publish it on a node.
/// 2. Run IO to the volume.
/// 3. Disconnect the io-engine that hosts volume target from the network. IO path loss.
/// 4. Republish volume to a different node.
/// 5. Find and nvme connect to new path. Frontend node remains as earlier.
/// 6. Expect IO to finish without error.
#[tokio::test]
async fn republished_nexus_io_engine_txn_fail() {
    let grpc_timeout = Duration::from_millis(512);

    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_csi(true, true)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("100ms")
        .with_reconcile_period(Duration::from_millis(150), Duration::from_millis(150))
        .with_node_deadline("2s")
        .with_options(|o| {
            o.with_io_engine_env("MAYASTOR_HB_INTERVAL_SEC", "1")
                .with_isolated_io_engine(true)
        })
        .with_req_timeouts_min(true, grpc_timeout, grpc_timeout)
        .build()
        .await
        .unwrap();

    let node_idx0 = cluster.node(0);
    let app_node_idx0 = cluster.csi_node(0);
    let node_idx1 = cluster.node(1);
    let app_id = cluster.csi_node(0);
    let vol_cli = cluster.grpc_client().volume();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 52428800,
                replicas: 3,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let volume = vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                share: Some(VolumeShareProtocol::Nvmf),
                target_node: Some(node_idx0.clone()),
                publish_context: HashMap::new(),
                frontend_nodes: vec![app_node_idx0.to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .unwrap();

    let api = cluster.rest_v00();
    let rest_cli = api.volumes_api();
    let volume_grpc = volume.clone();
    let volume_models = rest_cli.get_volume(volume_grpc.uuid()).await.unwrap();

    let (s, r) = tokio::sync::oneshot::channel::<()>();
    let task = run_fio_vol(&cluster, volume_models.clone(), r).await;
    drop(s);
    println!("STEP: started fio...");

    cluster.composer().pause(node_idx0.as_str()).await.unwrap();
    println!("STEP: disconnected container from network...");

    cluster
        .wait_node_status_ext(node_idx0.clone(), |s| !matches!(s, NodeStatus::Online))
        .await
        .unwrap();
    println!("STEP: node now unknown...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let vol_republished = vol_cli
        .republish(
            &RepublishVolume {
                uuid: volume.uuid().clone(),
                target_node: Some(node_idx1),
                share: VolumeShareProtocol::Nvmf,
                reuse_existing: true,
                frontend_node: app_node_idx0.clone(),
                reuse_existing_fallback: false,
            },
            None,
        )
        .await
        .unwrap();
    println!("STEP: Republished volume to new node...");

    let csi_node_ip = cluster.composer().container_ip(&cluster.csi_container(0));
    let socket_path_cp = std::path::PathBuf::from(format!("/var/tmp/csi-{app_id}.sock"));
    let channel =
        tonic::transport::channel::Endpoint::try_from(format!("http://{csi_node_ip}:50051"))
            .expect("local endpoint should be valid")
            .connect_with_connector_lazy(service_fn(move |_: Uri| {
                let path = socket_path_cp.clone();
                async {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        tokio::net::UnixStream::connect(path).await?,
                    ))
                }
            }));
    let mut nvme_clnt = nvme_operations_client::NvmeOperationsClient::new(channel);
    let new_path_uri = vol_republished.state().target.unwrap().device_uri;
    let _ = nvme_clnt
        .nvme_connect(NvmeConnectRequest {
            uri: new_path_uri,
            publish_context: None,
        })
        .await;

    cluster.composer().thaw(node_idx0.as_str()).await.unwrap();
    cluster
        .wait_node_status_tmo(
            node_idx0.clone(),
            NodeStatus::Online,
            Duration::from_secs(4),
        )
        .await
        .unwrap();
    println!("STEP: Reconnected older container back to network...");

    let nexus_cli = cluster.grpc_client().nexus();
    let nexuses = nexus_cli
        .get(Filter::Node(node_idx0.clone()), None)
        .await
        .unwrap();
    assert_eq!(nexuses.into_inner().len(), 1);

    tracing::info!("STEP: wait for fio completion...");
    let errcode = task.await.unwrap();
    assert!(errcode.is_some_and(|s| s == 0));
}

async fn run_fio_vol(
    cluster: &Cluster,
    volume: models::Volume,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Option<i64>> {
    run_fio_vol_verify(cluster, volume, stop).await
}
async fn run_fio_vol_verify(
    cluster: &Cluster,
    volume: models::Volume,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Option<i64>> {
    let fio_builder = |device: &str| {
        let filename = format!("--filename={device}");
        vec![
            "taskset",
            "-c",
            cluster.fio_taskset().as_str(),
            "fio",
            "--direct=1",
            "--ioengine=libaio",
            "--bs=4k",
            "--iodepth=16",
            "--loops=1",
            "--numjobs=1",
            "--name=fio",
            "--readwrite=randwrite",
            "--verify=crc32",
            filename.as_str(),
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
    };

    println!("STEP: staging volume");
    let mut node = cluster.csi_node_client(0).await.unwrap();
    node.node_stage_volume(&volume, HashMap::from([("ioTimeout".into(), "11".into())]))
        .await
        .unwrap();

    let response = node
        .internal()
        .find_volume(FindVolumeRequest {
            volume_id: volume.spec.uuid.to_string(),
        })
        .await
        .unwrap();

    let device_path = response.into_inner().device_path;
    let device_path = device_path.trim_end();
    let fio_cmd = fio_builder(device_path);
    let fio_cmdline = fio_cmd
        .iter()
        .fold(String::new(), |acc, next| format!("{acc} {next}"));
    let composer = cluster.composer().clone();
    let name = cluster.csi_container(0);

    println!("STEP: spawn fio in container");
    tokio::spawn(async move {
        use tokio::sync::oneshot::error::TryRecvError;
        let code = loop {
            let (code, out) = composer.exec(&name, fio_cmd.clone()).await.unwrap();
            println!("{fio_cmdline}: {out}, code: {code:?}");
            if code != Some(0) {
                return code;
            }
            assert_eq!(code, Some(0));

            if stop.try_recv().is_ok() || matches!(stop.try_recv(), Err(TryRecvError::Closed)) {
                break code;
            }
        };

        node.node_unstage_volume(&volume).await.unwrap();
        code
    })
}

/// Similar to the test `republished_nexus_io_engine_txn_fail` but here the
/// republish is spawned on a separate thread so most of the times old nexus thaws
/// earlier than republish and it makes core-agent's txn fail during republish.
#[tokio::test]
async fn republished_nexus_core_agent_txn_fail() {
    let grpc_timeout = Duration::from_millis(512);

    const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_csi(true, true)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("100ms")
        .with_reconcile_period(Duration::from_millis(150), Duration::from_millis(150))
        .with_node_deadline("2s")
        .with_options(|o| {
            o.with_io_engine_env("MAYASTOR_HB_INTERVAL_SEC", "1")
                .with_isolated_io_engine(true)
        })
        .with_req_timeouts_min(true, grpc_timeout, grpc_timeout)
        .build()
        .await
        .unwrap();

    let node_idx0 = cluster.node(0);
    let app_node_idx0 = cluster.csi_node(0);
    let node_idx1 = cluster.node(1);
    let csi_id = cluster.csi_container(0);
    let app_id = cluster.csi_node(0);
    let vol_cli = cluster.grpc_client().volume();

    let volume = vol_cli
        .create(
            &CreateVolume {
                uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
                size: 52428800,
                replicas: 3,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let volume = vol_cli
        .publish(
            &PublishVolume {
                uuid: volume.uuid().clone(),
                share: Some(VolumeShareProtocol::Nvmf),
                target_node: Some(node_idx0.clone()),
                publish_context: HashMap::new(),
                frontend_nodes: vec![app_node_idx0.to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .unwrap();

    let api = cluster.rest_v00();
    let rest_cli = api.volumes_api();
    let volume_grpc = volume.clone();
    let volume_models = rest_cli.get_volume(volume_grpc.uuid()).await.unwrap();

    let (s, r) = tokio::sync::oneshot::channel::<()>();
    let task = run_fio_vol(&cluster, volume_models.clone(), r).await;
    drop(s);
    println!("STEP: started fio...");

    cluster.composer().pause(node_idx0.as_str()).await.unwrap();
    println!("STEP: disconnected container from network...");

    cluster
        .wait_node_status(node_idx0.clone(), NodeStatus::Unknown)
        .await
        .unwrap();
    println!("STEP: node now unknown...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let csi_node_ip = cluster.composer().container_ip(&csi_id);
    let vol_clone = volume.clone();
    let node_idx0_spawn = app_node_idx0.clone();
    let node_idx1_spawn = node_idx1.clone();
    let repub_task = tokio::spawn(async move {
        let mut vol_republished = vol_cli
            .republish(
                &RepublishVolume {
                    uuid: vol_clone.uuid().clone(),
                    target_node: Some(node_idx1_spawn.clone()),
                    share: VolumeShareProtocol::Nvmf,
                    reuse_existing: true,
                    frontend_node: node_idx0_spawn.clone(),
                    reuse_existing_fallback: false,
                },
                None,
            )
            .await;

        if let Err(ref e) = vol_republished {
            match e.kind {
                ReplyErrorKind::FailedPersist if e.source.contains("Etcd Txn Compare failed") => {
                    // try few times so that path is recovered.
                    for _ in 1..5 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        vol_republished = vol_cli
                            .republish(
                                &RepublishVolume {
                                    uuid: vol_clone.uuid().clone(),
                                    target_node: Some(node_idx1_spawn.clone()),
                                    share: VolumeShareProtocol::Nvmf,
                                    reuse_existing: true,
                                    frontend_node: node_idx0_spawn.clone(),
                                    reuse_existing_fallback: false,
                                },
                                None,
                            )
                            .await;
                        if vol_republished.is_ok() {
                            break;
                        }
                    }
                }
                _ => panic!("{}", e.source),
            }
        }

        let vol_republished = vol_republished.unwrap();

        println!("STEP: Republished volume to new node...");

        let socket_path_cp = std::path::PathBuf::from(format!("/var/tmp/csi-{app_id}.sock"));
        let channel =
            tonic::transport::channel::Endpoint::try_from(format!("http://{csi_node_ip}:50051"))
                .expect("local endpoint should be valid")
                .connect_with_connector_lazy(service_fn(move |_: Uri| {
                    let path = socket_path_cp.clone();
                    async {
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                            tokio::net::UnixStream::connect(path).await?,
                        ))
                    }
                }));
        let mut nvme_clnt = nvme_operations_client::NvmeOperationsClient::new(channel);
        let new_path_uri = vol_republished.state().target.unwrap().device_uri;
        let _ = nvme_clnt
            .nvme_connect(NvmeConnectRequest {
                uri: new_path_uri,
                publish_context: Some(MapWrapper {
                    map: HashMap::from([("ioTimeout".into(), "11".into())]),
                }),
            })
            .await;
    });

    cluster.composer().thaw(node_idx0.as_str()).await.unwrap();
    cluster
        .wait_node_status(node_idx0.clone(), NodeStatus::Online)
        .await
        .unwrap();
    println!("STEP: Reconnected older container back to network...");

    let nexus_cli = cluster.grpc_client().nexus();
    let nexuses = nexus_cli
        .get(Filter::Node(node_idx0.clone()), None)
        .await
        .unwrap();
    assert_eq!(nexuses.into_inner().len(), 1);

    repub_task.await.ok();

    tracing::info!("STEP: wait for fio completion...");
    let errcode = task.await.unwrap();
    assert!(errcode.is_some_and(|s| s == 0));
}
