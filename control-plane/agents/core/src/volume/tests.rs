#![cfg(test)]

use common_lib::{
    mbus_api,
    mbus_api::{message_bus::v0::Replicas, Message, ReplyError, ReplyErrorKind, ResourceKind},
    store::etcd::Etcd,
    types::v0::{
        message_bus::{
            Child, ChildState, CreateReplica, CreateVolume, DestroyVolume, ExplicitTopology,
            Filter, GetNexuses, GetNodes, GetReplicas, GetVolumes, Nexus, NodeId, Protocol,
            PublishVolume, SetVolumeReplica, ShareVolume, Topology, UnpublishVolume, UnshareVolume,
            Volume, VolumeShareProtocol, VolumeState, VolumeStatus,
        },
        store::{
            definitions::Store,
            nexus_persistence::{NexusInfo, NexusInfoKey},
        },
    },
};
use rpc::mayastor::FaultNexusChildRequest;
use testlib::{Cluster, ClusterBuilder};

use common_lib::{
    mbus_api::TimeoutOptions,
    types::v0::{
        message_bus::{
            ChannelVs, ChildUri, DestroyReplica, GetSpecs, Liveness, ReplicaId, ReplicaOwners,
            VolumeId,
        },
        store::{definitions::StorableObject, volume::VolumeSpec},
    },
};
use std::{str::FromStr, time::Duration};

#[actix_rt::test]
async fn volume() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(3)
        .with_pools(1)
        .with_cache_period("1s")
        // don't let the reconcile interfere with the tests
        .with_reconcile_period(Duration::from_secs(1000), Duration::from_secs(1000))
        .build()
        .await
        .unwrap();

    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    test_volume(&cluster).await;
}

async fn test_volume(cluster: &Cluster) {
    smoke_test().await;
    publishing_test(cluster).await;
    replica_count_test().await;
    nexus_persistence_test(cluster).await;
}

const HOTSPARE_RECONCILE_TIMEOUT_SECS: u64 = 7;

#[actix_rt::test]
async fn hotspare() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(3)
        .with_pools(1)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    hotspare_faulty_children(&cluster).await;
    hotspare_unknown_children(&cluster).await;
    hotspare_missing_children(&cluster).await;
    hotspare_replica_count(&cluster).await;
    hotspare_nexus_replica_count(&cluster).await;
}

/// Faults a volume nexus replica and waits for it to be replaced with a new one
async fn hotspare_faulty_children(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state().unwrap().clone();
    let nexus = volume_state.child.unwrap().clone();

    let mut rpc_handle = cluster
        .composer()
        .grpc_handle(cluster.node(0).as_str())
        .await
        .unwrap();

    let children_before_fault = volume_children(volume.uuid()).await;
    tracing::info!("volume children: {:?}", children_before_fault);

    let fault_child = nexus.children.first().unwrap().uri.to_string();
    rpc_handle
        .mayastor
        .fault_nexus_child(FaultNexusChildRequest {
            uuid: nexus.uuid.to_string(),
            uri: fault_child.clone(),
        })
        .await
        .unwrap();

    tracing::debug!(
        "Nexus: {:?}",
        rpc_handle
            .mayastor
            .list_nexus(rpc::mayastor::Null {})
            .await
            .unwrap()
    );

    let children = wait_till_volume_nexus(volume.uuid(), 2, &fault_child).await;
    tracing::info!("volume children: {:?}", children);

    assert_eq!(children.len(), 2);
    // the faulted child should have been replaced!
    assert!(!children.iter().any(|c| c.uri == fault_child));

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Wait for the published volume to have the specified replicas and to not having the specified
/// child. Wait up to the specified timeout.
async fn wait_till_volume_nexus(volume: &VolumeId, replicas: usize, no_child: &str) -> Vec<Child> {
    let timeout = Duration::from_secs(HOTSPARE_RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let volume = GetVolumes::new(volume).request().await.unwrap();
        let volume_state = volume.0.clone().first().unwrap().state().unwrap();
        let nexus = volume_state.child.clone().unwrap();
        let specs = GetSpecs::default().request().await.unwrap();
        let nexus_spec = specs.nexuses.first().unwrap().clone();

        if !nexus.contains_child(&ChildUri::from(no_child))
            && nexus.children.len() == replicas
            && nexus_spec.children.len() == replicas
        {
            return nexus.children;
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified state (replicas: '{}', no_child: '{}')! Current: {:#?}",
                replicas, no_child, volume_state
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Get the children of the specified volume (assumes non ANA)
async fn volume_children(volume: &VolumeId) -> Vec<Child> {
    let volume = GetVolumes::new(volume).request().await.unwrap();
    let volume_state = volume.0.first().unwrap().state().unwrap();
    volume_state.child.unwrap().children
}

/// Adds a child to the volume nexus (under the control plane) and waits till it gets removed
async fn hotspare_unknown_children(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();
    let size_mb = volume.spec().size / 1024 / 1024 + 5;

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state().unwrap().clone();
    let nexus = volume_state.child.unwrap().clone();

    let mut rpc_handle = cluster
        .composer()
        .grpc_handle(cluster.node(0).as_str())
        .await
        .unwrap();

    let children_before_fault = volume_children(volume.uuid()).await;
    tracing::info!("volume children: {:?}", children_before_fault);

    let unknown_replica = format!(
        "malloc:///xxxxx?size_mb={}&uuid={}",
        size_mb,
        ReplicaId::new()
    );

    // todo: this sometimes fails??
    // is the reconciler interleaving with the add_child_nexus?
    rpc_handle
        .mayastor
        .add_child_nexus(rpc::mayastor::AddChildNexusRequest {
            uuid: nexus.uuid.to_string(),
            uri: unknown_replica.clone(),
            norebuild: true,
        })
        .await
        .ok();

    tracing::debug!(
        "Nexus: {:?}",
        rpc_handle
            .mayastor
            .list_nexus(rpc::mayastor::Null {})
            .await
            .unwrap()
    );
    let children = wait_till_volume_nexus(volume.uuid(), 2, &unknown_replica).await;
    tracing::info!("volume children: {:?}", children);

    assert_eq!(children.len(), 2);
    // the unknown child should have been replaced!
    assert!(!children.iter().any(|c| c.uri == unknown_replica));

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Remove a child from a volume nexus (under the control plane) and waits till it gets added back
async fn hotspare_missing_children(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state().unwrap().clone();
    let nexus = volume_state.child.unwrap().clone();

    let mut rpc_handle = cluster
        .composer()
        .grpc_handle(cluster.node(0).as_str())
        .await
        .unwrap();

    let children_before_fault = volume_children(volume.uuid()).await;
    tracing::info!("volume children: {:?}", children_before_fault);

    let missing_child = nexus.children.first().unwrap().uri.to_string();
    rpc_handle
        .mayastor
        .remove_child_nexus(rpc::mayastor::RemoveChildNexusRequest {
            uuid: nexus.uuid.to_string(),
            uri: missing_child.clone(),
        })
        .await
        .unwrap();

    tracing::debug!(
        "Nexus: {:?}",
        rpc_handle
            .mayastor
            .list_nexus(rpc::mayastor::Null {})
            .await
            .unwrap()
    );
    let children = wait_till_volume_nexus(volume.uuid(), 2, &missing_child).await;
    tracing::info!("volume children: {:?}", children);

    assert_eq!(children.len(), 2);
    // the missing child should have been replaced!
    assert!(!children.iter().any(|c| c.uri == missing_child));

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Remove a replica that belongs to a volume. Another should be created.
async fn hotspare_replica_count(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let specs = GetSpecs::default().request().await.unwrap();

    let replica_spec = specs.replicas.first().cloned().unwrap();
    let replicas = GetReplicas::new(&replica_spec.uuid).request().await;
    let replica = replicas.unwrap().0.first().unwrap().clone();

    // forcefully destroy a volume replica
    let mut destroy = DestroyReplica::from(replica);
    destroy.disowners = ReplicaOwners::from_volume(volume.uuid());
    destroy.request().await.unwrap();

    // check we have 2 replicas
    wait_till_volume(volume.uuid(), 2).await;

    // now add one extra replica (it should be removed)
    CreateReplica {
        node: cluster.node(1),
        uuid: ReplicaId::new(),
        pool: cluster.pool(1, 0),
        size: volume.spec().size / 1024 / 1024 + 5,
        thin: false,
        share: Default::default(),
        managed: true,
        owners: ReplicaOwners::from_volume(volume.uuid()),
    }
    .request()
    .await
    .unwrap();

    wait_till_volume(volume.uuid(), 2).await;

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Remove a replica that belongs to a volume. Another should be created.
async fn hotspare_nexus_replica_count(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);

    let timeout_opts = TimeoutOptions::default()
        .with_max_retries(10)
        .with_timeout(Duration::from_millis(500))
        .with_timeout_backoff(Duration::from_millis(50));
    let mut store = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd.");

    let mut volume_spec: VolumeSpec = store.get_obj(&volume.spec().key()).await.unwrap();
    volume_spec.num_replicas += 1;
    tracing::info!("VolumeSpec: {:?}", volume_spec);
    store.put_obj(&volume_spec).await.unwrap();

    cluster.composer().restart("core").await.unwrap();

    Liveness::default()
        .request_on_ext(ChannelVs::Volume, timeout_opts.clone())
        .await
        .expect("Should have restarted by now");

    wait_till_volume_nexus(volume.uuid(), volume_spec.num_replicas as usize, "").await;

    let mut volume_spec: VolumeSpec = store.get_obj(&volume.spec().key()).await.unwrap();
    volume_spec.num_replicas -= 1;
    tracing::info!("VolumeSpec: {:?}", volume_spec);
    store.put_obj(&volume_spec).await.unwrap();

    cluster.composer().restart("core").await.unwrap();
    Liveness::default()
        .request_on_ext(ChannelVs::Volume, timeout_opts)
        .await
        .expect("Should have restarted by now");

    wait_till_volume_nexus(volume.uuid(), volume_spec.num_replicas as usize, "").await;

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Wait for the unpublished volume to have the specified replica count
async fn wait_till_volume(volume: &VolumeId, replicas: usize) {
    let timeout = Duration::from_secs(HOTSPARE_RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        // the volume state does not carry replica information, so inspect the replica spec instead.
        let specs = GetSpecs::default().request().await.unwrap();
        let replica_specs = specs
            .replicas
            .iter()
            .filter(|r| r.owners.owned_by(volume))
            .collect::<Vec<_>>();

        if replica_specs.len() == replicas {
            return;
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified replica ('{}'), current: '{}'",
                replicas, replica_specs.len()
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Either fault the local replica, the remote, or set the nexus as having an unclean shutdown
#[derive(Debug)]
enum FaultTest {
    Local,
    Remote,
    Unclean,
}

async fn nexus_persistence_test(cluster: &Cluster) {
    for (local, remote) in &vec![
        (cluster.node(0), cluster.node(1)),
        (cluster.node(1), cluster.node(0)),
    ] {
        for test in vec![FaultTest::Local, FaultTest::Remote, FaultTest::Unclean] {
            nexus_persistence_test_iteration(local, remote, test).await;
        }
    }
}
async fn nexus_persistence_test_iteration(local: &NodeId, remote: &NodeId, fault: FaultTest) {
    tracing::debug!("arguments ({:?}, {:?}, {:?})", local, remote, fault);

    let volume = CreateVolume {
        uuid: "6e3cf927-80c2-47a8-adf0-95c486bdd7b7".into(),
        size: 5242880,
        replicas: 2,
        topology: Topology {
            labelled: None,
            explicit: Some(ExplicitTopology {
                allowed_nodes: vec![local.clone(), remote.clone()],
                preferred_nodes: vec![],
            }),
        },
        ..Default::default()
    }
    .request()
    .await
    .unwrap();
    tracing::info!("Volume: {:?}", volume);

    let volume = PublishVolume {
        uuid: volume.spec().uuid.clone(),
        // publish it on the remote first, to complicate things
        target_node: Some(remote.clone()),
        share: None,
    }
    .request()
    .await
    .unwrap();

    let volume_state = volume.state().unwrap();
    let nexus = volume_state.child.unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    let nexus_uuid = nexus.uuid.clone();

    UnpublishVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let mut store = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd.");
    let mut nexus_info: NexusInfo = store
        .get_obj(&NexusInfoKey::from(&nexus_uuid))
        .await
        .unwrap();
    nexus_info.uuid = nexus_uuid.clone();
    tracing::info!("NexusInfo: {:?}", nexus_info);

    let replicas = GetReplicas {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let node_child = |node: &NodeId, nexus: &Nexus, replicas: Replicas| {
        let replica = replicas.into_inner().into_iter().find(|r| &r.node == node);
        nexus
            .children
            .iter()
            .find(|c| Some(c.uri.as_str()) == replica.as_ref().map(|r| r.uri.as_str()))
            .cloned()
            .unwrap()
    };

    let mark_child_unhealthy = |c: &Child, ni: &mut NexusInfo| {
        let uri = url::Url::from_str(c.uri.as_str()).unwrap();
        let uuid = uri.query_pairs().find(|(q, _)| q == "uuid").unwrap().1;
        let child_info = ni.children.iter_mut().find(|c| c.uuid == uuid);
        child_info.unwrap().healthy = false;
    };
    match fault {
        FaultTest::Local => {
            let local_child = node_child(local, &nexus, replicas);
            mark_child_unhealthy(&local_child, &mut nexus_info);
        }
        FaultTest::Remote => {
            let remote_child = node_child(remote, &nexus, replicas);
            mark_child_unhealthy(&remote_child, &mut nexus_info);
        }
        FaultTest::Unclean => {
            nexus_info.clean_shutdown = false;
        }
    }
    store.put_obj(&nexus_info).await.unwrap();
    nexus_info = store
        .get_obj(&NexusInfoKey::from(&nexus_uuid))
        .await
        .unwrap();
    nexus_info.uuid = nexus_uuid.clone();
    tracing::info!("NexusInfo: {:?}", nexus_info);

    let volume = PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: Some(local.clone()),
        share: None,
    }
    .request()
    .await
    .unwrap();
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state().unwrap();
    let nexus = volume_state.child.unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    assert_eq!(nexus.children.len(), 1);

    let replicas = GetReplicas {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let child = nexus.children.first().unwrap();
    match fault {
        FaultTest::Local => {
            let remote_child = node_child(remote, &nexus, replicas);
            assert_eq!(child.uri, remote_child.uri);
        }
        FaultTest::Remote => {
            let local_child = node_child(local, &nexus, replicas);
            assert_eq!(child.uri, local_child.uri);
        }
        FaultTest::Unclean => {
            // if the shutdown is not clean, then we prefer the local replica
            let local_child = node_child(local, &nexus, replicas);
            assert_eq!(child.uri, local_child.uri);
        }
    }

    DestroyVolume {
        uuid: volume_state.uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn publishing_test(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume.request().await.unwrap();
    let volumes = GetVolumes::default().request().await.unwrap().0;
    tracing::info!("Volumes: {:?}", volumes);
    assert_eq!(Some(&volume), volumes.first());

    let volume = PublishVolume {
        uuid: volume.spec().uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect("Should be able to publish a newly created volume");

    let volume_state = volume.state().unwrap();

    tracing::info!("Published on: {}", volume_state.child.clone().unwrap().node);

    let share = ShareVolume {
        uuid: volume_state.uuid.clone(),
        protocol: Default::default(),
    }
    .request()
    .await
    .unwrap();

    tracing::info!("Share: {}", share);

    ShareVolume {
        uuid: volume_state.uuid.clone(),
        protocol: Default::default(),
    }
    .request()
    .await
    .expect_err("Can't share a shared volume");

    UnshareVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .expect("Should be able to unshare a shared volume");

    UnshareVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .expect_err("Can't unshare an unshared volume");

    PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect_err("The Volume cannot be published again because it's already published");

    UnpublishVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: Some(cluster.node(0)),
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");

    let volume_state = volume.state().unwrap();
    let nx = volume_state.child.unwrap();
    tracing::info!("Published on '{}' with share '{}'", nx.node, nx.device_uri);

    let volumes = GetVolumes {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let first_volume_state = volumes.0.first().unwrap().state().unwrap();
    assert_eq!(first_volume_state.protocol, Protocol::Iscsi);
    assert_eq!(
        first_volume_state.target_node(),
        Some(Some(cluster.node(0)))
    );

    PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: None,
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect_err("The volume is already published");

    UnpublishVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: Some(cluster.node(1)),
        share: None,
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");

    let volume_state = volume.state().unwrap();
    tracing::info!("Published on: {}", volume_state.child.clone().unwrap().node);

    let volumes = GetVolumes {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let first_volume_state = volumes.0.first().unwrap().state().unwrap();
    assert_eq!(
        first_volume_state.protocol,
        Protocol::None,
        "Was published but not shared"
    );
    assert_eq!(
        first_volume_state.target_node(),
        Some(Some(cluster.node(1)))
    );

    DestroyVolume {
        uuid: volume_state.uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn get_volume(volume: &VolumeState) -> Volume {
    let request = GetVolumes {
        filter: Filter::Volume(volume.uuid.clone()),
    }
    .request()
    .await
    .unwrap();
    request.into_inner().first().cloned().unwrap()
}

async fn wait_for_volume_online(volume: &VolumeState) -> Result<VolumeState, ()> {
    let mut volume = get_volume(volume).await;
    let mut volume_state = volume.state().unwrap();
    let mut tries = 0;
    while volume_state.status != VolumeStatus::Online && tries < 20 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        volume = get_volume(&volume_state).await;
        volume_state = volume.state().unwrap();
        tries += 1;
    }
    if volume_state.status == VolumeStatus::Online {
        Ok(volume_state)
    } else {
        Err(())
    }
}

async fn replica_count_test() {
    let volume = CreateVolume {
        uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume.request().await.unwrap();
    let volumes = GetVolumes::default().request().await.unwrap().0;
    tracing::info!("Volumes: {:?}", volumes);
    assert_eq!(Some(&volume), volumes.first());

    let volume = PublishVolume {
        uuid: volume.spec().uuid.clone(),
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = SetVolumeReplica {
        uuid: volume.spec().uuid.clone(),
        replicas: 3,
    }
    .request()
    .await
    .expect("Should have enough nodes/pools to increase replica count");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state().unwrap();
    let error = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 4,
    }
    .request()
    .await
    .expect_err("The volume is degraded (rebuild in progress)");
    tracing::error!("error: {:?}", error);
    assert!(matches!(
        error,
        mbus_api::Error::ReplyWithError {
            source: ReplyError {
                kind: ReplyErrorKind::ReplicaIncrease,
                resource: ResourceKind::Volume,
                ..
            },
        }
    ));

    let volume = wait_for_volume_online(&volume_state).await.unwrap();

    let error = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 4,
    }
    .request()
    .await
    .expect_err("Not enough pools available");
    tracing::error!("error: {:?}", error);

    assert!(matches!(
        error,
        mbus_api::Error::ReplyWithError {
            source: ReplyError {
                kind: ReplyErrorKind::ResourceExhausted,
                resource: ResourceKind::Pool,
                ..
            },
        }
    ));

    let volume = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 2,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back down");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state().unwrap();
    let volume = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 1,
    }
    .request()
    .await
    .expect("Should be able to bring the replica to 1");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state().unwrap();
    assert_eq!(volume_state.status, VolumeStatus::Online);
    assert!(!volume_state
        .child
        .iter()
        .any(|n| n.children.iter().any(|c| c.state != ChildState::Online)));

    let error = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 0,
    }
    .request()
    .await
    .expect_err("Can't bring the replica count down to 0");
    tracing::error!("error: {:?}", error);

    assert!(matches!(
        error,
        mbus_api::Error::ReplyWithError {
            source: ReplyError {
                kind: ReplyErrorKind::FailedPrecondition,
                resource: ResourceKind::Volume,
                ..
            },
        }
    ));

    let volume = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 2,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back to 2");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state().unwrap();
    UnpublishVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let volume = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 3,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back to 3");
    tracing::info!("Volume: {:?}", volume);

    DestroyVolume {
        uuid: volume.spec().uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn smoke_test() {
    let volume = CreateVolume {
        uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume.request().await.unwrap();
    let volumes = GetVolumes::default().request().await.unwrap().0;
    tracing::info!("Volumes: {:?}", volumes);

    assert_eq!(Some(&volume), volumes.first());

    DestroyVolume {
        uuid: volume.spec().uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}
