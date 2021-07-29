#![cfg(test)]

use common_lib::{
    mbus_api,
    mbus_api::{message_bus::v0::Replicas, Message, ReplyError, ReplyErrorKind, ResourceKind},
    store::etcd::Etcd,
    types::v0::{
        message_bus::{
            Child, ChildState, CreateVolume, DestroyVolume, ExplicitTopology, Filter, GetNexuses,
            GetNodes, GetReplicas, GetVolumes, Nexus, NodeId, Protocol, PublishVolume,
            SetVolumeReplica, ShareVolume, Topology, UnpublishVolume, UnshareVolume, Volume,
            VolumeShareProtocol, VolumeState,
        },
        store::{
            definitions::Store,
            nexus_persistence::{NexusInfo, NexusInfoKey},
        },
    },
};
use testlib::{Cluster, ClusterBuilder};

use std::str::FromStr;

#[actix_rt::test]
async fn volume() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_mayastors(3)
        .with_pools(1)
        .with_cache_period("1s")
        .build()
        .await
        .unwrap();

    let nodes = GetNodes {}.request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    test_volume(&cluster).await;
}

async fn test_volume(cluster: &Cluster) {
    smoke_test().await;
    publishing_test(cluster).await;
    replica_count_test().await;
    nexus_persistence_test(cluster).await;
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
        uuid: volume.uuid.clone(),
        // publish it on the remote first, to complicate things
        target_node: Some(remote.clone()),
        share: None,
    }
    .request()
    .await
    .unwrap();

    let nexus = volume.children.first().unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    let nexus_uuid = nexus.uuid.clone();

    UnpublishVolume {
        uuid: volume.uuid.clone(),
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
        filter: Filter::Volume(volume.uuid.clone()),
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
        uuid: volume.uuid.clone(),
        target_node: Some(local.clone()),
        share: None,
    }
    .request()
    .await
    .unwrap();
    tracing::info!("Volume: {:?}", volume);
    let nexus = volume.children.first().unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    assert_eq!(nexus.children.len(), 1);

    let replicas = GetReplicas {
        filter: Filter::Volume(volume.uuid.clone()),
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

    DestroyVolume { uuid: volume.uuid }
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
        uuid: volume.uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect("Should be able to publish a newly created volume");
    tracing::info!("Published on: {}", volume.children.first().unwrap().node);

    let share = ShareVolume {
        uuid: volume.uuid.clone(),
        protocol: Default::default(),
    }
    .request()
    .await
    .unwrap();

    tracing::info!("Share: {}", share);

    ShareVolume {
        uuid: volume.uuid.clone(),
        protocol: Default::default(),
    }
    .request()
    .await
    .expect_err("Can't share a shared volume");

    UnshareVolume {
        uuid: volume.uuid.clone(),
    }
    .request()
    .await
    .expect("Should be able to unshare a shared volume");

    UnshareVolume {
        uuid: volume.uuid.clone(),
    }
    .request()
    .await
    .expect_err("Can't unshare an unshared volume");

    PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect_err("The Volume cannot be published again because it's already published");

    UnpublishVolume {
        uuid: volume.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: Some(cluster.node(0)),
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");
    let nx = volume.children.first().unwrap();
    tracing::info!("Published on '{}' with share '{}'", nx.node, nx.device_uri);

    let volumes = GetVolumes {
        filter: Filter::Volume(volume.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    assert_eq!(volumes.0.first().unwrap().protocol, Protocol::Iscsi);
    assert_eq!(
        volumes.0.first().unwrap().target_node(),
        Some(Some(cluster.node(0)))
    );

    PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: None,
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect_err("The volume is already published");

    UnpublishVolume {
        uuid: volume.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: Some(cluster.node(1)),
        share: None,
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");
    tracing::info!("Published on: {}", volume.children.first().unwrap().node);

    let volumes = GetVolumes {
        filter: Filter::Volume(volume.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    assert_eq!(
        volumes.0.first().unwrap().protocol,
        Protocol::None,
        "Was published but not shared"
    );
    assert_eq!(
        volumes.0.first().unwrap().target_node(),
        Some(Some(cluster.node(1)))
    );

    DestroyVolume { uuid: volume.uuid }
        .request()
        .await
        .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn get_volume(volume: &Volume) -> Volume {
    let request = GetVolumes {
        filter: Filter::Volume(volume.uuid.clone()),
    }
    .request()
    .await
    .unwrap();
    request.into_inner().first().cloned().unwrap()
}

async fn wait_for_volume_online(volume: &Volume) -> Result<Volume, ()> {
    let mut volume = get_volume(volume).await;
    let mut tries = 0;
    while volume.state != VolumeState::Online && tries < 20 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        volume = get_volume(&volume).await;
        tries += 1;
    }
    if volume.state == VolumeState::Online {
        Ok(volume)
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
        uuid: volume.uuid.clone(),
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 3,
    }
    .request()
    .await
    .expect("Should have enough nodes/pools to increase replica count");
    tracing::info!("Volume: {:?}", volume);

    let error = SetVolumeReplica {
        uuid: volume.uuid.clone(),
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

    let volume = wait_for_volume_online(&volume).await.unwrap();

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

    let volume = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 1,
    }
    .request()
    .await
    .expect("Should be able to bring the replica to 1");
    tracing::info!("Volume: {:?}", volume);

    assert_eq!(volume.state, VolumeState::Online);
    assert!(!volume
        .children
        .iter()
        .any(|n| n.children.iter().any(|c| c.state != ChildState::Online)));

    let error = SetVolumeReplica {
        uuid: volume.uuid.clone(),
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
        uuid: volume.uuid.clone(),
        replicas: 2,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back to 2");
    tracing::info!("Volume: {:?}", volume);

    UnpublishVolume {
        uuid: volume.uuid.clone(),
    }
    .request()
    .await
    .unwrap();

    let volume = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 3,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back to 3");
    tracing::info!("Volume: {:?}", volume);

    DestroyVolume { uuid: volume.uuid }
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

    DestroyVolume { uuid: volume.uuid }
        .request()
        .await
        .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}
