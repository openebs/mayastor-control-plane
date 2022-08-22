#![cfg(test)]

mod capacity;
mod garbage_collection;
mod helpers;
mod hotspare;

use common_lib::{
    store::etcd::Etcd,
    transport_api::{v0::Replicas, ReplyError, ReplyErrorKind, ResourceKind},
    types::v0::{
        openapi::{models, models::NodeStatus},
        store::{
            definitions::Store,
            nexus_persistence::{NexusInfo, NexusInfoKey},
        },
        transport::{
            Child, ChildState, CreateVolume, DestroyVolume, Filter, GetNexuses, GetReplicas,
            GetVolumes, Nexus, NodeId, PublishVolume, SetVolumeReplica, ShareVolume, Topology,
            UnpublishVolume, UnshareVolume, Volume, VolumeId, VolumeShareProtocol, VolumeState,
            VolumeStatus,
        },
    },
};
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    nexus::traits::NexusOperations, node::traits::NodeOperations,
    replica::traits::ReplicaOperations, volume::traits::VolumeOperations,
};
use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    time::Duration,
};

#[tokio::test]
async fn volume() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_io_engines(3)
        .with_pools(1)
        .with_cache_period("1s")
        // don't let the reconcile interfere with the tests
        .with_reconcile_period(Duration::from_secs(1000), Duration::from_secs(1000))
        .build()
        .await
        .unwrap();

    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    test_volume(&cluster).await;
}

#[tracing::instrument(skip(cluster))]
async fn test_volume(cluster: &Cluster) {
    smoke_test(cluster).await;
    publishing_test(cluster).await;
    replica_count_test(cluster).await;
    nexus_persistence_test(cluster).await;
}

const RECONCILE_TIMEOUT_SECS: u64 = 7;

/// Either fault the local replica, the remote, or set the nexus as having an unclean shutdown
#[derive(Debug)]
enum FaultTest {
    Local,
    Remote,
    Unclean,
}

#[tracing::instrument(skip(cluster))]
async fn nexus_persistence_test(cluster: &Cluster) {
    for (local, remote) in &vec![
        (cluster.node(0), cluster.node(1)),
        (cluster.node(1), cluster.node(0)),
    ] {
        for test in vec![FaultTest::Local, FaultTest::Remote, FaultTest::Unclean] {
            nexus_persistence_test_iteration(local, remote, test, cluster).await;
        }
    }
}
async fn nexus_persistence_test_iteration(
    local: &NodeId,
    remote: &NodeId,
    fault: FaultTest,
    cluster: &Cluster,
) {
    let replica_client = cluster.grpc_client().replica();
    let volume_client = cluster.grpc_client().volume();
    let nexus_client = cluster.grpc_client().nexus();
    tracing::debug!("arguments ({:?}, {:?}, {:?})", local, remote, fault);
    let allowed_nodes = vec![local.to_string(), remote.to_string()];
    let preferred_nodes: Vec<String> = vec![];
    let volume_uuid: VolumeId = "6e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap();
    let volume = volume_client
        .create(
            &CreateVolume {
                uuid: volume_uuid.clone(),
                size: 5242880,
                replicas: 2,
                topology: Some(Topology::from(models::Topology::new_all(
                    Some(models::NodeTopology::explicit(
                        models::ExplicitNodeTopology::new(allowed_nodes, preferred_nodes),
                    )),
                    None,
                ))),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    tracing::info!("Volume: {:?}", volume);

    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                // publish it on the remote first, to complicate things
                target_node: Some(remote.clone()),
                share: None,
            },
            None,
        )
        .await
        .unwrap();

    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    let nexus_uuid = nexus.uuid.clone();

    volume_client
        .unpublish(&UnpublishVolume::new(&volume_state.uuid, false), None)
        .await
        .unwrap();

    let mut store = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd.");
    let mut nexus_info: NexusInfo = store
        .get_obj(&NexusInfoKey::new(&Some(volume_uuid.clone()), &nexus_uuid))
        .await
        .unwrap();
    nexus_info.uuid = nexus_uuid.clone();
    nexus_info.volume_uuid = Some(volume_uuid.clone());
    tracing::info!("NexusInfo: {:?}", nexus_info);

    let replicas = replica_client
        .get(Filter::Volume(volume_state.uuid.clone()), None)
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
        .get_obj(&NexusInfoKey::new(&Some(volume_uuid.clone()), &nexus_uuid))
        .await
        .unwrap();
    nexus_info.uuid = nexus_uuid.clone();
    nexus_info.volume_uuid = Some(volume_uuid.clone());
    tracing::info!("NexusInfo: {:?}", nexus_info);

    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume_state.uuid.clone(),
                target_node: Some(local.clone()),
                share: None,
            },
            None,
        )
        .await
        .unwrap();
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    assert_eq!(nexus.children.len(), 1);

    let replicas = replica_client
        .get(Filter::Volume(volume_state.uuid.clone()), None)
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

    volume_client
        .destroy(
            &DestroyVolume {
                uuid: volume_state.uuid,
            },
            None,
        )
        .await
        .expect("Should be able to destroy the volume");

    assert!(volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert!(nexus_client
        .get(GetNexuses::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
    assert!(replica_client
        .get(GetReplicas::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
}

#[tracing::instrument(skip(cluster))]
async fn publishing_test(cluster: &Cluster) {
    let replica_client = cluster.grpc_client().replica();
    let volume_client = cluster.grpc_client().volume();
    let nexus_client = cluster.grpc_client().nexus();
    let volume = volume_client
        .create(
            &CreateVolume {
                uuid: VolumeId::try_from("359b7e1a-b724-443b-98b4-e6d97fabbb40").unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let volumes = volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries;
    tracing::info!("Volumes: {:?}", volumes);
    assert_eq!(Some(&volume), volumes.first());

    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: None,
                share: None,
            },
            None,
        )
        .await
        .expect("Should be able to publish a newly created volume");

    let volume_state = volume.state();

    tracing::info!(
        "Published on: {}",
        volume_state.target.clone().unwrap().node
    );

    let share = volume_client
        .share(
            &ShareVolume {
                uuid: volume_state.uuid.clone(),
                protocol: Default::default(),
            },
            None,
        )
        .await
        .unwrap();

    tracing::info!("Share: {}", share);

    volume_client
        .share(
            &ShareVolume {
                uuid: volume_state.uuid.clone(),
                protocol: Default::default(),
            },
            None,
        )
        .await
        .expect_err("Can't share a shared volume");

    volume_client
        .unshare(
            &UnshareVolume {
                uuid: volume_state.uuid.clone(),
            },
            None,
        )
        .await
        .expect("Should be able to unshare a shared volume");

    volume_client
        .unshare(
            &UnshareVolume {
                uuid: volume_state.uuid.clone(),
            },
            None,
        )
        .await
        .expect_err("Can't unshare an unshared volume");

    volume_client
        .publish(
            &PublishVolume {
                uuid: volume_state.uuid.clone(),
                target_node: None,
                share: None,
            },
            None,
        )
        .await
        .expect_err("The Volume cannot be published again because it's already published");

    volume_client
        .unpublish(&UnpublishVolume::new(&volume_state.uuid, false), None)
        .await
        .unwrap();

    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume_state.uuid.clone(),
                target_node: Some(cluster.node(0)),
                share: Some(VolumeShareProtocol::Nvmf),
            },
            None,
        )
        .await
        .expect("The volume is unpublished so we should be able to publish again");

    tracing::info!("{:#?}", volume);

    let volume_state = volume.state();
    let nx = volume_state.target.unwrap();
    tracing::info!("Published on '{}' with share '{}'", nx.node, nx.device_uri);

    let volumes = volume_client
        .get(Filter::Volume(volume_state.uuid.clone()), None, None)
        .await
        .unwrap();

    let first_volume_state = volumes.entries.first().unwrap().state();
    assert_eq!(
        first_volume_state.target_protocol(),
        Some(VolumeShareProtocol::Nvmf)
    );
    assert_eq!(
        first_volume_state.target_node(),
        Some(Some(cluster.node(0)))
    );

    volume_client
        .publish(
            &PublishVolume {
                uuid: volume_state.uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Iscsi),
            },
            None,
        )
        .await
        .expect_err("The volume publish should fail with Invalid protocol error");

    volume_client
        .publish(
            &PublishVolume {
                uuid: volume_state.uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Nvmf),
            },
            None,
        )
        .await
        .expect_err("The volume is already published");

    volume_client
        .unpublish(&UnpublishVolume::new(&volume_state.uuid, false), None)
        .await
        .unwrap();

    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume_state.uuid.clone(),
                target_node: Some(cluster.node(1)),
                share: None,
            },
            None,
        )
        .await
        .expect("The volume is unpublished so we should be able to publish again");

    let volume_state = volume.state();
    tracing::info!(
        "Published on: {}",
        volume_state.target.clone().unwrap().node
    );

    let volumes = volume_client
        .get(Filter::Volume(volume_state.uuid.clone()), None, None)
        .await
        .unwrap();

    let first_volume_state = volumes.entries.first().unwrap().state();
    assert_eq!(
        first_volume_state.target_protocol(),
        None,
        "Was published but not shared"
    );
    assert_eq!(
        first_volume_state.target_node(),
        Some(Some(cluster.node(1)))
    );

    let target_node = first_volume_state.target_node().flatten().unwrap();
    cluster.composer().kill(target_node.as_str()).await.unwrap();

    volume_client
        .unpublish(&UnpublishVolume::new(&volume_state.uuid, false), None)
        .await
        .expect_err("The node is not online...");

    volume_client
        .unpublish(&UnpublishVolume::new(&volume_state.uuid, true), None)
        .await
        .expect("With force comes great responsibility...");

    cluster
        .composer()
        .start(target_node.as_str())
        .await
        .unwrap();
    wait_for_node_online(cluster, &target_node).await;

    volume_client
        .destroy(
            &DestroyVolume {
                uuid: volume_state.uuid,
            },
            None,
        )
        .await
        .expect("Should be able to destroy the volume");

    assert!(volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert!(nexus_client
        .get(GetNexuses::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
    assert!(replica_client
        .get(GetReplicas::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
}

async fn get_volume(volume: &VolumeState, client: &dyn VolumeOperations) -> Volume {
    let request = client
        .get(Filter::Volume(volume.uuid.clone()), None, None)
        .await
        .unwrap();
    request.entries.first().cloned().unwrap()
}

async fn wait_for_node_online(cluster: &Cluster, node: &NodeId) {
    let client = cluster.rest_v00();

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);
    loop {
        if let Ok(node) = client.nodes_api().get_node(node.as_str()).await {
            let status = node.state.map(|n| n.status).unwrap_or(NodeStatus::Unknown);
            if status == NodeStatus::Online {
                return;
            }
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!("Timeout waiting for the node to become online");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_volume_online(
    volume: &VolumeState,
    client: &dyn VolumeOperations,
) -> Result<VolumeState, ()> {
    let mut volume = get_volume(volume, client).await;
    let mut volume_state = volume.state();
    let mut tries = 0;
    while volume_state.status != VolumeStatus::Online && tries < 20 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        volume = get_volume(&volume_state, client).await;
        volume_state = volume.state();
        tries += 1;
    }
    if volume_state.status == VolumeStatus::Online {
        Ok(volume_state)
    } else {
        Err(())
    }
}

async fn replica_count_test(cluster: &Cluster) {
    let replica_client = cluster.grpc_client().replica();
    let volume_client = cluster.grpc_client().volume();
    let nexus_client = cluster.grpc_client().nexus();
    let volume = volume_client
        .create(
            &CreateVolume {
                uuid: VolumeId::try_from("359b7e1a-b724-443b-98b4-e6d97fabbb40").unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let volumes = volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries;
    tracing::info!("Volumes: {:?}", volumes);
    assert_eq!(Some(&volume), volumes.first());

    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let volume = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume.spec().uuid.clone(),
                replicas: 3,
            },
            None,
        )
        .await
        .expect("Should have enough nodes/pools to increase replica count");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    let error = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume_state.uuid.clone(),
                replicas: 4,
            },
            None,
        )
        .await
        .expect_err("The volume is degraded (rebuild in progress)");
    tracing::error!("error: {:?}", error);
    assert!(matches!(
        error,
        ReplyError {
            kind: ReplyErrorKind::ReplicaIncrease,
            resource: ResourceKind::Volume,
            ..
        },
    ));

    let volume = wait_for_volume_online(&volume_state, &volume_client)
        .await
        .unwrap();

    let error = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume.uuid.clone(),
                replicas: 4,
            },
            None,
        )
        .await
        .expect_err("Not enough pools available");
    tracing::error!("error: {:?}", error);

    assert!(matches!(
        error,
        ReplyError {
            kind: ReplyErrorKind::ResourceExhausted,
            resource: ResourceKind::Pool,
            ..
        },
    ));

    let volume = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume.uuid.clone(),
                replicas: 2,
            },
            None,
        )
        .await
        .expect("Should be able to bring the replica count back down");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    let volume = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume_state.uuid.clone(),
                replicas: 1,
            },
            None,
        )
        .await
        .expect("Should be able to bring the replica to 1");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    assert_eq!(volume_state.status, VolumeStatus::Online);
    assert!(!volume_state
        .target
        .iter()
        .any(|n| n.children.iter().any(|c| c.state != ChildState::Online)));

    let error = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume_state.uuid.clone(),
                replicas: 0,
            },
            None,
        )
        .await
        .expect_err("Can't bring the replica count down to 0");
    tracing::error!("error: {:?}", error);

    assert!(matches!(
        error,
        ReplyError {
            kind: ReplyErrorKind::FailedPrecondition,
            resource: ResourceKind::Volume,
            ..
        },
    ));

    let volume = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume_state.uuid.clone(),
                replicas: 2,
            },
            None,
        )
        .await
        .expect("Should be able to bring the replica count back to 2");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    volume_client
        .unpublish(&UnpublishVolume::new(&volume_state.uuid, false), None)
        .await
        .unwrap();

    let volume = volume_client
        .set_replica(
            &SetVolumeReplica {
                uuid: volume_state.uuid.clone(),
                replicas: 3,
            },
            None,
        )
        .await
        .expect("Should be able to bring the replica count back to 3");
    tracing::info!("Volume: {:?}", volume);

    volume_client
        .destroy(
            &DestroyVolume {
                uuid: volume.spec().uuid,
            },
            None,
        )
        .await
        .expect("Should be able to destroy the volume");

    assert!(volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert!(nexus_client
        .get(GetNexuses::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
    assert!(replica_client
        .get(GetReplicas::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
}

async fn smoke_test(cluster: &Cluster) {
    let replica_client = cluster.grpc_client().replica();
    let volume_client = cluster.grpc_client().volume();
    let nexus_client = cluster.grpc_client().nexus();
    let create_volume = CreateVolume {
        uuid: VolumeId::try_from("359b7e1a-b724-443b-98b4-e6d97fabbb40").unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume_client.create(&create_volume, None).await.unwrap();
    let volumes = volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries;
    tracing::info!("Volumes: {:?}", volumes);

    assert_eq!(Some(&volume), volumes.first());

    volume_client
        .destroy(
            &DestroyVolume {
                uuid: volume.spec().uuid,
            },
            None,
        )
        .await
        .expect("Should be able to destroy the volume");

    assert!(volume_client
        .get(GetVolumes::default().filter, None, None)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert!(nexus_client
        .get(GetNexuses::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
    assert!(replica_client
        .get(GetReplicas::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
}
