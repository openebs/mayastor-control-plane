#![cfg(test)]

use common_lib::{
    mbus_api,
    mbus_api::{Message, ReplyError, ReplyErrorKind, ResourceKind},
    types::v0::message_bus::{
        ChildState, CreateVolume, DestroyVolume, GetNexuses, GetNodes, GetReplicas, GetVolumes,
        PublishVolume, SetVolumeReplica, ShareVolume, UnpublishVolume, UnshareVolume, Volume,
        VolumeShareProtocol, VolumeState,
    },
};
use testlib::{
    v0::{Filter, Protocol},
    Cluster, ClusterBuilder,
};

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

    publishing_test(cluster, &volume).await;
    replica_count_test(cluster, &volume).await;

    DestroyVolume {
        uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn publishing_test(cluster: &Cluster, volume: &Volume) {
    PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect("Should be able to publish a newly created volume");

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

    let uri = PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: Some(cluster.node(0)),
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");
    tracing::info!("Published to: {}", uri);

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

    PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: Some(cluster.node(1)),
        share: None,
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");

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

async fn replica_count_test(_cluster: &Cluster, volume: &Volume) {
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
}
