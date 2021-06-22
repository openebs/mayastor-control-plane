#![cfg(test)]

use mbus_api::Message;
use testlib::{
    v0::{Filter, Protocol},
    Cluster, ClusterBuilder,
};
use types::v0::message_bus::mbus::{
    CreatePool, CreateVolume, DestroyVolume, GetNexuses, GetNodes, GetPools, GetReplicas,
    GetVolumes, PublishVolume, ShareVolume, UnpublishVolume, UnshareVolume, VolumeShareProtocol,
};

#[actix_rt::test]
async fn volume() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_mayastors(2)
        .build()
        .await
        .unwrap();

    let mayastor = cluster.node(0).to_string();
    let mayastor2 = cluster.node(1).to_string();

    let nodes = GetNodes {}.request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    prepare_pools(&mayastor, &mayastor2).await;
    test_volume(&cluster).await;

    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
}

async fn prepare_pools(mayastor: &str, mayastor2: &str) {
    CreatePool {
        node: mayastor.into(),
        id: "pooloop".into(),
        disks: vec!["malloc:///disk0?size_mb=100".into()],
    }
    .request()
    .await
    .unwrap();

    CreatePool {
        node: mayastor2.into(),
        id: "pooloop2".into(),
        disks: vec!["malloc:///disk0?size_mb=100".into()],
    }
    .request()
    .await
    .unwrap();

    let pools = GetPools::default().request().await.unwrap();
    tracing::info!("Pools: {:?}", pools);
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

    PublishVolume {
        uuid: volume.uuid.clone(),
        target_node: Some(cluster.node(0)),
        share: Some(VolumeShareProtocol::Iscsi),
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
        Protocol::Off,
        "Was published but not shared"
    );
    assert_eq!(
        volumes.0.first().unwrap().target_node(),
        Some(Some(cluster.node(1)))
    );

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
