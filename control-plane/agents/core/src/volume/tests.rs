#![cfg(test)]

use mbus_api::{v0::*, *};
use testlib::ClusterBuilder;

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
    test_volume().await;

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

async fn test_volume() {
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
        uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
    }
    .request()
    .await
    .unwrap();

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}
