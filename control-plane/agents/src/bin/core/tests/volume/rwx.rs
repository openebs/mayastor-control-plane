#![cfg(test)]

use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::volume::traits::VolumeOperations;
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::transport::{
    CreateVolume, PublishVolume, VolumeAccessMode, VolumeId, VolumeShareProtocol,
};

#[tokio::test]
async fn rwx() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(2)
        .with_tmpfs_pool(100 * 1024 * 1024)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .with_csi(false, true)
        .with_agents(vec!["Core", "HaNode", "HaCluster"])
        .with_rwx_vm(true)
        .with_app_nodes(2)
        .build()
        .await
        .unwrap();

    test_migrate(&cluster).await;
}

#[tracing::instrument(skip(cluster))]
async fn test_migrate(cluster: &Cluster) {
    tracing::info!("Migrating...");

    let volume_client = cluster.grpc_client().volume();
    let volume = volume_client
        .create(
            &CreateVolume {
                uuid: VolumeId::try_from("ec4e66fd-3b33-4439-b504-d49aba53da26").unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // source VM
    let vm_1 = cluster.csi_node(0);
    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: vec![vm_1.to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .unwrap();
    tracing::info!("Staging volume to {vm_1}");
    let mut node_1 = cluster.csi_node_client(0).await.unwrap();
    node_1
        .node_stage_volume_(&volume, HashMap::default())
        .await
        .unwrap();

    // destination VM
    let vm_2 = cluster.csi_node(1);
    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: vec![vm_1.to_string(), vm_2.to_string()],
                access_mode: VolumeAccessMode::MultiNodeMultiWriter,
            },
            None,
        )
        .await
        .unwrap();

    tracing::info!("Staging volume to {vm_2}");
    let mut node_2 = cluster.csi_node_client_tcp().await.unwrap();
    node_2
        .node_stage_volume_(&volume, HashMap::default())
        .await
        .unwrap();

    // live migration starts

    // todo: simulate node restarts, split-brain....

    // live migration completes

    // disconnect source node...
    node_1.node_unstage_volume_(&volume).await.unwrap();

    // todo: some disruption on destination node

    // disconnect destination node
    node_2.node_unstage_volume_(&volume).await.unwrap();
}
