#![cfg(test)]

use crate::volume::helpers::wait_volume_target_node;
use deployer_cluster::{Cluster, ClusterBuilder, CsiNodeClient, FindVolumeRequest};
use grpc::operations::volume::traits::VolumeOperations;
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::transport::{
    CreateVolume, PublishVolume, Volume, VolumeAccessMode, VolumeId, VolumeShareProtocol,
};
use uuid::Uuid;

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
    let context = HashMap::from([
        ("nvmeReconnectDelay".into(), "1".into()),
        ("nvmeKeepAliveTmo".into(), "1".into()),
    ]);

    // source VM
    let vm_1 = cluster.csi_node(1);
    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: Some(cluster.node(0)),
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: context.clone(),
                frontend_nodes: vec![vm_1.to_string()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .unwrap();
    tracing::info!("Staging volume to {vm_1}");
    let target = volume.state().target.unwrap().node;
    let next_target = cluster.node(1);
    assert_ne!(target, next_target);

    let mut node_1 = cluster.csi_node_client_tcp().await.unwrap();
    node_1
        .node_stage_volume_(&volume, context.clone())
        .await
        .unwrap();

    // destination VM
    let vm_2 = cluster.csi_node(0);
    let volume = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: context.clone(),
                frontend_nodes: vec![vm_1.to_string(), vm_2.to_string()],
                access_mode: VolumeAccessMode::MultiNodeMultiWriter,
            },
            None,
        )
        .await
        .unwrap();

    tracing::info!("Staging volume to {vm_2}");
    let mut node_2 = cluster.csi_node_client(0).await.unwrap();
    node_2.node_stage_volume_(&volume, context).await.unwrap();

    // live migration starts

    // workload is ongoing...
    let (s, r) = tokio::sync::oneshot::channel::<()>();
    let join = run_fio_vol(cluster, volume.uuid(), &mut node_2, r).await;

    // Simulate target node loss
    tracing::info!("Simulating node loss by stopping {target}");
    cluster.composer().stop(&target).await.unwrap();
    tracing::info!("Waiting for volume target to switch from {target} to {next_target}");
    wait_volume_target_node(
        cluster,
        volume.uuid(),
        &next_target,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    tracing::info!("Volume target switched from {target} to {next_target}");

    // todo: simulate split-brain....

    // live migration completes, remove vm_1
    tracing::info!("Waiting for volume unstage from vm_1");
    wait_unstage(&mut node_1, &volume).await.unwrap();
    tracing::info!("Volume unstaged from vm_1");

    drop(s);
    let code = join.await.unwrap();

    tracing::info!("Fio completed with {code:?}");

    assert_eq!(code, Some(0));

    // disconnect destination node
    node_2.node_unstage_volume_(&volume).await.unwrap();
}

async fn wait_unstage(
    node: &mut CsiNodeClient,
    volume: &Volume,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();

    let mut result = node.node_unstage_volume_(volume).await.map(drop);

    while start.elapsed() < Duration::from_secs(10) {
        result = node.node_unstage_volume_(volume).await.map(drop);
        if result.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    result
}

async fn run_fio_vol(
    cluster: &Cluster,
    volume: &Uuid,
    node: &mut CsiNodeClient,
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

    let response = node
        .internal()
        .find_volume(FindVolumeRequest {
            volume_id: volume.to_string(),
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
    let name = node.name().to_string();

    println!("STEP: spawn fio in container");
    tokio::spawn(async move {
        use tokio::sync::oneshot::error::TryRecvError;
        loop {
            tracing::info!("Running fio: {fio_cmdline}");
            let (code, out) = composer.exec(&name, fio_cmd.clone()).await.unwrap();
            println!("{fio_cmdline}: {out}, code: {code:?}");
            if code != Some(0) {
                return code;
            }
            assert_eq!(code, Some(0));

            if stop.try_recv().is_ok() || matches!(stop.try_recv(), Err(TryRecvError::Closed)) {
                break code;
            }
        }
    })
}
