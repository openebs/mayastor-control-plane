use common_lib::types::v0::{
    transport as v0,
    transport::{NexusNvmePreemption, NvmeReservation},
};
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::nexus::traits::NexusOperations;
use std::convert::From;

#[tokio::test]
async fn preservation() {
    let size = 20 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_io_engines(3)
        .with_tmpfs_pool(size * 2)
        .with_replicas(1, size, v0::Protocol::Nvmf)
        .with_options(|o| {
            o.with_io_engine_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_fio_spdk(true)
        })
        .build()
        .await
        .unwrap();
    let nexus_client = cluster.grpc_client().nexus();

    let replica = cluster
        .rest_v00()
        .replicas_api()
        .get_replica(&Cluster::replica(2, 0, 0))
        .await
        .unwrap();
    let replica_uri = replica.uri.clone();

    let nexus1 = nexus_client
        .create(
            &v0::CreateNexus {
                node: cluster.node(0),
                uuid: v0::NexusId::new(),
                size,
                children: vec![replica_uri.clone().into()],
                config: Some(v0::NexusNvmfConfig::new(
                    v0::NvmfControllerIdRange::default(),
                    0x12345678911,
                    NvmeReservation::ExclusiveAccess,
                    NexusNvmePreemption::ArgKey(None),
                )),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let share = v0::ShareNexus::from((&nexus1, None, v0::NexusShareProtocol::Nvmf));
    let _nexus1_uri = nexus_client.share(&share, None).await.unwrap();

    let nexus_1_ip = cluster.composer().container_ip(cluster.node(0).as_str());
    let fio_builder = |ip: &str, uuid: &str| {
        let nqn = format!("nqn.2019-05.io.openebs\\:{uuid}");
        let filename =
            format!("--filename=trtype=tcp adrfam=IPv4 traddr={ip} trsvcid=8420 subnqn={nqn} ns=1");
        vec![
            "fio",
            "--name=benchtest",
            filename.as_str(),
            "--direct=1",
            "--rw=randwrite",
            "--ioengine=spdk",
            "--bs=64k",
            "--iodepth=64",
            "--numjobs=1",
            "--thread=1",
            "--size=8M",
            "--norandommap=1",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
    };
    let fio_cmd = fio_builder(&nexus_1_ip, nexus1.uuid.as_str());
    let (code, out) = cluster.composer().exec("fio-spdk", fio_cmd).await.unwrap();
    tracing::info!("{}", out);
    assert_eq!(code, Some(0));

    let nexus2_uuid = v0::NexusId::new();
    let nexus2 = nexus_client
        .create(
            &v0::CreateNexus {
                node: cluster.node(1),
                uuid: nexus2_uuid.clone(),
                size,
                children: vec![replica_uri.into()],
                config: Some(v0::NexusNvmfConfig::new(
                    v0::NvmfControllerIdRange::default(),
                    nexus2_uuid.as_u128() as u64,
                    NvmeReservation::ExclusiveAccess,
                    NexusNvmePreemption::Holder,
                )),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let share = v0::ShareNexus::from((&nexus2, None, v0::NexusShareProtocol::Nvmf));
    let _nexus2_uri = nexus_client.share(&share, None).await.unwrap();

    let nexus_2_ip = cluster.composer().container_ip(cluster.node(1).as_str());
    let fio_cmd = fio_builder(&nexus_2_ip, nexus2.uuid.as_str());
    let (code, out) = cluster.composer().exec("fio-spdk", fio_cmd).await.unwrap();
    tracing::info!("{}", out);
    assert_eq!(code, Some(0));

    let fio_cmd = fio_builder(&nexus_1_ip, nexus1.uuid.as_str());
    let (code, out) = cluster.composer().exec("fio-spdk", fio_cmd).await.unwrap();
    assert_eq!(code, Some(1), "{}", out);
}
