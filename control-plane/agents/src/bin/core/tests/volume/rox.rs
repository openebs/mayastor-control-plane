#![cfg(test)]

use deployer_cluster::ClusterBuilder;
use grpc::operations::volume::traits::VolumeOperations;
use std::{collections::HashMap, time::Duration};
use stor_port::{
    transport_api::{ReplyErrorKind, ResourceKind},
    types::v0::transport::{
        CreateVolume, PublishVolume, UnpublishVolume, VolumeAccessMode, VolumeId,
        VolumeShareProtocol,
    },
};

/// End-to-end plumbing test for read-only (ROX) publishes. Covers:
///
/// - A `MultiNodeReaderOnly` publish sets `TargetConfig.read_only == true` on
///   the volume spec. The resulting nexus records one `allowed_hosts` entry per
///   `frontend_node` passed on the publish, which is the RWX-block multi-
///   initiator fan-out — ROX inherits it without a separate code path.
/// - RWO ↔ ROX mode switches on an already-published volume are rejected with
///   `SvcError::VolumeAccessModeConflict` (→ `FailedPrecondition`).
/// - Unpublish followed by a publish in the other mode succeeds: the guard is
///   about concurrent conflicting modes on one target, not about forbidding
///   transitions.
#[tokio::test]
async fn rox_publish_and_mode_conflict_guard() {
    // `no_deprecated_access_mode(true)` flips core into strict ACL mode; without
    // it the `--deprecated-access-mode` arg makes `frontend_nodes` a no-op and
    // `allowed_hosts` stays empty. NQNs get synthesised from the nodename
    // string via `HostNqn::from_nodename`, so we don't need real csi-node
    // containers.
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_io_engines(2)
        .with_tmpfs_pool(100 * 1024 * 1024)
        .with_cache_period("150ms")
        .with_options(|o| o.no_deprecated_access_mode(true))
        .with_reconcile_period(Duration::from_secs(100), Duration::from_secs(100))
        .build()
        .await
        .unwrap();

    let volume_client = cluster.grpc_client().volume();
    let volume = volume_client
        .create(
            &CreateVolume {
                uuid: VolumeId::try_from("2e3cf927-80c2-47a8-adf0-95c481000000").unwrap(),
                size: 5242880,
                replicas: 2,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // === ROX publish sets read_only + fans allowed_hosts to every frontend_node ===
    let frontends: Vec<String> = vec!["reader-a".into(), "reader-b".into()];
    let published = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: Some(cluster.node(0)),
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: frontends.clone(),
                access_mode: VolumeAccessMode::MultiNodeReaderOnly,
            },
            None,
        )
        .await
        .unwrap();

    assert!(
        published
            .spec_ref()
            .target_cfg()
            .expect("target config present")
            .read_only(),
        "MultiNodeReaderOnly publish must set TargetConfig.read_only"
    );

    // `Nexus::allowed_hosts` returned via gRPC is hardcoded to `vec![]` in the
    // wire→transport conversion, so the CP-observable check is the target's
    // frontend config, which is what actually gets forwarded to `share_nvmf`
    // via `ShareNexus::allowed_hosts` on the io-engine side.
    let cfg_hosts = published
        .spec_ref()
        .target_cfg()
        .expect("target config present")
        .frontend()
        .node_nqns();
    assert_eq!(
        cfg_hosts.len(),
        frontends.len(),
        "each frontend_node must appear in target_cfg.frontend nqns: {cfg_hosts:?}"
    );

    // === Same volume, RWO publish must be rejected while ROX is active ===
    let err = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: vec!["reader-a".into()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect_err("RWO publish on ROX-published volume must fail");
    assert_eq!(err.kind, ReplyErrorKind::FailedPrecondition, "{err:?}");
    assert_eq!(err.resource, ResourceKind::Volume);

    // === Unpublish, then RWO publish succeeds and clears read_only ===
    volume_client
        .unpublish(
            &UnpublishVolume::new(&volume.spec().uuid, false, vec![]),
            None,
        )
        .await
        .unwrap();

    let republished = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: Some(cluster.node(0)),
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: vec!["reader-a".into()],
                access_mode: VolumeAccessMode::SingleNodeWriter,
            },
            None,
        )
        .await
        .expect("RWO publish after unpublish should succeed");
    assert!(
        !republished.spec_ref().target_cfg().unwrap().read_only(),
        "RWO publish must clear read_only"
    );

    // === Mirror guard: ROX on already-RWO volume is rejected ===
    let err = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: None,
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: vec!["reader-a".into()],
                access_mode: VolumeAccessMode::MultiNodeReaderOnly,
            },
            None,
        )
        .await
        .expect_err("ROX publish on RWO-published volume must fail");
    assert_eq!(err.kind, ReplyErrorKind::FailedPrecondition, "{err:?}");

    // === Unpublish, then ROX publish succeeds and sets read_only ===
    volume_client
        .unpublish(
            &UnpublishVolume::new(&volume.spec().uuid, false, vec![]),
            None,
        )
        .await
        .unwrap();

    let republished_rox = volume_client
        .publish(
            &PublishVolume {
                uuid: volume.spec().uuid.clone(),
                target_node: Some(cluster.node(0)),
                share: Some(VolumeShareProtocol::Nvmf),
                publish_context: HashMap::new(),
                frontend_nodes: vec!["reader-a".into()],
                access_mode: VolumeAccessMode::MultiNodeReaderOnly,
            },
            None,
        )
        .await
        .expect("RWO → unpublish → ROX should succeed");
    assert!(
        republished_rox.spec_ref().target_cfg().unwrap().read_only(),
        "post-unpublish ROX must set read_only"
    );
}
