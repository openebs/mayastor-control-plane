#![cfg(test)]

use deployer_cluster::ClusterBuilder;
use stor_port::types::v0::{openapi::models, transport::VolumeId};

use std::{collections::HashMap, time::Duration};

const VOLUME_SIZE: u64 = 32u64 * 1024 * 1024;
const POOL_SIZE: u64 = 140u64 * 1024 * 1024;
const BYTES_PER_INODE: u64 = 64u64 * 1024;

struct DeviceDisconnect(nvmeadm::NvmeTarget);
impl Drop for DeviceDisconnect {
    fn drop(&mut self) {
        if self.0.disconnect().is_err() {
            std::process::Command::new(env!("SUDO"))
                .args(["nvme", "disconnect-all"])
                .status()
                .unwrap();
        }
    }
}

/// Creates a volume and format using formatOption = bytes-per-inode.
/// Mounts the fs onto the mount path inside csi-node container.
/// Inspects tune2fs output for the device and ensure inode count is correct.
#[tokio::test]
async fn filesystem_volume_format_options() {
    let cache_period = Duration::from_millis(3);
    let reconcile_period = Duration::from_millis(3);
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_tmpfs_pool_ix(0, POOL_SIZE)
        .with_csi(true, true)
        .with_cache_period(&humantime::Duration::from(cache_period).to_string())
        .with_reconcile_period(reconcile_period, reconcile_period)
        .build()
        .await
        .expect("Cluster to be built");

    let api_client = cluster.rest_v00();
    let volumes_api = api_client.volumes_api();
    let vol_id = VolumeId::new();
    let volume = volumes_api
        .put_volume(
            &vol_id,
            models::CreateVolumeBody::new(
                models::VolumePolicy::new(true),
                1,
                VOLUME_SIZE,
                false,
                false,
            ),
        )
        .await
        .expect("Volume to be created");

    let mut node = cluster
        .csi_node_client(0)
        .await
        .expect("To get node client");

    let mut controller = cluster
        .csi_controller_client()
        .await
        .expect("To get controller client");

    let publish_result = controller
        .controller_publish_volume(&volume, &cluster.csi_node(0))
        .await
        .expect("To publish volume");

    let uri = publish_result
        .publish_context
        .get("uri")
        .expect("To get publish uri")
        .to_string();

    let _nvme_io_subsys = DeviceDisconnect(nvmeadm::NvmeTarget::try_from(uri).unwrap());

    let mut volume_context = HashMap::new();
    let flags = format!("-i {BYTES_PER_INODE}");
    volume_context.insert("formatOptions".to_string(), flags);

    let vol = volumes_api
        .get_volume(&vol_id)
        .await
        .expect("To get the volume");

    node.node_stage_volume_fs(
        &vol,
        "ext4",
        publish_result.publish_context.clone(),
        volume_context,
    )
    .await
    .expect("To stage volume");

    node.node_publish_volume_fs(&volume, "ext4", publish_result.publish_context)
        .await
        .expect("To publish volume");

    let path = format!("/var/tmp/target/mount/{}", volume.spec.uuid);
    let expected_inodes = VOLUME_SIZE / BYTES_PER_INODE;
    let composer = cluster.composer().clone();

    // Gets device backing the mountpath.
    let findmnt_args: Vec<&str> = vec!["findmnt", "-n", "-o", "SOURCE", "--target", path.as_str()];
    let (_, device) = composer.exec("csi-node-1", findmnt_args).await.unwrap();

    // Gets tune2fs output of the device.
    let tune2fs_args: Vec<&str> = vec!["tune2fs", "-l", device.trim()];
    let (_, tune2fs) = composer.exec("csi-node-1", tune2fs_args).await.unwrap();

    // We expect actual inode count to be lesser as we don't get the actual device size of the
    // created volume. Its almost 4 to 5 Mb lesser. This affects the number of inode count
    // when we use -i as it creates inode for each block (65k in this test).
    let mut inode_count: u64 = 1000;
    for line in tune2fs.trim().lines() {
        if line.starts_with("Inode count:") {
            let i_count = line.split_whitespace().nth(2).unwrap_or("Unknown");
            inode_count = i_count
                .parse::<u64>()
                .expect("To parse inode count into u64");
            break;
        }
    }

    assert!(
        inode_count < expected_inodes,
        "inode count was greater then expected"
    );

    node.node_unpublish_volume(&volume)
        .await
        .expect("To unpublish volume");
    node.node_unstage_volume(&volume)
        .await
        .expect("To unstage volume");
    controller
        .controller_unpublish_volume(&volume, &cluster.csi_node(0))
        .await
        .expect("To Controller Unpublish volume");
}
