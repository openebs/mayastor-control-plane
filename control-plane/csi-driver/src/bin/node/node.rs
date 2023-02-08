use nix::{errno::Errno, sys};
use rpc::csi;
use std::{boxed::Box, collections::HashMap, path::Path, time::Duration, vec::Vec};
use tonic::{Code, Request, Response, Status};
use tracing::{debug, error, info, trace};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use uuid::Uuid;

use crate::{
    block_vol::{publish_block_volume, unpublish_block_volume},
    dev::Device,
    filesystem_vol::{publish_fs_volume, stage_fs_volume, unpublish_fs_volume, unstage_fs_volume},
};
use csi_driver::csi::volume_capability::{access_mode::Mode, AccessType};
use rpc::csi::{
    node_server, node_service_capability, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
    NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
    NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
    NodePublishVolumeRequest, NodePublishVolumeResponse, NodeServiceCapability,
    NodeStageVolumeRequest, NodeStageVolumeResponse, NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, Topology,
    VolumeCapability,
};

/// The Csi Node implementation.
#[derive(Clone, Debug)]
pub(crate) struct Node {
    node_name: String,
    node_selector: HashMap<String, String>,
    filesystems: Vec<String>,
}

impl Node {
    /// Creates new node.
    pub(crate) fn new(
        node_name: String,
        node_selector: HashMap<String, String>,
        filesystems: Vec<String>,
    ) -> Node {
        let self_ = Self {
            node_name,
            node_selector,
            filesystems,
        };
        info!("Node topology segments: {:?}", self_.segments());
        self_
    }
    /// Get the node_name label segment.
    fn node_name_segment(&self) -> (String, String) {
        (
            csi_driver::NODE_NAME_TOPOLOGY_KEY.to_string(),
            self.node_name.clone(),
        )
    }
    /// Get the node selector label segment.
    fn node_selector_segment(&self) -> HashMap<String, String> {
        self.node_selector.clone()
    }
    /// Get the topology segments.
    fn segments(&self) -> HashMap<String, String> {
        self.node_selector_segment()
            .into_iter()
            .chain(vec![self.node_name_segment()])
            .collect()
    }
}

const ATTACH_TIMEOUT_INTERVAL: Duration = Duration::from_millis(100);
const ATTACH_RETRIES: u32 = 100;

// Determine if given access mode in conjunction with ro mount flag makes
// sense or not. If access mode is not supported or the combination does
// not make sense, return error string.
//
// NOTE: Following is based on our limited understanding of access mode
// meaning. Access mode does not control if the mount is rw/ro (that is
// rather part of the mount flags). Access mode serves as advisory info
// for CO when attaching volumes to pods. It is out of scope of storage
// plugin running on particular node to check that access mode for particular
// publish or stage request makes sense.

/// Check that the access_mode from VolumeCapability is consistent with
/// the readonly status
fn check_access_mode(
    volume_capability: &Option<VolumeCapability>,
    readonly: bool,
) -> Result<(), String> {
    match volume_capability {
        Some(capability) => match &capability.access_mode {
            Some(access) => match Mode::from_i32(access.mode) {
                Some(mode) => match mode {
                    Mode::SingleNodeWriter | Mode::MultiNodeSingleWriter => Ok(()),
                    Mode::SingleNodeReaderOnly | Mode::MultiNodeReaderOnly => {
                        if readonly {
                            return Ok(());
                        }
                        Err(format!("volume capability: invalid combination of access mode ({mode:?}) and mount flag (rw)"))
                    }
                    Mode::Unknown => Err(String::from("volume capability: unknown access mode")),
                    _ => Err(format!(
                        "volume capability: unsupported access mode: {mode:?}"
                    )),
                },
                None => Err(format!(
                    "volume capability: invalid access mode: {}",
                    access.mode
                )),
            },
            None => Err(String::from("volume capability: missing access mode")),
        },
        None => Err(String::from("missing volume capability")),
    }
}

/// Retrieve the AccessType from VolumeCapability
fn get_access_type(volume_capability: &Option<VolumeCapability>) -> Result<&AccessType, String> {
    match volume_capability {
        Some(capability) => match &capability.access_type {
            Some(access) => Ok(access),
            None => Err(String::from("volume capability: missing access type")),
        },
        None => Err(String::from("missing volume capability")),
    }
}

/// Detach the nexus device from the system, either at volume unstage,
/// or after failed filesystem mount at volume stage.
async fn detach(uuid: &Uuid, errheader: String) -> Result<(), Status> {
    if let Some(device) = Device::lookup(uuid).await.map_err(|error| {
        failure!(
            Code::Internal,
            "{} error locating device: {}",
            &errheader,
            error
        )
    })? {
        let device_path = device.devname();
        debug!("Detaching device {}", device_path);

        let mounts = crate::mount::find_src_mounts(&device_path, None);
        if !mounts.is_empty() {
            return Err(failure!(
                Code::FailedPrecondition,
                "{} device is still mounted {}: {:?}",
                errheader,
                device_path,
                mounts
            ));
        }

        crate::mount::wait_fs_shutdown(&device_path, None).await?;

        if let Err(error) = device.detach().await {
            return Err(failure!(
                Code::Internal,
                "{} failed to detach device {}: {}",
                errheader,
                device_path,
                error
            ));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl node_server::Node for Node {
    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        let node_id = self.node_name.clone();

        debug!(node.id = node_id, "NodeGetInfo request");

        Ok(Response::new(NodeGetInfoResponse {
            node_id,
            max_volumes_per_node: 0,
            accessible_topology: Some(Topology {
                segments: self.segments(),
            }),
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        let caps = vec![
            node_service_capability::rpc::Type::StageUnstageVolume,
            node_service_capability::rpc::Type::GetVolumeStats,
        ];

        debug!("NodeGetCapabilities request: {:?}", caps);

        // We don't support stage/unstage and expand volume rpcs
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: caps
                .into_iter()
                .map(|c| NodeServiceCapability {
                    r#type: Some(node_service_capability::Type::Rpc(
                        node_service_capability::Rpc { r#type: c as i32 },
                    )),
                })
                .collect(),
        }))
    }

    /// This RPC is called by the CO when a workload that wants to use the
    /// specified volume is placed (scheduled) on a node. The Plugin SHALL
    /// assume that this RPC will be executed on the node where the volume will
    /// be used. If the corresponding Controller Plugin has
    /// PUBLISH_UNPUBLISH_VOLUME controller capability, the CO MUST guarantee
    /// that this RPC is called after ControllerPublishVolume is called for the
    /// given volume on the given node and returns a success. This operation
    /// MUST be idempotent. If the volume corresponding to the volume_id has
    /// already been published at the specified target_path, and is compatible
    /// with the specified volume_capability and readonly flag, the Plugin MUST
    /// reply 0 OK. If this RPC failed, or the CO does not know if it failed or
    /// not, it MAY choose to call NodePublishVolume again, or choose to call
    /// NodeUnpublishVolume. This RPC MAY be called by the CO multiple times on
    /// the same node for the same volume with possibly different target_path
    /// and/or other arguments if the volume has MULTI_NODE capability (i.e.,
    /// access_mode is either MULTI_NODE_READER_ONLY, MULTI_NODE_SINGLE_WRITER
    /// or MULTI_NODE_MULTI_WRITER).
    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_publish_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume: missing volume id"
            ));
        }
        let _guard = csi_driver::limiter::VolumeOpGuard::new_str(&msg.volume_id)?;

        if msg.target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: missing target path",
                &msg.volume_id
            ));
        }

        if let Err(error) = check_access_mode(&msg.volume_capability, msg.readonly) {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: {}",
                &msg.volume_id,
                error
            ));
        }

        // Note that the staging path is NOT optional,
        // as we advertise StageUnstageVolume.
        if msg.staging_target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: missing staging path",
                &msg.volume_id
            ));
        }

        // The CO must ensure that the parent of target path exists,
        // make sure that it exists.
        let target_parent = Path::new(&msg.target_path).parent().unwrap();
        if !target_parent.exists() || !target_parent.is_dir() {
            return Err(Status::new(
                Code::Internal,
                format!(
                    "Failed to find parent dir for mountpoint {}, volume {}",
                    &msg.target_path, &msg.volume_id
                ),
            ));
        }

        match get_access_type(&msg.volume_capability).map_err(|error| {
            failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: {}",
                &msg.volume_id,
                error
            )
        })? {
            AccessType::Mount(mnt) => {
                publish_fs_volume(&msg, mnt, &self.filesystems)?;
            }
            AccessType::Block(_) => {
                publish_block_volume(&msg).await?;
            }
        }
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    /// This RPC is called by the CO when a workload using the specified
    /// volume is removed (unscheduled) from a node.
    /// If the corresponding Controller Plugin has PUBLISH_UNPUBLISH_VOLUME
    /// controller capability, the CO MUST guarantee that this RPC is called
    /// after ControllerPublishVolume is called for the given volume on the
    /// given node and returns a success.
    ///
    /// This operation MUST be idempotent.
    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_unpublish_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unpublish volume: missing volume id"
            ));
        }
        let _guard = csi_driver::limiter::VolumeOpGuard::new_str(&msg.volume_id)?;

        if msg.target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unpublish volume {}: missing target path",
                msg.volume_id
            ));
        }

        // target path will have been created previously in node_publish_volume
        // and is one of
        //  1. a directory for filesystem volumes ,
        //  2. a block special file for block volumes.
        //
        // If it does not exist, then a previously unpublish request has
        // succeeded.
        let target_path = Path::new(&msg.target_path);
        if target_path.exists() {
            if target_path.is_dir() {
                unpublish_fs_volume(&msg)?;
            } else {
                if target_path.is_file() {
                    return Err(Status::new(
                        Code::Unknown,
                        format!(
                            "Failed to unpublish volume {}: {} is a file.",
                            &msg.volume_id, &msg.target_path
                        ),
                    ));
                }

                unpublish_block_volume(&msg)?;
            }
        }
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    /// Get volume stats method evaluates and returns capacity metrics.
    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let msg = request.into_inner();
        trace!("node_get_volume_stats {:?}", msg);
        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume: missing volume id"
            ));
        }
        if msg.volume_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume: missing volume path"
            ));
        }
        let _guard = csi_driver::limiter::VolumeOpGuard::new_str(&msg.volume_id)?;

        let volume_path = Path::new(&msg.volume_path);
        if volume_path.exists() {
            // Check if its a filesystem.
            if volume_path.is_dir() {
                trace!("Getting statfs metrics for : {:?}", volume_path);
                match sys::statfs::statfs(&*msg.volume_path) {
                    Ok(info) => Ok(Response::new(NodeGetVolumeStatsResponse {
                        usage: vec![
                            csi::VolumeUsage {
                                total: info.blocks() as i64 * info.block_size(),
                                unit: csi::volume_usage::Unit::Bytes as i32,
                                available: info.blocks_available() as i64 * info.block_size(),
                                used: (info.blocks() - info.blocks_free()) as i64
                                    * info.block_size(),
                            },
                            csi::VolumeUsage {
                                total: info.files() as i64,
                                unit: csi::volume_usage::Unit::Inodes as i32,
                                available: info.files_free() as i64,
                                used: (info.files() - info.files_free()) as i64,
                            },
                        ],
                        volume_condition: None,
                    })),
                    Err(err) => match err {
                        Errno::ENOENT => Err(Status::new(Code::NotFound, err.to_string())),
                        Errno::EIO => Err(Status::new(Code::Internal, err.to_string())),
                        Errno::ENOSYS => Err(Status::new(Code::Unavailable, err.to_string())),
                        Errno::ENOTDIR => Err(Status::new(Code::Internal, err.to_string())),
                        _ => Err(Status::new(Code::InvalidArgument, err.to_string())),
                    },
                }
            } else {
                Ok(Response::new(NodeGetVolumeStatsResponse {
                    usage: vec![],
                    volume_condition: None,
                }))
            }
        } else {
            Err(Status::new(Code::NotFound, "volume path doesn't exist"))
        }
    }

    async fn node_expand_volume(
        &self,
        request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let msg = request.into_inner();
        error!("Unimplemented {:?}", msg);
        Err(Status::new(Code::Unimplemented, "Method not implemented"))
    }

    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_stage_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume: missing volume id"
            ));
        }

        if msg.staging_target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: missing staging path",
                &msg.volume_id
            ));
        }

        if let Err(error) = check_access_mode(
            &msg.volume_capability,
            // relax the check a bit by pretending all stage mounts are ro
            true,
        ) {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: {}",
                &msg.volume_id,
                error
            ));
        };

        let access_type = match get_access_type(&msg.volume_capability) {
            Ok(accesstype) => accesstype,
            Err(error) => {
                return Err(failure!(
                    Code::InvalidArgument,
                    "Failed to stage volume {}: {}",
                    &msg.volume_id,
                    error
                ));
            }
        };

        let uri = msg.publish_context.get("uri").ok_or_else(|| {
            failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: URI attribute missing from publish context",
                &msg.volume_id
            )
        })?;

        let uuid = Uuid::parse_str(&msg.volume_id).map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to stage volume {}: not a valid UUID: {}",
                &msg.volume_id,
                error
            )
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(uuid)?;

        // Note checking existence of staging_target_path, is delegated to
        // code handling those volume types where it is relevant.

        // All checks complete, now attach, if not attached already.
        debug!("Volume {} has URI {}", &msg.volume_id, uri);

        let mut device = Device::parse(uri).map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to stage volume {}: error parsing URI {}: {}",
                &msg.volume_id,
                uri,
                error
            )
        })?;
        device
            .parse_parameters(&msg.publish_context)
            .await
            .map_err(|error| {
                failure!(
                    Code::InvalidArgument,
                    "Failed to parse storage class parameters for volume {}: {}",
                    &msg.volume_id,
                    error
                )
            })?;

        let device_path = match device.find().await.map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to stage volume {}: error locating device for URI {}: {}",
                &msg.volume_id,
                uri,
                error
            )
        })? {
            Some(devpath) => devpath,
            None => {
                debug!("Attaching volume {}", &msg.volume_id);
                // device.attach is idempotent, so does not restart the attach
                // process
                if let Err(error) = device.attach().await {
                    return Err(failure!(
                        Code::Internal,
                        "Failed to stage volume {}: attach failed: {}",
                        &msg.volume_id,
                        error
                    ));
                }

                let devpath =
                    Device::wait_for_device(&*device, ATTACH_TIMEOUT_INTERVAL, ATTACH_RETRIES)
                        .await
                        .map_err(|error| {
                            failure!(
                                Code::Unavailable,
                                "Failed to stage volume {}: {}",
                                &msg.volume_id,
                                error
                            )
                        })?;

                device.fixup().await.map_err(|error| {
                    failure!(
                        Code::Internal,
                        "Could not set parameters on staged device {}: {}",
                        &msg.volume_id,
                        error
                    )
                })?;

                devpath
            }
        };

        // Attach successful, now stage mount if required.
        match access_type {
            AccessType::Mount(mnt) => {
                if let Err(fsmount_error) =
                    stage_fs_volume(&msg, &device_path, mnt, &self.filesystems).await
                {
                    let mounts = crate::mount::find_src_mounts(&device_path, None);
                    // If the device is mounted elsewhere, don't detach it!
                    if mounts.is_empty() {
                        detach(
                            &uuid,
                            format!(
                                "Failed to stage volume {}: {};",
                                &msg.volume_id, fsmount_error
                            ),
                        )
                        .await?;
                    }
                    return Err(fsmount_error);
                }
            }
            AccessType::Block(_) => {
                // block volumes are not staged
            }
        }

        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_unstage_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unstage volume: missing volume id"
            ));
        }

        if msg.staging_target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unstage volume {}: missing staging path",
                &msg.volume_id
            ));
        }

        debug!("Unstaging volume {}", &msg.volume_id);

        let uuid = Uuid::parse_str(&msg.volume_id).map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to unstage volume {}: not a valid UUID: {}",
                &msg.volume_id,
                error
            )
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(uuid)?;

        // All checks complete, stage unmount if required.

        // unstage_fs_volume checks for mounted filesystems
        // at the staging directory and umounts if any are
        // found.
        unstage_fs_volume(&msg).await?;

        // Sometimes when disconnecting we see page read errors due to ENXIO.
        // There seems to be some race in the kernel when removing a device with queued IOs.
        // While this is not strictly an issue, it may confuse or hide other problems.
        // Sleeping between umount and disconnect seems to alleviate this.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // unmounts (if any) are complete.
        // If the device is attached, detach the device.
        // Device::lookup will return None for nbd devices,
        // this is correct, as the attach for nbd is a no-op.
        detach(
            &uuid,
            format!("Failed to unstage volume {}:", &msg.volume_id),
        )
        .await?;
        info!("Volume {} unstaged", &msg.volume_id);
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }
}
