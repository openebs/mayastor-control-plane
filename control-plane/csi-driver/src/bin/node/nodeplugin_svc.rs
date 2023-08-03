//! Implement services required by the node plugin
//! find volumes provisioned by us
//! freeze and unfreeze filesystem volumes provisioned by us
use crate::{
    dev::{Detach, Device, DeviceError},
    findmnt, mount,
};
use csi_driver::filesystem::FileSystem as Fs;

use snafu::{OptionExt, ResultExt, Snafu};
use tokio::process::Command;
use tracing::debug;
use uuid::Uuid;

/// Error for the internal node service.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub(crate) enum ServiceError {
    #[snafu(display("Cannot find volume: volume ID: {}", volume_id))]
    VolumeNotFound { volume_id: String },
    #[snafu(display("Invalid volume ID: {}, {}", volume_id, source))]
    InvalidVolumeId {
        source: uuid::Error,
        volume_id: String,
    },
    #[snafu(display("fsfreeze failed: volume ID: {}, {}", volume_id, error))]
    FsfreezeFailed { volume_id: String, error: String },
    #[snafu(display("Internal failure: volume ID: {}, {}", volume_id, source))]
    InternalFailure {
        source: DeviceError,
        volume_id: String,
    },
    #[snafu(display("IO error: volume ID: {}, {}", volume_id, source))]
    IoError {
        source: std::io::Error,
        volume_id: String,
    },
    #[snafu(display("Inconsistent mount filesystems: volume ID: {}", volume_id))]
    InconsistentMountFs { volume_id: String },
    #[snafu(display("Not a filesystem mount: volume ID: {}", volume_id))]
    BlockDeviceMount { volume_id: String },
}

/// Type of mount.
pub(crate) enum TypeOfMount {
    FileSystem,
    RawBlock,
}

const FSFREEZE: &str = "fsfreeze";

async fn fsfreeze(volume_id: &str, freeze_op: &str) -> Result<(), ServiceError> {
    let uuid = Uuid::parse_str(volume_id).context(InvalidVolumeId {
        volume_id: volume_id.to_string(),
    })?;

    if let Some(device) = Device::lookup(&uuid).await.context(InternalFailure {
        volume_id: volume_id.to_string(),
    })? {
        let device_path = device.devname();
        if let Some(mnt) = mount::find_mount(Some(&device_path), None) {
            let dest = mnt.dest.display().to_string();
            let args = [freeze_op, &dest];
            let output = Command::new(FSFREEZE)
                .args(args)
                .output()
                .await
                .context(Io {
                    volume_id: volume_id.to_string(),
                })?;
            return if output.status.success() {
                Ok(())
            } else {
                let errmsg = String::from_utf8(output.stderr).unwrap();
                debug!(
                    "{} for volume_id :{} : failed, {}",
                    freeze_op, volume_id, errmsg
                );
                Err(ServiceError::FsfreezeFailed {
                    volume_id: volume_id.to_string(),
                    error: errmsg,
                })
            };
        } else {
            // mount::find_mount does not return any matches,
            // for mounts which are bind mounts to block devices
            // (raw block volume).
            // It would be incorrect to return the VolumeNotFound error,
            // if the volume is mounted as a raw block volume on this node.
            // Use findmnt to work out if volume is mounted as a raw
            // block, i.e. we get some matches, and return the
            // BlockDeviceMount error.
            let mountpaths = findmnt::get_mountpaths(&device_path).context(InternalFailure {
                volume_id: volume_id.to_string(),
            })?;
            if !mountpaths.is_empty() {
                debug!(
                    "{} for volume_id :{} : failed for block device",
                    freeze_op, volume_id
                );
                return Err(ServiceError::BlockDeviceMount {
                    volume_id: volume_id.to_string(),
                });
            }
            debug!(
                "{} for volume_id :{} : failed, cannot find volume",
                freeze_op, volume_id
            );
        }
    }
    Err(ServiceError::VolumeNotFound {
        volume_id: volume_id.to_string(),
    })
}

pub(crate) async fn freeze_volume(volume_id: &str) -> Result<(), ServiceError> {
    fsfreeze(volume_id, "--freeze").await
}

pub(crate) async fn unfreeze_volume(volume_id: &str) -> Result<(), ServiceError> {
    fsfreeze(volume_id, "--unfreeze").await
}

/// Lookup the device by its volume id.
pub(crate) async fn lookup_device(volume_id: &str) -> Result<Box<dyn Detach>, ServiceError> {
    let uuid = Uuid::parse_str(volume_id).context(InvalidVolumeId {
        volume_id: volume_id.to_string(),
    })?;

    Device::lookup(&uuid)
        .await
        .context(InternalFailure {
            volume_id: volume_id.to_string(),
        })?
        .context(VolumeNotFound {
            volume_id: volume_id.to_string(),
        })
}

/// Find the `TypeOfMount` for the given volume.
pub(crate) async fn find_mount(
    volume_id: &str,
    device: &dyn Detach,
) -> Result<Option<TypeOfMount>, ServiceError> {
    let device_path = device.devname();
    let mountpaths = findmnt::get_mountpaths(&device_path).context(InternalFailure {
        volume_id: volume_id.to_string(),
    })?;
    debug!(
        volume.uuid = volume_id,
        "Mountpaths for volume: {:?}", mountpaths
    );
    if !mountpaths.is_empty() {
        let fstype = mountpaths[0].fstype();
        for devmount in mountpaths {
            if fstype != devmount.fstype() {
                debug!(
                    volume.uuid = volume_id,
                    "Find volume failed, multiple fstypes {}, {}",
                    fstype,
                    devmount.fstype()
                );
                // This failure is very unlikely but include for
                // completeness
                return Err(ServiceError::InconsistentMountFs {
                    volume_id: volume_id.to_string(),
                });
            }
        }
        debug!(volume.uuid = volume_id, ?fstype, "Found fstype for volume");
        if fstype == Fs::DevTmpFs.into() {
            Ok(Some(TypeOfMount::RawBlock))
        } else {
            Ok(Some(TypeOfMount::FileSystem))
        }
    } else {
        Ok(None)
    }
}
