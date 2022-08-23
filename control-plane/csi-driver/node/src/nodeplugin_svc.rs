//! Implement services required by the node plugin
//! find volumes provisioned by us
//! freeze and unfreeze filesystem volumes provisioned by us
use crate::{
    dev::{Detach, Device, DeviceError},
    findmnt, mount,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::process::Command;
use tracing::debug;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub(crate) enum ServiceError {
    #[snafu(display("Cannot find volume: volume ID: {}", volid))]
    VolumeNotFound { volid: String },
    #[snafu(display("Invalid volume ID: {}, {}", volid, source))]
    InvalidVolumeId { source: uuid::Error, volid: String },
    #[snafu(display("fsfreeze failed: volume ID: {}, {}", volid, error))]
    FsfreezeFailed { volid: String, error: String },
    #[snafu(display("Internal failure: volume ID: {}, {}", volid, source))]
    InternalFailure { source: DeviceError, volid: String },
    #[snafu(display("IO error: volume ID: {}, {}", volid, source))]
    IoError {
        source: std::io::Error,
        volid: String,
    },
    #[snafu(display("Inconsistent mount filesystems: volume ID: {}", volid))]
    InconsistentMountFs { volid: String },
    #[snafu(display("Not a filesystem mount: volume ID: {}", volid))]
    BlockDeviceMount { volid: String },
}

/// Type of mount.
pub(crate) enum TypeOfMount {
    FileSystem,
    RawBlock,
}

const FSFREEZE: &str = "fsfreeze";

async fn fsfreeze(volume_id: &str, freeze_op: &str) -> Result<(), ServiceError> {
    let uuid = Uuid::parse_str(volume_id).context(InvalidVolumeId {
        volid: volume_id.to_string(),
    })?;

    if let Some(device) = Device::lookup(&uuid).await.context(InternalFailure {
        volid: volume_id.to_string(),
    })? {
        let device_path = device.devname();
        if let Some(mnt) = mount::find_mount(Some(&device_path), None) {
            let dest = mnt.dest.display().to_string();
            let args = [freeze_op, &dest];
            let output = Command::new(FSFREEZE)
                .args(&args)
                .output()
                .await
                .context(Io {
                    volid: volume_id.to_string(),
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
                    volid: volume_id.to_string(),
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
                volid: volume_id.to_string(),
            })?;
            if !mountpaths.is_empty() {
                debug!(
                    "{} for volume_id :{} : failed for block device",
                    freeze_op, volume_id
                );
                return Err(ServiceError::BlockDeviceMount {
                    volid: volume_id.to_string(),
                });
            }
            debug!(
                "{} for volume_id :{} : failed, cannot find volume",
                freeze_op, volume_id
            );
        }
    }
    Err(ServiceError::VolumeNotFound {
        volid: volume_id.to_string(),
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
        volid: volume_id.to_string(),
    })?;

    Device::lookup(&uuid)
        .await
        .context(InternalFailure {
            volid: volume_id.to_string(),
        })?
        .context(VolumeNotFound {
            volid: volume_id.to_string(),
        })
}

/// Find the `TypeOfMount` for the given volume.
pub(crate) async fn find_mount(
    volume_id: &str,
    device: &dyn Detach,
) -> Result<Option<TypeOfMount>, ServiceError> {
    let device_path = device.devname();
    let mountpaths = findmnt::get_mountpaths(&device_path).context(InternalFailure {
        volid: volume_id.to_string(),
    })?;
    debug!("mountpaths for volume_id :{} : {:?}", volume_id, mountpaths);
    if !mountpaths.is_empty() {
        let fstype = mountpaths[0].fstype();
        for devmount in mountpaths {
            if fstype != devmount.fstype() {
                debug!(
                    "Find volume_id :{} : failed, multiple fstypes {}, {}",
                    volume_id,
                    fstype,
                    devmount.fstype()
                );
                // This failure is very unlikely but include for
                // completeness
                return Err(ServiceError::InconsistentMountFs {
                    volid: volume_id.to_string(),
                });
            }
        }
        debug!("fstype for volume_id :{} is {}", volume_id, fstype);
        if fstype == "devtmpfs" {
            Ok(Some(TypeOfMount::RawBlock))
        } else {
            Ok(Some(TypeOfMount::FileSystem))
        }
    } else {
        Ok(None)
    }
}
