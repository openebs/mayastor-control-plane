use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Partition information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct Partition {
    /// devname of parent device to which this partition belongs
    pub parent: String,
    /// partition number
    pub number: u32,
    /// partition name
    pub name: String,
    /// partition scheme: gpt, dos, ...
    pub scheme: String,
    /// partition type identifier
    pub typeid: String,
    /// UUID identifying partition
    pub uuid: String,
}
impl From<Partition> for models::BlockDevicePartition {
    fn from(src: Partition) -> Self {
        models::BlockDevicePartition::new(
            src.parent,
            src.number as i32,
            src.name,
            src.scheme,
            src.typeid,
            src.uuid,
        )
    }
}
impl From<models::BlockDevicePartition> for Partition {
    fn from(src: models::BlockDevicePartition) -> Self {
        Self {
            parent: src.parent,
            number: src.number as u32,
            name: src.name,
            scheme: src.scheme,
            typeid: src.typeid,
            uuid: src.uuid,
        }
    }
}

/// Filesystem information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct Filesystem {
    /// filesystem type: ext3, ntfs, ...
    pub fstype: String,
    /// volume label
    pub label: String,
    /// UUID identifying the volume (filesystem)
    pub uuid: String,
    /// path where filesystem is currently mounted
    pub mountpoint: String,
}
impl From<Filesystem> for models::BlockDeviceFilesystem {
    fn from(src: Filesystem) -> Self {
        models::BlockDeviceFilesystem::new(src.fstype, src.mountpoint, src.label, src.uuid)
    }
}
impl From<models::BlockDeviceFilesystem> for Filesystem {
    fn from(src: models::BlockDeviceFilesystem) -> Self {
        Self {
            fstype: src.fstype,
            label: src.label,
            uuid: src.uuid,
            mountpoint: src.mountpoint,
        }
    }
}

/// Block device information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BlockDevice {
    /// entry in /dev associated with device
    pub devname: String,
    /// currently "disk" or "partition"
    pub devtype: String,
    /// major device number
    pub devmajor: u32,
    /// minor device number
    pub devminor: u32,
    /// device model - useful for identifying mayastor devices
    pub model: String,
    /// official device path
    pub devpath: String,
    /// list of udev generated symlinks by which device may be identified
    pub devlinks: Vec<String>,
    /// size of device in (512 byte) blocks
    pub size: u64,
    /// partition information in case where device represents a partition
    pub partition: Partition,
    /// filesystem information in case where a filesystem is present
    pub filesystem: Filesystem,
    /// identifies if device is available for use (ie. is not "currently" in
    /// use)
    pub available: bool,
}

impl From<BlockDevice> for models::BlockDevice {
    fn from(src: BlockDevice) -> Self {
        models::BlockDevice::new(
            src.available,
            src.devlinks,
            src.devmajor as i32,
            src.devminor as i32,
            src.devname,
            src.devpath,
            src.devtype,
            src.filesystem,
            src.model,
            src.partition,
            src.size as i64,
        )
    }
}
impl From<models::BlockDevice> for BlockDevice {
    fn from(src: models::BlockDevice) -> Self {
        Self {
            devname: src.devname,
            devtype: src.devtype,
            devmajor: src.devmajor as u32,
            devminor: src.devminor as u32,
            model: src.model,
            devpath: src.devpath,
            devlinks: src.devlinks,
            size: src.size as u64,
            partition: src.partition.into(),
            filesystem: src.filesystem.into(),
            available: src.available,
        }
    }
}

/// Get block devices
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetBlockDevices {
    /// id of the mayastor instance
    pub node: NodeId,
    /// specifies whether to get all devices or only usable devices
    pub all: bool,
}
