//! This module consists of the various filesystem specific operations utility. Including creation
//! of filesystem, changing the parameter of filesystem like uuid, default mount options for
//! specific filesystem, repairing of the filesystem, retrieving specific properties of the
//! filesystem.

use crate::mount;
use csi_driver::filesystem::FileSystem as Fs;
use devinfo::{blkid::probe::Probe, DevInfoError};
use std::process::Output;
use tokio::process::Command;
use tonic::async_trait;
use tracing::{debug, trace};
use uuid::Uuid;

/// `nouuid` mount flag, to allow duplicate fs uuid.
const XFS_NO_UUID_FLAG: &str = "nouuid";

/// Error type filesystem operations.
type Error = String;

/// Ext4 filesystem type.
pub(crate) struct Ext4Fs;
/// XFS filesystem type.
pub(crate) struct XFs;
/// BTRFS filesystem type.
pub(crate) struct BtrFs;

/// Filesystem type for csi node ops, wrapper over the parent Filesystem enum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileSystem(Fs);

impl From<Fs> for FileSystem {
    fn from(value: Fs) -> Self {
        Self(value)
    }
}

impl AsRef<str> for FileSystem {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for FileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_ref())
    }
}

impl FileSystem {
    /// Convert the top level enumeration to specific filesystem types to access the supported
    /// operations by them.
    pub(crate) fn fs_ops(&self) -> Result<&dyn FileSystemOps, Error> {
        static EXT4FS: Ext4Fs = Ext4Fs {};
        static XFS: XFs = XFs {};
        static BRTFS: BtrFs = BtrFs {};
        match self.0 {
            Fs::Ext4 => Ok(&EXT4FS),
            Fs::Xfs => Ok(&XFS),
            Fs::Btrfs => Ok(&BRTFS),
            _ => Err(format!("Unsupported filesystem {self}")),
        }
    }
    /// Get a specific filesystem property by specifying the device path.
    pub(crate) fn property(device: &str, property: &str) -> Result<String, DevInfoError> {
        let probe = Probe::new_from_filename(device)?;
        probe.do_probe()?;
        probe.lookup_value(property)
    }
}

#[async_trait]
pub(crate) trait FileSystemOps: Send + Sync {
    /// Create the filesystem using its fs util.
    async fn create(&self, device: &str) -> Result<(), Error>;
    /// Get the default mount options along with the user passed options for specific filesystems.
    fn mount_flags(&self, mount_flags: Vec<String>) -> Vec<String>;
    /// Unmount the filesystem if the filesystem uuid and the provided uuid differ.
    fn unmount_on_fs_id_diff(
        &self,
        device_path: &str,
        fs_staging_path: &str,
        volume_uuid: &Uuid,
    ) -> Result<(), Error>;
    /// Repair the filesystem with specific filesystem utility.
    async fn repair(
        &self,
        device: &str,
        staging_path: &str,
        options: &[String],
        volume_uuid: &Uuid,
    ) -> Result<(), Error>;
    /// Set the filesystem uuid.
    async fn set_uuid(&self, device: &str, volume_uuid: &Uuid) -> Result<(), Error>;
    /// Set the filesystem uuid with repair if needed.
    async fn set_uuid_with_repair(
        &self,
        device: &str,
        staging_path: &str,
        options: &[String],
        volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        if self.set_uuid(device, volume_uuid).await.is_err() {
            self.repair(device, staging_path, options, volume_uuid)
                .await?;
            self.set_uuid(device, volume_uuid).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl FileSystemOps for Ext4Fs {
    async fn create(&self, device: &str) -> Result<(), Error> {
        let binary = "mkfs.ext4";
        let output = Command::new(binary)
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;
        ack_command_output(output, binary)
    }

    fn mount_flags(&self, mount_flags: Vec<String>) -> Vec<String> {
        mount_flags
    }

    fn unmount_on_fs_id_diff(
        &self,
        _device_path: &str,
        _fs_staging_path: &str,
        _volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn repair(
        &self,
        device: &str,
        _staging_path: &str,
        _options: &[String],
        _volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        let binary = "e2fsck".to_string();
        let output = Command::new(&binary)
            .arg("-y")
            .arg("-f")
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;

        trace!(
            "Output from {} command: {}, status code: {:?}",
            binary,
            String::from_utf8(output.stdout.clone()).unwrap(),
            output.status.code()
        );

        // For ext4 fs repair using e2fsck the success condition is when status code is 0 or 1.
        if !output.status.success() && output.status.code() != Some(1) {
            return Err(format!(
                "{} command failed: {}",
                binary,
                String::from_utf8(output.stderr).unwrap()
            ));
        }
        Ok(())
    }

    async fn set_uuid(&self, device: &str, volume_uuid: &Uuid) -> Result<(), Error> {
        if let Ok(probed_uuid) = FileSystem::property(device, "UUID") {
            if probed_uuid == volume_uuid.to_string() {
                return Ok(());
            }
        }

        let binary = "tune2fs".to_string();
        let output = Command::new(&binary)
            .arg("-U")
            .arg(volume_uuid.to_string())
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;

        if !output.status.success() {
            return Err(format!(
                "{} command failed: {}",
                binary,
                String::from_utf8(output.stderr).unwrap()
            ));
        }

        let probe_uuid = FileSystem::property(device, "UUID")
            .map_err(|error| format!("Failed to get UUID of device {device}: {error}"))?;

        if volume_uuid.to_string() != probe_uuid {
            return Err(format!(
                "failed to set filesystem uuid using : {binary}, {}",
                String::from_utf8(output.stdout).unwrap_or_default()
            ));
        }
        debug!("Changed filesystem uuid to {volume_uuid} for {device}");
        Ok(())
    }
}

#[async_trait]
impl FileSystemOps for XFs {
    async fn create(&self, device: &str) -> Result<(), Error> {
        let binary = "mkfs.xfs";
        let args = match std::env::var("MKFS_XFS_ARGS") {
            Ok(args) => args
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>(),
            _ => vec![],
        };
        let output = Command::new(binary)
            .args(args)
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;
        ack_command_output(output, binary)
    }

    fn mount_flags(&self, mount_flags: Vec<String>) -> Vec<String> {
        let mut mount_flags = mount_flags;
        if !mount_flags.contains(&XFS_NO_UUID_FLAG.to_string()) {
            mount_flags.push(XFS_NO_UUID_FLAG.to_string())
        }
        mount_flags
    }

    fn unmount_on_fs_id_diff(
        &self,
        device_path: &str,
        fs_staging_path: &str,
        volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        mount::unmount_on_fs_id_diff(device_path, fs_staging_path, volume_uuid)
    }

    /// Xfs filesystem needs an unmount to clear the log, so that the parameters can be changed.
    /// Mount the filesystem to a defined path and then unmount it.
    async fn repair(
        &self,
        device: &str,
        staging_path: &str,
        options: &[String],
        volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        mount::filesystem_mount(device, staging_path, &FileSystem(Fs::Xfs), options).map_err(|error| {
            format!(
                "(xfs repairing) Failed to mount device {device} onto {staging_path} for {volume_uuid} : {error}",
            )
        })?;
        mount::filesystem_unmount(staging_path).map_err(|error| {
            format!(
                "(xfs repairing) Failed to unmount device {device} from {staging_path} for {volume_uuid} : {error}",
            )
        })
    }

    async fn set_uuid(&self, device: &str, volume_uuid: &Uuid) -> Result<(), Error> {
        if let Ok(probed_uuid) = FileSystem::property(device, "UUID") {
            if probed_uuid == volume_uuid.to_string() {
                return Ok(());
            }
        }

        let binary = "xfs_admin".to_string();
        let output = Command::new(&binary)
            .arg("-U")
            .arg(volume_uuid.to_string())
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;

        if !output.status.success() {
            return Err(format!(
                "{} command failed: {}",
                binary,
                String::from_utf8(output.stderr).unwrap()
            ));
        }

        let probe_uuid = FileSystem::property(device, "UUID")
            .map_err(|error| format!("Failed to get UUID of device {device}: {error}"))?;

        if volume_uuid.to_string() != probe_uuid {
            return Err(format!(
                "failed to set filesystem uuid using : {binary}, {}",
                String::from_utf8(output.stdout).unwrap_or_default()
            ));
        }
        debug!("Changed filesystem uuid to {volume_uuid} for {device}");
        Ok(())
    }
}

#[async_trait]
impl FileSystemOps for BtrFs {
    async fn create(&self, device: &str) -> Result<(), Error> {
        let binary = "mkfs.btrfs";
        let output = Command::new(binary)
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;
        ack_command_output(output, binary)
    }

    fn mount_flags(&self, mount_flags: Vec<String>) -> Vec<String> {
        mount_flags
    }

    fn unmount_on_fs_id_diff(
        &self,
        device_path: &str,
        fs_staging_path: &str,
        volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        mount::unmount_on_fs_id_diff(device_path, fs_staging_path, volume_uuid)
    }

    /// `btrfs check --readonly` is a not a `DANGEROUS OPTION` as it only exists to calm potential
    /// panic when users are going to run the checker and doesn't try to attempt to fix problems.
    async fn repair(
        &self,
        device: &str,
        _staging_path: &str,
        _options: &[String],
        _volume_uuid: &Uuid,
    ) -> Result<(), Error> {
        let binary = "btrfs".to_string();
        let output = Command::new(&binary)
            .arg("check")
            .arg("--readonly")
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;

        trace!(
            "Output from {} command: {}, status code: {:?}",
            binary,
            String::from_utf8(output.stdout.clone()).unwrap(),
            output.status.code()
        );

        if !output.status.success() {
            return Err(format!(
                "{} command failed: {}",
                binary,
                String::from_utf8(output.stderr).unwrap()
            ));
        }
        Ok(())
    }

    async fn set_uuid(&self, device: &str, volume_uuid: &Uuid) -> Result<(), Error> {
        if let Ok(probed_uuid) = FileSystem::property(device, "UUID") {
            if probed_uuid == volume_uuid.to_string() {
                return Ok(());
            }
        }

        let binary = "btrfstune".to_string();
        let output = Command::new(&binary)
            .arg("-M")
            .arg(volume_uuid.to_string())
            .arg(device)
            .output()
            .await
            .map_err(|error| format!("failed to execute {binary}: {error}"))?;

        if !output.status.success() {
            return Err(format!(
                "{} command failed: {}",
                binary,
                String::from_utf8(output.stderr).unwrap()
            ));
        }

        let probe_uuid = FileSystem::property(device, "UUID")
            .map_err(|error| format!("Failed to get UUID of device {device}: {error}"))?;

        if volume_uuid.to_string() != probe_uuid {
            return Err(format!(
                "failed to set filesystem uuid using : {binary}, {}",
                String::from_utf8(output.stdout).unwrap_or_default()
            ));
        }
        debug!("Changed filesystem uuid to {volume_uuid} for {device}");
        Ok(())
    }
}

// Acknowledge the output from Command.
fn ack_command_output(output: Output, binary: &str) -> Result<(), Error> {
    trace!(
        "Output from {} command: {}, status code: {:?}",
        binary,
        String::from_utf8(output.stdout.clone()).unwrap(),
        output.status.code()
    );

    if output.status.success() {
        return Ok(());
    }

    Err(format!(
        "{} command failed: {}",
        binary,
        String::from_utf8(output.stderr).unwrap()
    ))
}
