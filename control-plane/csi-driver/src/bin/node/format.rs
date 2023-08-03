//! Utility function for formatting a device with filesystem
use crate::filesystem_ops::FileSystem;

use tracing::debug;
use uuid::Uuid;

/// Prepare the filesystem before mount, change parameters if requested.
pub(crate) async fn prepare_device(
    fstype: &FileSystem,
    device: &str,
    staging_path: &str,
    options: &[String],
    fs_id: &Option<Uuid>,
) -> Result<(), String> {
    debug!("Probing device {}", device);
    let fs = FileSystem::property(device, "TYPE");

    let fs_ops = fstype.fs_ops()?;

    if let Ok(fs) = fs {
        debug!("Found existing filesystem ({}) on device {}", fs, device);
        if let Some(fs_id) = fs_id {
            debug!("Attempting to set uuid for filesystem {fs_id}, device: {device}");
            fs_ops
                .set_uuid_with_repair(device, staging_path, options, fs_id)
                .await?;
        }
        return Ok(());
    }
    debug!("Creating new filesystem ({}) on device {}", fstype, device);
    fs_ops.create(device).await
}
