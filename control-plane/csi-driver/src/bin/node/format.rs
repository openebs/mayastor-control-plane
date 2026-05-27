//! Utility function for formatting a device with filesystem
use crate::filesystem_ops::FileSystem;

use tracing::{debug, info};
use uuid::Uuid;

/// Prepare the filesystem before mount, change parameters if requested.
pub(crate) async fn prepare_device(
    fstype: &FileSystem,
    device: &str,
    staging_path: &str,
    options: &[String],
    fs_id: &Option<Uuid>,
    format_options: &str,
) -> Result<(), String> {
    debug!("Probing device {}", device);
    let fs = FileSystem::property(device, "TYPE");

    let fs_ops = fstype.fs_ops()?;

    if let Ok(ref found_fs) = fs {
        debug!("Found existing filesystem ({found_fs}) on device {device}");
        if found_fs != fstype.as_ref() {
            return Err(format!(
                "device {device} has filesystem {found_fs} but {fstype} was requested; cross-filesystem restores are not supported"
            ));
        }
        if let Some(fs_id) = fs_id {
            debug!("Attempting to set uuid for filesystem {fs_id}, device: {device}");
            fs_ops
                .set_uuid_with_repair(device, staging_path, options, fs_id)
                .await?;
        }
        return Ok(());
    }
    info!(%format_options, "Formatting device {device} with filesystem {fstype}");
    fs_ops.create(device, format_options).await
}
