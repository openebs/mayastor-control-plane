pub(crate) use bin::FsFreezeOpt;
use csi_driver::limiter::VolumeOpGuard;

use nix::errno::Errno;
use std::os::unix::process::ExitStatusExt;
use tokio::process::Command;
use tonic::Status;
use tracing::{debug, error};

pub(crate) mod bin;

// Default csi-node binary name.
const CSI_NODE_BINARY: &str = "csi-node";
// Volume name arg for fs freeze and unfreeze subcommand.
const FREEZE_VOLUME_ID_ARG_NAME: &str = "--volume-id";

/// Creates a child process that calls up the csi-node binary to execute FIFREEZE and FITHAW using
/// the volume id.
pub async fn fsfreeze(volume_id: &str, freeze_op: FsFreezeOpt) -> Result<(), Status> {
    let volume_id = volume_id.to_string();
    let binary_name = std::env::current_exe().unwrap_or(CSI_NODE_BINARY.into());
    tokio::spawn(async move {
        debug!(volume.uuid = volume_id, "{freeze_op}");
        let _guard = VolumeOpGuard::new_str(&volume_id)?;
        match Command::new(binary_name)
            .arg(freeze_op.as_ref())
            .arg(FREEZE_VOLUME_ID_ARG_NAME)
            .arg(&volume_id)
            .output()
            .await
        {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(exitcode_to_status(output)),
            Err(error) => Err(Status::aborted(format!(
                "Failed to execute {} for {}, {}",
                freeze_op, volume_id, error
            ))),
        }
    })
    .await
    .map_err(|e| Status::aborted(e.to_string()))?
}

// Helper function that converts Command output status code to tonic status
fn exitcode_to_status(output: std::process::Output) -> Status {
    let stderr_msg = String::from_utf8(output.stderr).unwrap_or_default();
    error!(stderr_msg);
    match output.status.code() {
        Some(code) if code == Errno::ENODEV as i32 || code == Errno::ENODATA as i32 => {
            Status::not_found(stderr_msg)
        }
        Some(code) if code == Errno::EINVAL as i32 => Status::invalid_argument(stderr_msg),
        Some(code) if code == Errno::EMEDIUMTYPE as i32 => Status::unimplemented(stderr_msg),
        Some(code)
            if code == Errno::EOPNOTSUPP as i32
                || code == Errno::ENOENT as i32
                || code == Errno::ELIBACC as i32
                || code == Errno::EBADFD as i32 =>
        {
            Status::failed_precondition(stderr_msg)
        }
        Some(code) if code == Errno::ESRCH as i32 => Status::aborted(stderr_msg),
        Some(_) => Status::internal(stderr_msg),
        None => Status::aborted(format!(
            "FsFreeze process terminated by {:?} signal",
            output.status.signal()
        )),
    }
}
