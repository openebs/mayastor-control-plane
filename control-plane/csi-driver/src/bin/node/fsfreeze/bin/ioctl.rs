use crate::fsfreeze::bin::FsFreezeOpt;
use nix::{errno::Errno, ioctl_readwrite};
use std::{fs, os::fd::AsRawFd, path::PathBuf};
use tracing::error;

// Nix macro to issue FIFREEZE ioctl.
ioctl_readwrite!(fs_freeze, 'X', 119, i32);
// Nix macro to issue FITHAW ioctl.
ioctl_readwrite!(fs_thaw, 'X', 120, i32);

/// Preflight checks to ensure that fsfreeze call would be made in a desirable condition.
pub fn fsfreeze_preflight_check(volume_id: &str, nqn: &str) -> Result<(), Errno> {
    let subsystems: Vec<nvmeadm::nvmf_subsystem::Subsystem> =
        nvmeadm::nvmf_subsystem::Subsystem::try_from_nqn(nqn).map_err(|_| Errno::ENODATA)?;

    if subsystems
        .into_iter()
        .all(|subsystem| subsystem.state == "live")
    {
        return Ok(());
    }
    error!(
        volume.id = volume_id,
        subsystem.nqn = nqn,
        "Preflight check for fsfreeze failed, nvmf subsystem not in desired state"
    );
    Err(Errno::EOPNOTSUPP)
}

/// Issue FIFREEZE and FITHAW ioctl on the mount_path based on the command.
pub async fn fsfreeze_ioctl(command: FsFreezeOpt, mount_path: PathBuf) -> Result<(), Errno> {
    let blocking_task = tokio::task::spawn_blocking(move || {
        let file = fs::File::open(&mount_path).map_err(|error| {
            error!(
                %error,
                mount = %mount_path.display(),
                "Failed to open volume mount path"
            );
            Errno::EBADFD
        })?;
        match command {
            FsFreezeOpt::Freeze => match unsafe { fs_freeze(file.as_raw_fd(), &mut 0) } {
                Ok(_) => Ok(()),
                Err(Errno::EBUSY) => Ok(()),
                Err(errno) => Err(errno),
            },
            FsFreezeOpt::Unfreeze => loop {
                match unsafe { fs_thaw(file.as_raw_fd(), &mut 0) } {
                    Ok(val) => {
                        if val != 0 {
                            return Ok(());
                        }
                    }
                    Err(Errno::EINVAL) => return Ok(()),
                    Err(errno) => return Err(errno),
                }
            },
        }
    });

    blocking_task.await.map_err(|join_error| {
        error!(
            error = join_error.to_string(),
            "Failed to wait for the thread to complete"
        );
        Errno::ESRCH
    })?
}
