use std::os::unix::process::CommandExt;

use crate::infra::{async_trait, Builder, ComponentAction, Error, RwxVm, StartOptions};

macro_rules! ws_path {
    ($p:literal) => {
        concat!(env!("WORKSPACE_ROOT"), $p)
    };
    () => {
        env!("WORKSPACE_ROOT")
    };
}
const RWX_VM: &str = ws_path!("/deployer/misc/rwx-vm");
const RWX_VM_QC: &str = ws_path!("/deployer/misc/rwx-vm/nixos.qcow2");
const RWX_VM_NIX: &str = ws_path!("/deployer/misc/rwx-vm/csi-node-2.nix");

fn is_nixos() -> bool {
    // todo: any advantage is using nixos-rebuild?
    match std::fs::read_to_string("/etc/os-release") {
        Ok(contents) => contents.contains("ID=nixos"),
        Err(_) => false,
    }
}

#[async_trait]
impl ComponentAction for RwxVm {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        if !options.rwx_vm {
            return Ok(cfg);
        }

        let status = if is_nixos() {
            std::process::Command::new("nixos-rebuild")
                .arg("build-vm")
                .arg("-I")
                .arg(format!("nixos-config={RWX_VM_NIX}"))
                .current_dir(RWX_VM)
                .env("WORKSPACE_ROOT", ws_path!())
                .status()?
        } else {
            std::process::Command::new("nix")
                .arg("build")
                .arg("-f")
                .arg("<nixpkgs/nixos>")
                .arg("-I")
                .arg(format!("nixos-config={RWX_VM_NIX}"))
                .arg("config.system.build.vm")
                .arg("--extra-experimental-features")
                .arg("nix-command")
                .arg("--out-link")
                .arg(format!("{RWX_VM}/result"))
                .env("WORKSPACE_ROOT", ws_path!())
                .status()?
        };
        if !status.success() {
            return Err(
                std::io::Error::other(format!("Failed to build the rwx vm: {status:?}")).into(),
            );
        }

        Ok(cfg)
    }
    async fn start(&self, options: &StartOptions, cfg: &crate::ComposeTestNt) -> Result<(), Error> {
        if !options.rwx_vm {
            return Ok(());
        }

        let mut cmd = std::process::Command::new("./result/bin/run-nixos-vm");
        cmd.arg("-nographic")
            .current_dir(RWX_VM)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());

        if cfg.clean() {
            // When clean is set, we don't need to make use of `RwxVm::stop()` to kill the VM
            unsafe {
                cmd.pre_exec(|| {
                    // When parent dies, send SIGTERM to this process
                    nix::libc::prctl(nix::libc::PR_SET_PDEATHSIG, nix::libc::SIGTERM);
                    Ok(())
                })
            };
        }
        cmd.spawn()?;

        Ok(())
    }
    async fn wait_on(
        &self,
        _options: &StartOptions,
        _cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        // we let the csi-node-2 and agent-ha-node-2 do the waiting
        Ok(())
    }
}

use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::fs;

impl RwxVm {
    /// Stops the RWX VM by killing any QEMU processes that have the VM disk open.
    pub fn stop() -> std::io::Result<()> {
        let needle = format!("file={RWX_VM_QC}");

        let mut pids = Vec::new();
        for entry in fs::read_dir("/proc")? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();

            let pid: i32 = match name.parse() {
                Ok(p) => p,
                Err(_) => continue,
            };

            let cmdline_path = format!("/proc/{pid}/cmdline");
            let cmdline_bytes = match fs::read(&cmdline_path) {
                Ok(b) => b,
                Err(_) => continue,
            };

            // cmdline is NUL-separated args; replace with spaces for substring matching.
            let cmdline = String::from_utf8_lossy(&cmdline_bytes);
            if cmdline.contains("qemu-system") && cmdline.contains(&needle) {
                pids.push(pid);
            }
        }

        if pids.is_empty() {
            tracing::info!("No QEMU processes found using {RWX_VM_QC}");
            return Ok(());
        }

        tracing::info!(
            "Found QEMU processes: {}",
            pids.iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(" ")
        );

        for pid in pids {
            kill_pid(pid);
        }

        Ok(())
    }
}

/// Similar to the cleanup script, kill with grace, then with force, and ignore errors.
/// This is to avoid panics in the cleanup code when the process has already exited.
/// kill "$pid" || kill -9 "$pid" || :
fn kill_pid(pid: i32) {
    let nix_pid = Pid::from_raw(pid);

    if signal::kill(nix_pid, Signal::SIGTERM).is_err() {
        // Ignore the error, matching the bash `|| :` fallback semantics.
        let _ = signal::kill(nix_pid, Signal::SIGKILL);
    }
}
