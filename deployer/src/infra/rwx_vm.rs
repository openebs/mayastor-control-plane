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
/// Directory where the CI report collects artifacts (see scripts/ci-report.sh).
/// Logs are dropped here (as `*.txt`) so they get bundled into ci-report.tar.gz.
const CI_REPORT_DIR: &str = ws_path!("/ci-report");
/// Console log of the QEMU VM. Useful to debug boot/panic/slow-boot issues on CI.
const RWX_VM_LOG: &str = ws_path!("/ci-report/rwx-vm-console.txt");
/// Log of the nix build of the VM image. Useful to debug build failures on CI.
const RWX_VM_BUILD_LOG: &str = ws_path!("/ci-report/rwx-vm-nix-build.txt");

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

        std::fs::create_dir_all(CI_REPORT_DIR)?;
        let build_log = std::fs::File::create(RWX_VM_BUILD_LOG)?;
        let status = if is_nixos() {
            std::process::Command::new("nixos-rebuild")
                .arg("build-vm")
                .arg("-I")
                .arg(format!("nixos-config={RWX_VM_NIX}"))
                .current_dir(RWX_VM)
                .env("WORKSPACE_ROOT", ws_path!())
                .stdout(build_log.try_clone()?)
                .stderr(build_log)
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
                .stdout(build_log.try_clone()?)
                .stderr(build_log)
                .status()?
        };
        if !status.success() {
            return Err(std::io::Error::other(format!(
                "Failed to build the rwx vm: {status:?}; see {RWX_VM_BUILD_LOG}"
            ))
            .into());
        }

        Ok(cfg)
    }
    async fn start(&self, options: &StartOptions, cfg: &crate::ComposeTestNt) -> Result<(), Error> {
        if !options.rwx_vm {
            return Ok(());
        }

        // QEMU is started with `-machine accel=kvm:tcg`, so a missing/inaccessible
        // /dev/kvm silently falls back to (very slow) software emulation, which is a
        // common cause of the rwx test timing out on CI.
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
        {
            Ok(_) => tracing::info!("KVM acceleration available (/dev/kvm is accessible)"),
            Err(error) => tracing::warn!(
                "KVM not accessible ({error}); QEMU will fall back to TCG software \
                 emulation which is much slower and may cause the rwx test to time out"
            ),
        }

        // Capture the VM console so boot/panic/slow-boot can be inspected (e.g. on CI).
        std::fs::create_dir_all(CI_REPORT_DIR)?;
        let console = std::fs::File::create(RWX_VM_LOG)?;
        tracing::info!("Starting the rwx vm; console log at {RWX_VM_LOG}");

        let mut cmd = std::process::Command::new("./result/bin/run-nixos-vm");
        cmd.arg("-nographic")
            .current_dir(RWX_VM)
            .stdin(std::process::Stdio::null())
            .stdout(console.try_clone()?)
            .stderr(console);

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
