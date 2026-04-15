use std::os::unix::process::CommandExt;

use crate::infra::{
    async_trait, Builder, ComponentAction, ComposeTest, Error, RwxVm, StartOptions,
};

macro_rules! ws_path {
    ($p:literal) => {
        concat!(env!("WORKSPACE_ROOT"), $p)
    };
    () => {
        env!("WORKSPACE_ROOT")
    };
}
const RWX_VM: &str = ws_path!("/deployer/misc/rwx-vm");
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
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
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
    async fn wait_on(&self, _options: &StartOptions, _cfg: &ComposeTest) -> Result<(), Error> {
        // we let the csi-node-2 and agent-ha-node-2 do the waiting
        Ok(())
    }
}
