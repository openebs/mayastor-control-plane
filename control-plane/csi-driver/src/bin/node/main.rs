/// todo: cleanup this module cfg repetition..
#[cfg(target_os = "linux")]
mod block_vol;
/// Configuration Parameters.
#[cfg(target_os = "linux")]
pub(crate) mod config;
#[cfg(target_os = "linux")]
mod dev;
#[cfg(target_os = "linux")]
mod error;
/// Filesystem specific operations.
pub(crate) mod filesystem_ops;
#[cfg(target_os = "linux")]
mod filesystem_vol;
#[cfg(target_os = "linux")]
mod findmnt;
#[cfg(target_os = "linux")]
mod format;
#[cfg(target_os = "linux")]
mod identity;
#[cfg(target_os = "linux")]
mod main_;
#[cfg(target_os = "linux")]
mod match_dev;
#[cfg(target_os = "linux")]
mod mount;
#[cfg(target_os = "linux")]
mod node;
#[cfg(target_os = "linux")]
mod nodeplugin_grpc;
#[cfg(target_os = "linux")]
mod nodeplugin_nvme;
#[cfg(target_os = "linux")]
mod nodeplugin_svc;
/// Shutdown event which lets the plugin know it needs to stop processing new events and
/// complete any existing ones before shutting down.
#[cfg(target_os = "linux")]
pub(crate) mod shutdown_event;

#[tokio::main]
#[cfg(target_os = "linux")]
async fn main() -> anyhow::Result<()> {
    main_::main().await.map_err(|error| {
        tracing::error!(%error, "Terminated with error");
        error
    })
}

#[tokio::main]
#[cfg(not(target_os = "linux"))]
async fn main() -> anyhow::Result<()> {
    Ok(())
}
