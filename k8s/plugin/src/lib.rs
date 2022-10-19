use async_trait::async_trait;
use std::path::PathBuf;

pub mod resources;

/// Upgrade trait.
/// To be implemented by resources which support the 'upgrade' operation.
#[async_trait(?Send)]
pub trait Upgrade {
    async fn install(&self);
    async fn apply(
        &self,
        kube_config: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    );
    async fn get(
        &self,
        kube_config: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    );
    async fn uninstall(&self);
}
