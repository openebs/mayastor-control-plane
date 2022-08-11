#![deny(missing_docs)]
//! A utility library to facilitate connections to a kubernetes cluster via
//! the k8s-proxy library.

use std::{
    env,
    path::{Path, PathBuf},
};

mod proxy;

/// OpenApi client helpers.
pub use proxy::{ConfigBuilder, ForwardingProxy, LokiClient, Scheme};

/// Get the `kube::Config` from the given kubeconfig file, or the default.
pub async fn config_from_kubeconfig(
    kube_config_path: Option<PathBuf>,
) -> anyhow::Result<kube::Config> {
    let file = match kube_config_path {
        Some(config_path) => config_path,
        None => {
            let file_path = match env::var("KUBECONFIG") {
                Ok(value) => Some(value),
                Err(_) => {
                    // Look for kubeconfig file in default location.
                    #[cfg(any(target_os = "linux", target_os = "macos"))]
                    let default_path = format!("{}/.kube/config", env::var("HOME")?);
                    #[cfg(target_os = "windows")]
                    let default_path = format!("{}/.kube/config", env::var("USERPROFILE")?);
                    match Path::new(&default_path).exists() {
                        true => Some(default_path),
                        false => None,
                    }
                }
            };
            if file_path.is_none() {
                return Err(anyhow::anyhow!(
                    "kubeconfig file not found in default location"
                ));
            }
            let mut path = PathBuf::new();
            path.push(file_path.unwrap_or_default());
            path
        }
    };

    // NOTE: Kubeconfig file may hold multiple contexts to communicate
    //       with different kubernetes clusters. We have to pick master
    //       address of current-context config only
    let kube_config = kube::config::Kubeconfig::read_from(&file)?;
    let config = kube::Config::from_custom_kubeconfig(kube_config, &Default::default()).await?;
    Ok(config)
}
