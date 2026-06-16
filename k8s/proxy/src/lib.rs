// #![deny(missing_docs)]
//! A utility library to facilitate connections to a kubernetes cluster via
//! the k8s-proxy library.

use std::path::PathBuf;

mod error;
mod proxy;

/// A [`error::Error`].
pub use error::Error;
use kube::config::KubeConfigOptions;
/// OpenApi client helpers.
pub use proxy::{ConfigBuilder, ForwardingProxy, LokiClient, Scheme, TlsMode};

/// Get the `kube::Config` from the given kubeconfig file, or the default.
pub async fn config_from_kubeconfig(
    kube_config_path: Option<PathBuf>,
    options: KubeConfigOptions,
) -> anyhow::Result<kube::Config> {
    let mut config = match kube_config_path {
        Some(config_path) => {
            // NOTE: Kubeconfig file may hold multiple contexts to communicate
            //       with different kubernetes clusters. We'll pick either the one
            //       specified in the options, or the current-context from the
            //       kubeconfig file.
            let kube_config = kube::config::Kubeconfig::read_from(&config_path)?;
            kube::Config::from_custom_kubeconfig(kube_config, &options).await?
        }

        None => kube::Config::from_kubeconfig(&options).await?,
    };
    config.apply_debug_overrides();
    Ok(config)
}

/// Get the `kube::Client` from the given kubeconfig file, or the default.
pub async fn client_from_kubeconfig(
    kube_config_path: Option<PathBuf>,
    context: Option<String>,
) -> anyhow::Result<kube::Client> {
    let options = kubeconfig_options_from_context(context);
    Ok(kube::Client::try_from(
        config_from_kubeconfig(kube_config_path, options).await?,
    )?)
}

/// Create a `KubeConfigOptions` from the given context.
pub fn kubeconfig_options_from_context(context: Option<String>) -> KubeConfigOptions {
    match context {
        Some(ctx) => KubeConfigOptions {
            context: Some(ctx),
            ..Default::default()
        },
        None => KubeConfigOptions::default(),
    }
}
