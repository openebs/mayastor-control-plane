use clap::Parser;
use plugin::resources::{CordonResources, DrainResources, GetResources, ScaleResources};
use supportability::DumpArgs;

use async_trait::async_trait;
use std::path::PathBuf;

pub mod constant;
pub mod objects;
pub mod upgrade;
pub mod upgradeoperatorclient;

/// The types of operations that are supported.
#[derive(Parser, Debug)]
pub enum Operations {
    /// 'Drain' resources.
    #[clap(subcommand)]
    Drain(DrainResources),
    /// 'Get' resources.
    #[clap(subcommand)]
    Get(GetResources),
    /// 'Scale' resources.
    #[clap(subcommand)]
    Scale(ScaleResources),
    /// 'Cordon' resources.
    #[clap(subcommand)]
    Cordon(CordonResources),
    /// 'Uncordon' resources.
    #[clap(subcommand)]
    Uncordon(CordonResources),
    /// 'Upgrade' resources
    #[clap(subcommand)]
    Upgrade(GetUpgradeResources),
    /// `Dump` resources.
    Dump(DumpArgs),
}

/// The types of resources that support the 'upgrade' operation.
#[derive(clap::Subcommand, Debug)]
pub enum GetUpgradeResources {
    /// Intall the upgrade operator.
    Install,
    /// Apply the upgrade.
    Apply,
    /// Get the upgrade status.
    Get,
    /// Delete the upgrade operator.
    Uninstall,
}

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
