use crate::resources::{utils, CordonResources, DrainResources, GetResources, ScaleResources};
use async_trait::async_trait;

/// The types of operations that are supported.
#[derive(clap::Subcommand, Debug)]
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
}

/// Drain trait.
/// To be implemented by resources which support the 'drain' operation.
#[async_trait(?Send)]
pub trait Drain {
    type ID;
    async fn drain(
        id: &Self::ID,
        label: String,
        drain_timeout: Option<humantime::Duration>,
        output: &utils::OutputFormat,
    );
}

/// List trait.
/// To be implemented by resources which support the 'list' operation.
#[async_trait(?Send)]
pub trait List {
    async fn list(output: &utils::OutputFormat);
}

/// Get trait.
/// To be implemented by resources which support the 'get' operation.
#[async_trait(?Send)]
pub trait Get {
    type ID;
    async fn get(id: &Self::ID, output: &utils::OutputFormat);
}

/// Scale trait.
/// To be implemented by resources which support the 'scale' operation.
#[async_trait(?Send)]
pub trait Scale {
    type ID;
    async fn scale(id: &Self::ID, replica_count: u8, output: &utils::OutputFormat);
}

/// Replica topology trait.
/// To be implemented by resources which support the 'replica-topology' operation
#[async_trait(?Send)]
pub trait ReplicaTopology {
    type ID;
    async fn topologies(output: &utils::OutputFormat);
    async fn topology(id: &Self::ID, output: &utils::OutputFormat);
}

/// GetBlockDevices trait.
/// To be implemented by resources which support the 'get block-devices' operation
#[async_trait(?Send)]
pub trait GetBlockDevices {
    type ID;
    async fn get_blockdevices(id: &Self::ID, all: &bool, output: &utils::OutputFormat);
}

/// Cordon trait.
/// To be implemented by resources which support cordoning.
#[async_trait(?Send)]
pub trait Cordoning {
    type ID;
    async fn cordon(id: &Self::ID, label: &str, output: &utils::OutputFormat);
    async fn uncordon(id: &Self::ID, label: &str, output: &utils::OutputFormat);
}
