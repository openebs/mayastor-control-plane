use crate::resources::{
    error::Error, utils, CordonResources, DrainResources, GetResources, LabelResources,
    ScaleResources, UnCordonResources,
};
use async_trait::async_trait;

/// Result wrapper for plugin commands.
pub type PluginResult = Result<(), Error>;

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
    Uncordon(UnCordonResources),
    /// 'Label' resources.
    #[clap(subcommand)]
    Label(LabelResources),
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
    ) -> PluginResult;
}

/// Label trait.
/// To be implemented by resources which support the 'label' operation.
#[async_trait(?Send)]
pub trait Label {
    type ID;
    async fn label(
        id: &Self::ID,
        label: String,
        overwrite: bool,
        output: &utils::OutputFormat,
    ) -> PluginResult;
}

/// List trait.
/// To be implemented by resources which support the 'list' operation.
#[async_trait(?Send)]
pub trait List {
    async fn list(output: &utils::OutputFormat) -> PluginResult;
}

/// List trait.
/// To be implemented by resources which support the 'list' operation, with context.
#[async_trait(?Send)]
pub trait ListExt {
    type Context;
    async fn list(output: &utils::OutputFormat, context: &Self::Context) -> PluginResult;
}

/// Get trait.
/// To be implemented by resources which support the 'get' operation.
#[async_trait(?Send)]
pub trait Get {
    type ID;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) -> PluginResult;
}

/// Scale trait.
/// To be implemented by resources which support the 'scale' operation.
#[async_trait(?Send)]
pub trait Scale {
    type ID;
    async fn scale(id: &Self::ID, replica_count: u8, output: &utils::OutputFormat) -> PluginResult;
}

/// Replica topology trait.
/// To be implemented by resources which support the 'replica-topology' operation
#[async_trait(?Send)]
pub trait ReplicaTopology {
    type ID;
    type Context;
    async fn topologies(output: &utils::OutputFormat, context: &Self::Context) -> PluginResult;
    async fn topology(id: &Self::ID, output: &utils::OutputFormat) -> PluginResult;
}

/// Rebuild trait.
/// To be implemented by resources which support the 'rebuild-history' operation
#[async_trait(?Send)]
pub trait RebuildHistory {
    type ID;
    async fn rebuild_history(id: &Self::ID, output: &utils::OutputFormat) -> PluginResult;
}

/// GetBlockDevices trait.
/// To be implemented by resources which support the 'get block-devices' operation
#[async_trait(?Send)]
pub trait GetBlockDevices {
    type ID;
    async fn get_blockdevices(
        id: &Self::ID,
        all: &bool,
        output: &utils::OutputFormat,
    ) -> PluginResult;
}

/// GetSnapshots trait.
/// To be implemented by resources which support the 'get snapshots' operation.
#[async_trait(?Send)]
pub trait GetSnapshots {
    // Representing a volume or replica for exmaple.
    type SourceID;
    // Representing the actual resource i.e. snapshot.
    type ResourceID;
    async fn get_snapshots(
        volid: &Self::SourceID,
        snapid: &Self::ResourceID,
        output: &utils::OutputFormat,
    ) -> PluginResult;
}

/// Cordon trait.
/// To be implemented by resources which support cordoning.
#[async_trait(?Send)]
pub trait Cordoning {
    type ID;
    async fn cordon(id: &Self::ID, label: &str, output: &utils::OutputFormat) -> PluginResult;
    async fn uncordon(id: &Self::ID, label: &str, output: &utils::OutputFormat) -> PluginResult;
}
