use clap::Parser;
use plugin::resources::{CordonResources, GetResources, ScaleResources};
use supportability::DumpArgs;

/// The types of operations that are supported.
#[derive(Parser, Debug)]
pub enum Operations {
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
    /// `Dump` resources.
    Dump(DumpArgs),
}
