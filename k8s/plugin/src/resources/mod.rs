use clap::Parser;
use plugin::resources::{GetResources, ScaleResources};
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
    /// `Dump` resources.
    Dump(DumpArgs),
}
