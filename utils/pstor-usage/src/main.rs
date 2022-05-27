//! This `pstore-usage` can be used to sample persistent store (ETCD) usage at runtime from a
//! simulated cluster as well as extrapolate future usage based on the samples. By default, it makes
//! use of the `deployer` library to create a local cluster running on docker.

mod config;
mod etcd;
mod extrapolation;
mod pools;
mod printer;
mod resources;
mod simulation;
mod volumes;

use crate::{
    etcd::Etcd,
    extrapolation::Extrapolation,
    printer::{PrettyPrinter, Printer},
    resources::ResourceUpdates,
    simulation::Simulation,
};

use openapi::apis::Url;

use structopt::StructOpt;

#[derive(structopt::StructOpt, Debug)]
#[structopt(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    /// The rest endpoint if reusing a cluster.
    #[structopt(short, long)]
    rest_url: Option<Url>,

    #[structopt(subcommand)]
    command: Operations,
}

#[derive(StructOpt, Debug)]
enum Operations {
    Simulate(Simulation),
    Extrapolate(Extrapolation),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::from_args();
    match args.command {
        Operations::Simulate(simulation) => {
            let _ = simulation.simulate(&args.rest_url).await?;
        }
        Operations::Extrapolate(mut extrapolation) => {
            extrapolation.extrapolate(&args.rest_url).await?
        }
    }
    Ok(())
}

/// New `prettytable::Cell` aligned to the center.
pub(crate) fn new_cell(string: &str) -> prettytable::Cell {
    prettytable::Cell::new_align(string, prettytable::format::Alignment::CENTER)
}
