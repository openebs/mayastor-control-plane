#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

mod operations;
mod resources;
mod rest_wrapper;

use crate::{
    operations::{Get, List, ReplicaTopology, Scale},
    resources::{node, pool, utils, volume, GetResources, ScaleResources},
    rest_wrapper::RestClient,
};
use anyhow::Result;
use openapi::tower::client::Url;
use operations::Operations;
use std::{env, path::Path};
use structopt::StructOpt;
use yaml_rust::YamlLoader;

#[derive(StructOpt, Debug)]
struct CliArgs {
    /// The rest endpoint, parsed from KUBECONFIG, if left empty .
    #[structopt(global = true, long, short)]
    rest: Option<Url>,
    /// The operation to be performed.
    #[structopt(subcommand)]
    operations: Operations,
    /// The Output, viz yaml, json.
    #[structopt(global = true, default_value = "none", short, long, possible_values=&["yaml", "json", "none"], parse(from_str))]
    output: utils::OutputFormat,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::from_args()
    }
}

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[tokio::main]
async fn main() {
    init_tracing();
    let cli_args = &CliArgs::args();

    // Initialise the REST client.
    if let Err(e) = init_rest(cli_args.rest.as_ref()) {
        println!("Failed to initialise the REST client. Error {}", e);
    }

    // Perform the operations based on the subcommand, with proper output format.
    match &cli_args.operations {
        Operations::Get(resource) => match resource {
            GetResources::Volumes => volume::Volumes::list(&cli_args.output).await,
            GetResources::Volume { id } => volume::Volume::get(id, &cli_args.output).await,
            GetResources::VolumeReplicaTopology { id } => {
                volume::Volume::topology(id, &cli_args.output).await
            }
            GetResources::Pools => pool::Pools::list(&cli_args.output).await,
            GetResources::Pool { id } => pool::Pool::get(id, &cli_args.output).await,
            GetResources::Nodes => node::Nodes::list(&cli_args.output).await,
            GetResources::Node { id } => node::Node::get(id, &cli_args.output).await,
        },
        Operations::Scale(resource) => match resource {
            ScaleResources::Volume { id, replica_count } => {
                volume::Volume::scale(id, *replica_count, &cli_args.output).await
            }
        },
    };
}

/// Initialise the REST client.
fn init_rest(url: Option<&Url>) -> Result<()> {
    // Use the supplied URL if there is one otherwise obtain one from the kubeconfig file.
    let url = match url {
        Some(url) => url.clone(),
        None => url_from_kubeconfig()?,
    };
    RestClient::init(&url)
}

/// Get the URL of the master node from the kubeconfig file.
fn url_from_kubeconfig() -> Result<Url> {
    // Search for the kubeconfig.
    // First look at the environment variable then look in the default directory.
    let file = match env::var("KUBECONFIG") {
        Ok(file_path) => Some(file_path),
        Err(_) => {
            // Look for kubeconfig file in default location.
            let default_path = format!("{}/.kube/config", env::var("HOME")?);
            match Path::new(&default_path).exists() {
                true => Some(default_path),
                false => None,
            }
        }
    };

    match file {
        Some(file) => {
            let cfg_str = std::fs::read_to_string(file)?;
            let cfg_yaml = &YamlLoader::load_from_str(&cfg_str)?[0];
            let master_ip = cfg_yaml["clusters"][0]["cluster"]["server"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Failed to convert IP of master node to string"))?;
            let mut url = Url::parse(master_ip)?;
            url.set_port(None)
                .map_err(|_| anyhow::anyhow!("Failed to unset port"))?;
            tracing::debug!(url=%url, "Found URL from the kubeconfig file,");
            Ok(url)
        }
        None => Err(anyhow::anyhow!(
            "Failed to get URL of master node from kubeconfig file."
        )),
    }
}
