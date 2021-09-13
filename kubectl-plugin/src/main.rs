#![feature(once_cell)]

#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

mod operations;
mod resources;
mod rest_wrapper;

use crate::{
    operations::{Get, List, Scale},
    resources::{pool, utils, volume, GetResources, ScaleResources},
    rest_wrapper::RestClient,
};
use anyhow::Result;
use operations::Operations;
use reqwest::Url;
use std::{env, path::Path};
use structopt::StructOpt;
use yaml_rust::YamlLoader;

#[derive(StructOpt, Debug)]
struct CliArgs {
    /// Rest endpoint.
    #[structopt(long, short)]
    rest: Option<Url>,
    /// The operation to be performed.
    #[structopt(subcommand)]
    operations: Operations,
    /// Output Format
    #[structopt(default_value = "", short, long)]
    output: String,
}

#[actix_rt::main]
async fn main() {
    let cli_args = &CliArgs::from_args();

    // Initialise the REST client.
    if let Err(e) = init_rest(cli_args.rest.as_ref()) {
        println!("Failed to initialise the REST client. Error {}", e);
    }

    if !&cli_args.output.is_empty()
        && cli_args.output != utils::YAML_FORMAT
        && cli_args.output != utils::JSON_FORMAT
    {
        // Incase of invalid output format, show error and gracefully end.
        println!("Output not supported for {} format", &cli_args.output);
    } else {
        let tmp = &cli_args.output.to_lowercase();
        let l = tmp.trim();
        let output_format: utils::OutputFormat = l.into();

        // Perform the requested operation.
        match &cli_args.operations {
            Operations::Get(resource) => match resource {
                GetResources::Volumes => volume::Volumes::list(output_format).await,
                GetResources::Volume { id } => volume::Volume::get(id, output_format).await,
                GetResources::Pools => pool::Pools::list(output_format).await,
                GetResources::Pool { id } => pool::Pool::get(id, output_format).await,
            },
            Operations::Scale(resource) => match resource {
                ScaleResources::Volume { id, replica_count } => {
                    volume::Volume::scale(id, *replica_count, output_format).await
                }
            },
        };
    }
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
            match Path::new(&default_path.clone()).exists() {
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
            Ok(Url::parse(master_ip)?)
        }
        None => Err(anyhow::anyhow!(
            "Failed to get URL of master node from kubeconfig file."
        )),
    }
}
