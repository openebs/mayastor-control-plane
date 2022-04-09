use anyhow::Result;
use clap::Parser;
use openapi::tower::client::Url;
use opentelemetry::global;
use plugin::{
    operations::{Get, List, ReplicaTopology, Scale},
    resources::{node, pool, volume, GetResources, ScaleResources},
    rest_wrapper::RestClient,
};
use std::{
    env,
    path::{Path, PathBuf},
};

mod resources;
use resources::Operations;

#[derive(Parser, Debug)]
#[clap(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    /// The rest endpoint to connect to.
    #[clap(global = true, long, short)]
    rest: Option<Url>,

    /// Path to kubeconfig file.
    #[clap(parse(from_os_str), global = true, long, short = 'k')]
    kube_config_path: Option<PathBuf>,

    /// The operation to be performed.
    #[clap(subcommand)]
    operations: Operations,

    /// The Output, viz yaml, json.
    #[clap(global = true, default_value = plugin::resources::utils::OutputFormat::None.as_ref(), short, long)]
    output: plugin::resources::utils::OutputFormat,

    /// Trace rest requests to the Jaeger endpoint agent.
    #[clap(global = true, long, short)]
    jaeger: Option<String>,

    /// Timeout for the REST operations.
    #[clap(long, short, default_value = "10s")]
    timeout: humantime::Duration,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

#[tokio::main]
async fn main() {
    plugin::init_tracing(&CliArgs::args().jaeger);

    execute(CliArgs::args()).await;

    global::shutdown_tracer_provider();
}

async fn execute(cli_args: CliArgs) {
    // Initialise the REST client.
    if let Err(e) = init_rest(cli_args.rest.clone(), *cli_args.timeout).await {
        println!("Failed to initialise the REST client. Error {}", e);
        std::process::exit(1);
    }

    // Perform the operations based on the subcommand, with proper output format.
    match cli_args.operations {
        Operations::Get(resource) => match resource {
            GetResources::Volumes => volume::Volumes::list(&cli_args.output).await,
            GetResources::Volume { id } => volume::Volume::get(&id, &cli_args.output).await,
            GetResources::VolumeReplicaTopology { id } => {
                volume::Volume::topology(&id, &cli_args.output).await
            }
            GetResources::Pools => pool::Pools::list(&cli_args.output).await,
            GetResources::Pool { id } => pool::Pool::get(&id, &cli_args.output).await,
            GetResources::Nodes => node::Nodes::list(&cli_args.output).await,
            GetResources::Node { id } => node::Node::get(&id, &cli_args.output).await,
        },
        Operations::Scale(resource) => match resource {
            ScaleResources::Volume { id, replica_count } => {
                volume::Volume::scale(&id, replica_count, &cli_args.output).await
            }
        },
        Operations::Dump(resources) => {
            resources.dump(cli_args.kube_config_path).await.unwrap();
        }
    };
}

/// Initialise the REST client.
async fn init_rest(url: Option<Url>, timeout: std::time::Duration) -> Result<()> {
    // Use the supplied URL if there is one otherwise obtain one from the kubeconfig file.
    let url = match url {
        Some(url) => url,
        None => url_from_kubeconfig().await?,
    };
    RestClient::init(url, timeout)
}

/// Get the URL of the master node from the kubeconfig file.
async fn url_from_kubeconfig() -> Result<Url> {
    // Search for the kubeconfig.
    // First look at the environment variable then look in the default directory.
    let file = match env::var("KUBECONFIG") {
        Ok(file_path) => Some(file_path),
        Err(_) => {
            // Look for kubeconfig file in default location.
            #[cfg(target_os = "linux")]
            let default_path = format!("{}/.kube/config", env::var("HOME")?);
            #[cfg(target_os = "windows")]
            let default_path = format!("{}/.kube/config", env::var("USERPROFILE")?);
            match Path::new(&default_path).exists() {
                true => Some(default_path),
                false => None,
            }
        }
    };

    match file {
        Some(file) => {
            // NOTE: Kubeconfig file may hold multiple contexts to communicate
            //       with different kubernetes clusters. We have to pick master
            //       address of current-context config
            let kube_config = kube::config::Kubeconfig::read_from(&file)?;
            let config =
                kube::Config::from_custom_kubeconfig(kube_config, &Default::default()).await?;
            let server_url = config
                .cluster_url
                .host()
                .ok_or_else(|| anyhow::anyhow!("Failed to get master address"))?;
            let mut url = Url::parse(("http://".to_owned() + server_url).as_str())?;
            url.set_port(None)
                .map_err(|_| anyhow::anyhow!("Failed to unset port"))?;
            url.set_scheme("http")
                .map_err(|_| anyhow::anyhow!("Failed to set REST client scheme"))?;
            tracing::debug!(url=%url, "Found URL from the kubeconfig file,");
            Ok(url)
        }
        None => Err(anyhow::anyhow!(
            "Failed to get URL of master node from kubeconfig file."
        )),
    }
}
