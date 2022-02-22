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
    if let Err(e) = init_rest(
        cli_args.rest.clone(),
        *cli_args.timeout,
        cli_args.kube_config_path.clone(),
    )
    .await
    {
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
            let _ignore = resources
                .dump(cli_args.kube_config_path)
                .await
                .map_err(|_e| {
                    println!("Partially collected dump information !!");
                    std::process::exit(1);
                });
            println!("Completed collection of dump !!");
        }
    };
}

/// Initialise the REST client.
async fn init_rest(
    url: Option<Url>,
    timeout: std::time::Duration,
    kube_config_path: Option<PathBuf>,
) -> Result<()> {
    // Use the supplied URL if there is one otherwise obtain one from the kubeconfig file.
    let url = match url {
        Some(url) => url,
        None => url_from_kubeconfig(kube_config_path).await?,
    };
    RestClient::init(url, timeout)
}

/// Get the URL of the master node from the kubeconfig file.
async fn url_from_kubeconfig(kube_config_path: Option<PathBuf>) -> Result<Url> {
    let file = match kube_config_path {
        Some(config_path) => config_path,
        None => {
            let file_path = match env::var("KUBECONFIG") {
                Ok(value) => Some(value),
                Err(_) => {
                    // Look for kubeconfig file in default location.
                    #[cfg(any(target_os = "linux", target_os = "macos"))]
                    let default_path = format!("{}/.kube/config", env::var("HOME")?);
                    #[cfg(target_os = "windows")]
                    let default_path = format!("{}/.kube/config", env::var("USERPROFILE")?);
                    match Path::new(&default_path).exists() {
                        true => Some(default_path),
                        false => None,
                    }
                }
            };
            if file_path.is_none() {
                return Err(anyhow::anyhow!(
                    "kubeconfig file not found in default location"
                ));
            }
            let mut path = PathBuf::new();
            path.push(file_path.unwrap_or_default());
            path
        }
    };

    // NOTE: Kubeconfig file may hold multiple contexts to communicate
    //       with different kubernetes clusters. We have to pick master
    //       address of current-context config only
    let kube_config = kube::config::Kubeconfig::read_from(&file)?;
    let config = kube::Config::from_custom_kubeconfig(kube_config, &Default::default()).await?;
    let server_url = config
        .cluster_url
        .host()
        .ok_or_else(|| anyhow::anyhow!("Failed to get master address".to_string()))?;
    let mut url = Url::parse(("http://".to_owned() + server_url).as_str())
        .map_err(|e| anyhow::anyhow!(format!("Failed to parse URL, error: {}", e)))?;
    url.set_port(None)
        .map_err(|e| anyhow::anyhow!(format!("Failed to unset port {:?}", e)))?;
    url.set_scheme("http").map_err(|e| {
        anyhow::anyhow!(format!("Failed to set REST client scheme, Error: {:?}", e))
    })?;
    Ok(url)
}
