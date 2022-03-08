#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

mod operations;
mod resources;
mod rest_wrapper;

use crate::{
    operations::{Get, List, ReplicaTopology, Scale},
    resources::{node, pool, volume, GetResources, ScaleResources},
    rest_wrapper::RestClient,
};
use anyhow::Result;
use openapi::tower::client::Url;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use operations::Operations;
use std::{env, path::Path};
use structopt::StructOpt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use yaml_rust::YamlLoader;

#[derive(StructOpt, Debug)]
#[structopt(version = utils::package_info!())]
struct CliArgs {
    /// The rest endpoint, parsed from KUBECONFIG, if left empty .
    #[structopt(global = true, long, short)]
    rest: Option<Url>,

    /// The operation to be performed.
    #[structopt(subcommand)]
    operations: Operations,

    /// The Output, viz yaml, json.
    #[structopt(global = true, default_value = "none", short, long, possible_values=&["yaml", "json", "none"], parse(from_str))]
    output: crate::resources::utils::OutputFormat,

    /// Trace rest requests to the Jaeger endpoint agent
    #[structopt(global = true, long, short)]
    jaeger: Option<String>,

    /// Timeout for the REST operations
    #[structopt(long, short, default_value = "10s")]
    timeout: humantime::Duration,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::from_args()
    }
}

fn default_log_filter(current: tracing_subscriber::EnvFilter) -> tracing_subscriber::EnvFilter {
    let log_level = match current.to_string().as_str() {
        "debug" => "debug",
        "trace" => "trace",
        _ => return current,
    };
    let logs = format!("kubectl_mayastor={},warn", log_level);
    tracing_subscriber::EnvFilter::try_new(logs).unwrap()
}

fn init_tracing() {
    let filter = default_log_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
    );

    let subscriber = Registry::default().with(filter).with(
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .pretty(),
    );

    match CliArgs::args().jaeger {
        Some(jaeger) => {
            global::set_text_map_propagator(TraceContextPropagator::new());
            let git_version = option_env!("GIT_VERSION").unwrap_or_else(utils::git_version);
            let tags = utils::tracing_telemetry::default_tracing_tags(
                git_version,
                env!("CARGO_PKG_VERSION"),
            );
            let tracer = opentelemetry_jaeger::new_pipeline()
                .with_agent_endpoint(jaeger)
                .with_service_name("kubectl-plugin")
                .with_tags(tags)
                .install_batch(opentelemetry::runtime::TokioCurrentThread)
                .expect("Should be able to initialise the exporter");
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
            subscriber.with(telemetry).init();
        }
        None => subscriber.init(),
    };
}

#[tokio::main]
async fn main() {
    init_tracing();

    execute(CliArgs::args()).await;

    global::shutdown_tracer_provider();
}

async fn execute(cli_args: CliArgs) {
    // Initialise the REST client.
    if let Err(e) = init_rest(cli_args.rest.clone(), *cli_args.timeout) {
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
fn init_rest(url: Option<Url>, timeout: std::time::Duration) -> Result<()> {
    // Use the supplied URL if there is one otherwise obtain one from the kubeconfig file.
    let url = match url {
        Some(url) => url,
        None => url_from_kubeconfig()?,
    };
    RestClient::init(url, timeout)
}

/// Get the URL of the master node from the kubeconfig file.
fn url_from_kubeconfig() -> Result<Url> {
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
            let cfg_str = std::fs::read_to_string(file)?;
            let cfg_yaml = &YamlLoader::load_from_str(&cfg_str)?[0];
            let master_ip = cfg_yaml["clusters"][0]["cluster"]["server"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Failed to convert IP of master node to string"))?;
            let mut url = Url::parse(master_ip)?;
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
