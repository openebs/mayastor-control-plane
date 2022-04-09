#[macro_use]
extern crate prettytable;

pub mod collect;
pub mod operations;

use collect::{
    common::DumpConfig,
    error::Error,
    resource_dump::ResourceDumper,
    resources::{
        node::NodeClientWrapper, pool::PoolClientWrapper, traits::Topologer,
        volume::VolumeClientWrapper, Resourcer,
    },
    rest_wrapper::rest_wrapper_client,
};
use operations::{Operations, Resource};

use std::path::PathBuf;

/// Collects state & log information of mayastor services running in the system and dump them.
#[derive(Debug, Clone, clap::Args)]
pub struct SupportArgs {
    /// Specifies the timeout value to interact with other modules of system
    #[clap(global = true, long, short, default_value = "10s")]
    timeout: humantime::Duration,

    /// Period states to collect all logs from last specified duration
    #[clap(global = true, long, short, default_value = "24h")]
    since: humantime::Duration,

    /// Endpoint of LOKI service, if left empty then it will try to parse endpoint
    /// from Loki service(K8s service resource), if the tool is unable to parse
    /// from service then logs will be collected using Kube-apiserver
    #[clap(global = true, short, long)]
    loki_endpoint: Option<String>,

    /// Endpoint of ETCD service, if left empty then will be parsed from the internal service name
    #[clap(global = true, short, long)]
    etcd_endpoint: Option<String>,

    /// Output directory path to store archive file
    #[clap(global = true, long, short = 'd', default_value = "./")]
    output_directory_path: String,

    /// Kubernetes namespace of mayastor service, defaults to mayastor
    #[clap(global = true, long, short = 'n', default_value = "mayastor")]
    namespace: String,
}

/// Supportability - collects state & log information of services and dumps it to a tar file.
#[derive(Debug, Clone, clap::Args)]
#[clap(
    after_help = "Supportability - collects state & log information of services and dumps it to a tar file."
)]
pub struct DumpArgs {
    #[clap(flatten)]
    args: SupportArgs,
    #[clap(subcommand)]
    resource: Resource,
}

impl DumpArgs {
    /// Execute the dump of the specified resources.
    pub async fn dump(self, kube_config: Option<PathBuf>) -> anyhow::Result<()> {
        self.args
            .execute(kube_config, Operations::Dump(self.resource))
            .await
    }
}

impl SupportArgs {
    /// Execute the specified operation.
    pub(crate) async fn execute(
        self,
        kube_config_path: Option<PathBuf>,
        operation: Operations,
    ) -> anyhow::Result<()> {
        // Initialise the REST client.
        let rest_client = rest_wrapper_client::RestClient::new(std::time::Duration::from_secs(
            self.timeout.as_secs(),
        ))
        .map_err(|e| anyhow::anyhow!("Failed to initialise REST client {e:?}"))?;

        // TODO: Move code inside options to some generic function
        // Perform the operations based on user chosen subcommands
        match operation {
            Operations::Dump(resource) => self
                .execute_resource_dump(rest_client, kube_config_path, resource)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e)),
        }
    }

    async fn execute_resource_dump(
        self,
        rest_client: &'static rest_wrapper_client::RestClient,
        kube_config_path: Option<PathBuf>,
        resource: Resource,
    ) -> Result<(), Error> {
        let cli_args = self;

        let topologer: Box<dyn Topologer>;
        let mut config = DumpConfig {
            rest_client,
            output_directory: cli_args.output_directory_path,
            namespace: cli_args.namespace,
            loki_uri: cli_args.loki_endpoint,
            etcd_uri: cli_args.etcd_endpoint,
            since: cli_args.since,
            kube_config_path,
            timeout: cli_args.timeout,
            topologer: None,
        };
        match resource {
            Resource::System => {
                let mut system_dumper =
                    collect::system_dump::SystemDumper::get_or_panic_system_dumper(config).await;
                if let Err(e) = system_dumper.dump_system().await {
                    println!("Failed to dump system state, error: {:?}", e);
                    return Err(e);
                }
            }
            Resource::Volumes => {
                let volume_client = VolumeClientWrapper::new(rest_client);
                topologer = volume_client.get_topologer(None).await?;
                config.topologer = Some(topologer);
                let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
                if let Err(e) = dumper.dump_info().await {
                    println!("Failed to dump volumes information, Error: {:?}", e);
                    return Err(e);
                }
            }
            Resource::Volume { id } => {
                let volume_client = VolumeClientWrapper::new(rest_client);
                topologer = volume_client.get_topologer(Some(id)).await?;
                config.topologer = Some(topologer);
                let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
                if let Err(e) = dumper.dump_info().await {
                    println!("Failed to dump volume {} information, Error: {:?}", id, e);
                    return Err(e);
                }
            }
            Resource::Pools => {
                let pool_client = PoolClientWrapper::new(rest_client);
                topologer = pool_client.get_topologer(None).await?;
                config.topologer = Some(topologer);
                let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
                if let Err(e) = dumper.dump_info().await {
                    println!("Failed to dump pools information, Error: {:?}", e);
                    return Err(e);
                }
            }
            Resource::Pool { id } => {
                let pool_client = PoolClientWrapper::new(rest_client);
                topologer = pool_client.get_topologer(Some(id.to_string())).await?;
                config.topologer = Some(topologer);
                let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
                if let Err(e) = dumper.dump_info().await {
                    println!("Failed to dump pool {} information, Error: {:?}", id, e);
                    return Err(e);
                }
            }
            Resource::Nodes => {
                let node_client = NodeClientWrapper { rest_client };
                topologer = node_client.get_topologer(None).await?;
                config.topologer = Some(topologer);
                let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
                if let Err(e) = dumper.dump_info().await {
                    println!("Failed to dump nodes information, Error: {:?}", e);
                    return Err(e);
                }
            }
            Resource::Node { id } => {
                let node_client = NodeClientWrapper { rest_client };
                topologer = node_client.get_topologer(Some(id.to_string())).await?;
                config.topologer = Some(topologer);
                let mut dumper = ResourceDumper::get_or_panic_resource_dumper(config).await;
                if let Err(e) = dumper.dump_info().await {
                    println!("Failed to dump node {} information, Error: {:?}", id, e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}
