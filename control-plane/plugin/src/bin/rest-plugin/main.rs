use clap::Parser;
use openapi::tower::client::Url;
use plugin::rest_wrapper::RestClient;
use snafu::ResultExt;
use std::{env, ops::Deref};

#[derive(clap::Parser, Debug)]
#[clap(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    /// The rest endpoint to connect to.
    #[clap(global = true, long, short, default_value = "http://localhost:8081")]
    rest: Url,

    #[clap(flatten)]
    args: plugin::RestCliArgs,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}
impl Deref for CliArgs {
    type Target = plugin::RestCliArgs;

    fn deref(&self) -> &Self::Target {
        &self.args
    }
}

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::args();
    let _trace_flush = cli_args.init_tracing();

    if let Err(error) = cli_args.execute().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[derive(Debug, snafu::Snafu)]
enum Error {
    #[snafu(display("Failed to initialise the REST client. Error {source}"))]
    RestClient { source: anyhow::Error },
    #[snafu(display("{source}"))]
    Resources { source: plugin::resources::Error },
}

impl CliArgs {
    async fn execute(&self) -> Result<(), Error> {
        // todo: client connection is lazy, we should do sanity connection test here.
        //  Example, we can use use rest liveness probe.
        RestClient::init(self.rest.clone(), *self.timeout).context(RestClientSnafu)?;
        self.args.execute().await.context(ResourcesSnafu)
    }
}
