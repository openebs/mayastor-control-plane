use clap::Parser;
use openapi::tower::client::{configuration::ClientSecurity, Url};

use plugin::{operations::Operations, rest_wrapper::RestClient, ExecuteOperation};
use snafu::ResultExt;
use std::path::PathBuf;

#[derive(clap::Parser, Debug)]
#[clap(name = utils::package_description!(), version = utils::version_info_string!())]
#[group(skip)]
struct CliArgs {
    /// The rest endpoint to connect to.
    #[clap(global = true, long, short, default_value = "http://localhost:8081")]
    rest: Url,

    /// The operation to be performed.
    #[clap(subcommand)]
    operation: Operations,

    /// Path to the TLS CA certificate bundle used to validate REST server certificates.
    #[clap(global = true, long)]
    tls_ca_file: Option<PathBuf>,

    /// Path to the client TLS certificate chain file (PEM) for mTLS.
    #[clap(global = true, long, requires = "tls_key_file")]
    tls_cert_file: Option<PathBuf>,

    /// Path to the client TLS private key file (PEM) for mTLS.
    #[clap(global = true, long, requires = "tls_cert_file")]
    tls_key_file: Option<PathBuf>,

    /// Path to a file containing the JWT bearer token for REST authentication.
    #[clap(global = true, long)]
    jwt: Option<PathBuf>,

    #[clap(flatten)]
    args: plugin::CliArgs,
}

#[tokio::main]
async fn main() {
    utils::init_rustls_crypto_provider();
    let cli_args = CliArgs::args();
    let _trace_flush = cli_args.args.init_tracing();
    if let Err(error) = cli_args.execute().await {
        eprintln!("{error:?}");
        std::process::exit(-1);
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
    fn args() -> Self {
        CliArgs::parse()
    }

    async fn execute(&self) -> Result<(), Error> {
        // todo: client connection is lazy, we should do sanity connection test here.
        //  Example, we can use use rest liveness probe.
        let tls = kube_proxy::TlsMode::new(
            self.tls_ca_file.as_ref(),
            self.tls_cert_file.as_ref(),
            self.tls_key_file.as_ref(),
        )
        .map_err(anyhow::Error::from)
        .context(RestClientSnafu)?;
        let client_security = ClientSecurity::try_new(&self.jwt, tls)
            .map_err(anyhow::Error::from)
            .context(RestClientSnafu)?;

        // todo: client connection is lazy, we should do sanity connection test here.
        //  Example, we can use use rest liveness probe.
        RestClient::init(
            self.rest.clone(),
            self.args.jaeger.is_some(),
            *self.args.timeout,
            client_security,
        )
        .context(RestClientSnafu)?;

        self.operation
            .execute(&self.args)
            .await
            .context(ResourcesSnafu)
    }
}
