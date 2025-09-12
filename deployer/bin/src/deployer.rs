use clap::Parser;
use deployer_lib::{ListOptions, StartOptions, StopOptions};

const RUST_LOG_SILENCE_DEFAULTS: &str =
    "h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,mio=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info,bollard=info,stor_port=warn";

fn rust_log_add_quiet_defaults(
    current: Option<tracing_subscriber::EnvFilter>,
) -> tracing_subscriber::EnvFilter {
    let main = match current {
        None => {
            format!("info,{RUST_LOG_SILENCE_DEFAULTS}")
        }
        Some(level) => match level.to_string().as_str() {
            "debug" | "trace" => {
                format!("{level},{RUST_LOG_SILENCE_DEFAULTS}")
            }
            _ => return level,
        },
    };
    tracing_subscriber::EnvFilter::try_new(main).unwrap()
}
fn init_tracing() {
    let filter =
        rust_log_add_quiet_defaults(tracing_subscriber::EnvFilter::try_from_default_env().ok());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[derive(Debug, Parser)]
#[clap(name = utils::package_description!(), version = utils::version_info_str!())]
struct CliArgs {
    #[clap(subcommand)]
    action: Action,
}
#[derive(Debug, Parser)]
#[clap(about = "Deployment actions")]
enum Action {
    Start(Box<StartOptions>),
    Stop(StopOptions),
    List(ListOptions),
}

impl Action {
    async fn execute(&mut self) -> anyhow::Result<()> {
        match self {
            Action::Start(options) => options.start().await.map(|_| ()),
            Action::Stop(options) => options.stop().await,
            Action::List(options) => options.list().await,
        }
        .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let mut cli_args = CliArgs::parse();
    tracing::info!("Using options: {:?}", &cli_args);

    composer::initialize(
        std::path::Path::new(std::env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("Can't get project root path")
            .to_str()
            .unwrap(),
    );
    cli_args.action.execute().await?;

    Ok(())
}
