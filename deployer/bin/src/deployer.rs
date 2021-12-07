use deployer_lib::CliArgs;
use structopt::StructOpt;

const RUST_LOG_QUIET_DEFAULTS: &str =
    "h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,mio=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info,bollard=info,common_lib=warn";
fn rust_log_add_quiet_defaults(
    current: Option<tracing_subscriber::EnvFilter>,
) -> tracing_subscriber::EnvFilter {
    let main = match current {
        None => {
            format!("info,{}", RUST_LOG_QUIET_DEFAULTS)
        }
        Some(level) => match level.to_string().as_str() {
            "debug" | "trace" => {
                format!("{},{}", level.to_string(), RUST_LOG_QUIET_DEFAULTS)
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

#[tokio::main]
async fn main() {
    init_tracing();

    let cli_args = CliArgs::from_args();
    tracing::info!("Using options: {:?}", &cli_args);

    composer::initialize(
        std::path::Path::new(std::env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("Can't get project root path")
            .to_str()
            .unwrap(),
    );
    cli_args.execute().await.unwrap();
}
