pub mod core;
pub mod nexus;
pub mod node;
pub mod pool;
pub mod volume;
pub mod watcher;

use crate::core::registry;
use common::Service;
use common_lib::types::v0::message_bus::ChannelVs;

use common_lib::mbus_api::BusClient;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use structopt::StructOpt;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

#[derive(Debug, StructOpt)]
pub(crate) struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    pub(crate) nats: String,

    /// The period at which the registry updates its cache of all
    /// resources from all nodes
    #[structopt(long, short, default_value = "30s")]
    pub(crate) cache_period: humantime::Duration,

    /// The period at which the reconcile loop checks for new work
    #[structopt(long, default_value = "30s")]
    pub(crate) reconcile_idle_period: humantime::Duration,

    /// The period at which the reconcile loop attempts to do work
    #[structopt(long, default_value = "10s")]
    pub(crate) reconcile_period: humantime::Duration,

    /// Deadline for the mayastor instance keep alive registration
    /// Default: 10s
    #[structopt(long, short, default_value = "10s")]
    pub(crate) deadline: humantime::Duration,

    /// The Persistent Store URLs to connect to
    /// (supports the http/https schema)
    /// Default: http://localhost:2379
    #[structopt(long, short, default_value = "http://localhost:2379")]
    pub(crate) store: String,

    /// The timeout for store operations
    #[structopt(long, default_value = "5s")]
    pub(crate) store_timeout: humantime::Duration,

    /// The timeout for every node connection (gRPC)
    #[structopt(long, default_value = common_lib::DEFAULT_CONN_TIMEOUT)]
    pub(crate) connect_timeout: humantime::Duration,

    /// The default timeout for node request timeouts (gRPC)
    #[structopt(long, short, default_value = common_lib::DEFAULT_REQ_TIMEOUT)]
    pub(crate) request_timeout: humantime::Duration,

    /// Don't use minimum timeouts for specific requests
    #[structopt(long)]
    no_min_timeouts: bool,

    /// Trace rest requests to the Jaeger endpoint agent
    #[structopt(long, short)]
    jaeger: Option<String>,
}

const RUST_LOG_QUIET_DEFAULTS: &str =
    "h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info";

fn rust_log_add_quiet_defaults(
    current: tracing_subscriber::EnvFilter,
) -> tracing_subscriber::EnvFilter {
    let main = match current.to_string().as_str() {
        "debug" => "debug",
        "trace" => "trace",
        _ => return current,
    };
    let logs = format!("{},{}", main, RUST_LOG_QUIET_DEFAULTS);
    tracing_subscriber::EnvFilter::try_new(logs).unwrap()
}

fn init_tracing() {
    let filter = rust_log_add_quiet_defaults(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    );

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty());

    match CliArgs::from_args().jaeger {
        Some(jaeger) => {
            global::set_text_map_propagator(TraceContextPropagator::new());
            let tracer = opentelemetry_jaeger::new_pipeline()
                .with_agent_endpoint(jaeger)
                .with_service_name("core-agent")
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

    let cli_args = CliArgs::from_args();
    info!("Starting Core Agent with options: {:?}", cli_args);

    server(cli_args).await;
}

async fn server(cli_args: CliArgs) {
    let registry = registry::Registry::new(
        CliArgs::from_args().cache_period.into(),
        CliArgs::from_args().store,
        CliArgs::from_args().store_timeout.into(),
        CliArgs::from_args().reconcile_period.into(),
        CliArgs::from_args().reconcile_idle_period.into(),
    )
    .await;

    let service = Service::builder(cli_args.nats, ChannelVs::Core)
        .with_shared_state(global::tracer_with_version(
            "core-agent",
            env!("CARGO_PKG_VERSION"),
        ))
        .with_default_liveness()
        .connect_message_bus(CliArgs::from_args().no_min_timeouts, BusClient::CoreAgent)
        .await
        .with_shared_state(registry.clone())
        .configure_async(node::configure)
        .await
        .configure(pool::configure)
        .configure(nexus::configure)
        .configure(volume::configure)
        .configure(watcher::configure);

    registry.start().await;
    service.run().await;
}

/// Constructs a service handler for `RequestType` which gets redirected to a
/// Service Handler named `ServiceFnName`
#[macro_export]
macro_rules! impl_request_handler {
    ($RequestType:ident, $ServiceFnName:ident) => {
        /// Needed so we can implement the ServiceSubscriber trait for
        /// the message types external to the crate
        #[derive(Clone, Default)]
        struct ServiceHandler<T> {
            data: PhantomData<T>,
        }
        #[async_trait]
        impl common::ServiceSubscriber for ServiceHandler<$RequestType> {
            async fn handler(&self, args: common::Arguments<'_>) -> Result<(), SvcError> {
                #[tracing::instrument(skip(args), fields(result, error, request.service = true))]
                async fn $ServiceFnName(
                    args: common::Arguments<'_>,
                ) -> Result<<$RequestType as Message>::Reply, SvcError> {
                    let request: ReceivedMessage<$RequestType> = args.request.try_into()?;
                    let service: &service::Service = args.context.get_state()?;
                    match service.$ServiceFnName(&request.inner()).await {
                        Ok(reply) => {
                            if let Ok(result_str) = serde_json::to_string(&reply) {
                                if result_str.len() < 2048 {
                                    tracing::Span::current().record("result", &result_str.as_str());
                                }
                            }
                            tracing::Span::current().record("error", &false);
                            Ok(reply)
                        }
                        Err(error) => {
                            tracing::Span::current()
                                .record("result", &format!("{:?}", error).as_str());
                            tracing::Span::current().record("error", &true);
                            Err(error)
                        }
                    }
                }
                use opentelemetry::trace::FutureExt;
                match $ServiceFnName(args.clone())
                    .with_context(args.request.context())
                    .await
                {
                    Ok(reply) => Ok(args.request.respond(reply).await?),
                    Err(error) => Err(error),
                }
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$RequestType::default().id()]
            }
        }
    };
}

/// Constructs a service handler for `PublishType` which gets redirected to a
/// Service Handler named `ServiceFnName`
#[macro_export]
macro_rules! impl_publish_handler {
    ($PublishType:ident, $ServiceFnName:ident) => {
        /// Needed so we can implement the ServiceSubscriber trait for
        /// the message types external to the crate
        #[derive(Clone, Default)]
        struct ServiceHandler<T> {
            data: PhantomData<T>,
        }
        #[async_trait]
        impl common::ServiceSubscriber for ServiceHandler<$PublishType> {
            async fn handler(&self, args: common::Arguments<'_>) -> Result<(), SvcError> {
                let request: ReceivedMessage<$PublishType> = args.request.try_into()?;

                let service: &service::Service = args.context.get_state()?;
                service.$ServiceFnName(&request.inner()).await;
                Ok(())
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$PublishType::default().id()]
            }
        }
    };
}

/// Constructs and calls out to a service handler for `RequestType` which gets
/// redirected to a Service Handler where its name is either:
/// `RequestType` as a snake lowercase (default) or
/// `ServiceFn` parameter (if provided)
#[macro_export]
macro_rules! handler {
    ($RequestType:ident) => {{
        paste::paste! {
            impl_request_handler!(
                $RequestType,
                [<$RequestType:snake:lower>]
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
    ($RequestType:ident, $ServiceFn:ident) => {{
        paste::paste! {
            impl_request_handler!(
                $RequestType,
                $ServiceFn
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
}

/// Constructs and calls out to a service handler for `RequestType` which gets
/// redirected to a Service Handler where its name is either:
/// `RequestType` as a snake lowercase (default) or
/// `ServiceFn` parameter (if provided)
#[macro_export]
macro_rules! handler_publish {
    ($RequestType:ident) => {{
        paste::paste! {
            impl_publish_handler!(
                $RequestType,
                [<$RequestType:snake:lower>]
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
    ($RequestType:ident, $ServiceFn:ident) => {{
        paste::paste! {
            impl_publish_handler!(
                $RequestType,
                $ServiceFn
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
}
