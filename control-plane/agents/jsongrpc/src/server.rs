pub mod service;

use async_trait::async_trait;
use common::{errors::SvcError, *};
use common_lib::{
    mbus_api::*,
    types::v0::message_bus::{ChannelVs, JsonGrpcRequest},
};
use grpc::client::CoreClient;
use http::Uri;
use once_cell::sync::OnceCell;
use service::*;
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;
use tracing::info;
use utils::DEFAULT_GRPC_CLIENT_ADDR;

#[derive(Debug, StructOpt)]
#[structopt(version = utils::package_info!())]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    nats: String,

    /// The CORE gRPC Server URL or address to connect to the services.
    #[structopt(long, short = "z", default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,

    /// Don't use minimum timeouts for specific requests
    #[structopt(long)]
    no_min_timeouts: bool,
}

/// Needed so we can implement the ServiceSubscriber trait for
/// the message types external to the crate
#[derive(Clone, Default)]
struct ServiceHandler<T> {
    data: PhantomData<T>,
}

/// Once cell static variable to store the grpc client and initialise once at startup
pub static CORE_CLIENT: OnceCell<CoreClient> = OnceCell::new();

macro_rules! impl_service_handler {
    // RequestType is the message bus request type
    // ServiceFnName is the name of the service function to route the request
    // into
    ($RequestType:ident, $ServiceFnName:ident) => {
        #[async_trait]
        impl ServiceSubscriber for ServiceHandler<$RequestType> {
            async fn handler(&self, args: Arguments<'_>) -> Result<(), SvcError> {
                let request: ReceivedMessage<$RequestType> = args.request.try_into()?;

                let reply = JsonGrpcSvc::$ServiceFnName(&request.inner()).await?;
                Ok(request.reply(reply).await?)
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$RequestType::default().id()]
            }
        }
    };
}

impl_service_handler!(JsonGrpcRequest, json_grpc_call);

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::from_args();
    utils::print_package_info!();
    info!("Using options: {:?}", &cli_args);

    init_tracing();

    let grpc_addr = &cli_args.core_grpc;
    // Initialise the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(grpc_addr.clone(), None).await)
        .ok()
        .expect("Expect to be initialised only once");

    server(cli_args).await;
}

async fn server(cli_args: CliArgs) {
    Service::builder(Some(cli_args.nats), ChannelVs::JsonGrpc)
        .connect_message_bus(
            CliArgs::from_args().no_min_timeouts,
            BusClient::JsonGrpcAgent,
        )
        .await
        .with_subscription(ServiceHandler::<JsonGrpcRequest>::default())
        .with_default_liveness()
        .run()
        .await;
}
