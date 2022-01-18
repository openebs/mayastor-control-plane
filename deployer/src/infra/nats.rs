use super::*;
use common_lib::mbus_api::{BusOptions, DynBus, NatsMessageBus, TimeoutOptions};
use once_cell::sync::OnceCell;
use std::time::Duration;

#[async_trait]
impl ComponentAction for Nats {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if options.no_nats {
            cfg
        } else {
            cfg.add_container_spec(
                ContainerSpec::from_binary(
                    "nats",
                    Binary::from_path("nats-server").with_arg("-DV"),
                )
                .with_portmap("4222", "4222"),
            )
        })
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        if !options.no_nats {
            cfg.start("nats").await?;
        }
        if !options.no_nats || cfg.container_exists("nats").await {
            message_bus_init_options("localhost:4222", bus_timeout_opts()).await;
        }
        Ok(())
    }
}

static NATS_MSG_BUS: OnceCell<NatsMessageBus> = OnceCell::new();

fn bus_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::new()
        .with_req_timeout(None)
        .with_timeout(Duration::from_millis(500))
        .with_timeout_backoff(Duration::from_millis(500))
        .with_max_retries(10)
}
async fn message_bus_init_options(server: &str, timeouts: TimeoutOptions) {
    if NATS_MSG_BUS.get().is_none() {
        let nc = NatsMessageBus::new(None, server, BusOptions::new(), timeouts).await;
        NATS_MSG_BUS.set(nc).ok();
    }
}

/// Get the static `NatsMessageBus` as a boxed `MessageBus`
pub fn bus() -> DynBus {
    Box::new(
        NATS_MSG_BUS
            .get()
            .expect("Shared message bus should be initialised before use.")
            .clone(),
    )
}
