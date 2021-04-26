use super::*;
use mbus_api::{BusOptions, DynBus, NatsMessageBus, TimeoutOptions};
use once_cell::sync::OnceCell;
use std::time::Duration;

#[async_trait]
impl ComponentAction for Nats {
    fn configure(&self, _options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(cfg.add_container_spec(
            ContainerSpec::from_binary("nats", Binary::from_nix("nats-server").with_arg("-DV"))
                .with_portmap("4222", "4222"),
        ))
    }
    async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        cfg.start("nats").await?;
        message_bus_init_options(cfg.container_ip("nats"), bus_timeout_opts()).await;
        Ok(())
    }
}

static NATS_MSG_BUS: OnceCell<NatsMessageBus> = OnceCell::new();

fn bus_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::new()
        .with_timeout(Duration::from_millis(500))
        .with_timeout_backoff(Duration::from_millis(500))
        .with_max_retries(10)
}
async fn message_bus_init_options(server: String, timeouts: TimeoutOptions) {
    if NATS_MSG_BUS.get().is_none() {
        let nc = NatsMessageBus::new(&server, BusOptions::new(), timeouts).await;
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
