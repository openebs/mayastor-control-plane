use super::*;

use once_cell::sync::OnceCell;

static NATS_MSG_BUS: OnceCell<NatsMessageBus> = OnceCell::new();

/// Initialise the Nats Message Bus
pub async fn message_bus_init(_server: String) {
    let nc = NatsMessageBus::new(None, TimeoutOptions::new()).await;
    NATS_MSG_BUS
        .set(nc)
        .ok()
        .expect("Expect to be initialised only once");
}

/// Initialise the Nats Message Bus with Options
/// IGNORES all but the first initialisation of NATS_MSG_BUS
pub async fn message_bus_init_options(
    client: impl Into<Option<BusClient>>,
    _server: String,
    timeouts: TimeoutOptions,
) {
    if NATS_MSG_BUS.get().is_none() {
        let nc = NatsMessageBus::new(client.into(), timeouts).await;
        NATS_MSG_BUS.set(nc).ok();
    }
}

// Would we want to have both sync and async clients?
/// Nats implementation of the Bus
#[derive(Clone)]
pub struct NatsMessageBus {
    timeout_options: TimeoutOptions,
    client_name: BusClient,
}
impl NatsMessageBus {
    /// Connects to the server and returns a new `NatsMessageBus`
    pub async fn new(client_name: Option<BusClient>, timeout_options: TimeoutOptions) -> Self {
        Self {
            timeout_options,
            client_name: client_name.unwrap_or(BusClient::Unnamed),
        }
    }
}

#[async_trait]
impl Bus for NatsMessageBus {
    fn client_name(&self) -> &BusClient {
        &self.client_name
    }
    fn timeout_opts(&self) -> &TimeoutOptions {
        &self.timeout_options
    }
}
