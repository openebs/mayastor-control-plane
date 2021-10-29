use super::*;
use nats::asynk::Connection;
use once_cell::sync::OnceCell;
use tracing::{info, warn};

static NATS_MSG_BUS: OnceCell<NatsMessageBus> = OnceCell::new();

/// Initialise the Nats Message Bus
pub async fn message_bus_init(server: String) {
    let nc = NatsMessageBus::new(None, &server, BusOptions::new(), TimeoutOptions::new()).await;
    NATS_MSG_BUS
        .set(nc)
        .ok()
        .expect("Expect to be initialised only once");
}

/// Initialise the Nats Message Bus with Options
/// IGNORES all but the first initialisation of NATS_MSG_BUS
pub async fn message_bus_init_options(
    client: impl Into<Option<BusClient>>,
    server: String,
    timeouts: TimeoutOptions,
) {
    if NATS_MSG_BUS.get().is_none() {
        let nc = NatsMessageBus::new(client.into(), &server, BusOptions::new(), timeouts).await;
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

// Would we want to have both sync and async clients?
/// Nats implementation of the Bus
#[derive(Clone)]
pub struct NatsMessageBus {
    timeout_options: TimeoutOptions,
    connection: Connection,
    client_name: BusClient,
}
impl NatsMessageBus {
    /// Connect to the provided server
    /// Logs the first error and quietly continues retrying forever
    pub async fn connect(timeout_opts: TimeoutOptions, server: &str) -> Connection {
        info!("Connecting to the nats server {}...", server);
        // We retry in a loop until successful. Once connected the nats
        // library will handle reconnections for us.
        let interval = std::time::Duration::from_millis(500);
        let mut log_error = true;
        loop {
            match BusOptions::new()
                .max_reconnects(None)
                .tcp_read_timeout(timeout_opts.tcp_read_timeout())
                .tcp_connect_timeout(timeout_opts.tcp_read_timeout())
                .cache_connect_urls(false)
                .disconnect_callback(|| tracing::warn!("NATS connection has been lost"))
                .reconnect_callback(|| tracing::info!("NATS connection has been reestablished"))
                .connect(server)
                .await
            {
                Ok(connection) => {
                    info!("Successfully connected to the nats server {}", server);
                    return connection;
                }
                Err(error) => {
                    if log_error {
                        warn!("Error connection: {}. Quietly retrying...", error);
                        log_error = false;
                    }
                    tokio::time::sleep(interval).await;
                    continue;
                }
            }
        }
    }

    /// Connects to the server and returns a new `NatsMessageBus`
    pub async fn new(
        client_name: Option<BusClient>,
        server: &str,
        _bus_options: BusOptions,
        timeout_options: TimeoutOptions,
    ) -> Self {
        Self {
            timeout_options: timeout_options.clone(),
            connection: Self::connect(timeout_options, server).await,
            client_name: client_name.unwrap_or(BusClient::Unnamed),
        }
    }
}

#[async_trait]
impl Bus for NatsMessageBus {
    async fn publish(&self, channel: Channel, message: &[u8]) -> BusResult<()> {
        self.connection
            .publish(&channel.to_string(), message)
            .await
            .context(Publish {
                channel: channel.to_string(),
                payload: String::from_utf8(Vec::from(message)),
            })
    }

    async fn send(&self, _channel: Channel, _message: &[u8]) -> BusResult<()> {
        unimplemented!()
    }

    async fn request(
        &self,
        channel: Channel,
        message: &[u8],
        req_options: Option<TimeoutOptions>,
    ) -> BusResult<BusMessage> {
        let channel = &channel.to_string();

        let options = req_options
            .clone()
            .unwrap_or_else(|| self.timeout_options.clone());
        let mut timeout = options.timeout;
        let mut retries = 0;

        loop {
            let request = self.connection.request(channel, message);

            let result = tokio::time::timeout(timeout, request).await;
            if let Ok(r) = result {
                return r.context(Publish {
                    channel: channel.to_string(),
                    payload: String::from_utf8(Vec::from(message)),
                });
            }
            if Some(retries) == options.max_retries {
                let error = Error::RequestTimeout {
                    channel: channel.to_string(),
                    payload: String::from_utf8(Vec::from(message)),
                    options: req_options.unwrap_or_else(|| self.timeout_options.clone()),
                };
                tracing::error!("{}", error);
                return Err(error);
            }

            tracing::debug!(
                "Timeout after {:?} on {} - {} retries left",
                timeout,
                channel,
                if let Some(max) = options.max_retries {
                    (max - retries).to_string()
                } else {
                    "unlimited".to_string()
                }
            );

            retries += 1;
            timeout = std::cmp::min(options.timeout_step * retries, Duration::from_secs(10));
        }
    }

    async fn flush(&self) -> BusResult<()> {
        self.connection.flush().await.context(Flush {})
    }

    async fn subscribe(&self, channel: Channel) -> BusResult<BusSubscription> {
        self.connection
            .subscribe(&channel.to_string())
            .await
            .context(Subscribe {
                channel: channel.to_string(),
            })
    }

    fn client_name(&self) -> &BusClient {
        &self.client_name
    }
    fn timeout_opts(&self) -> &TimeoutOptions {
        &self.timeout_options
    }
}
