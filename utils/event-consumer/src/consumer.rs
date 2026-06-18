use crate::ConsumerConfig;
use anyhow::bail;
use async_nats::jetstream::consumer::{pull, Consumer};
use events_api::common::retry::{backoff_with_options, BackoffOptions};
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// A message received from NATS, abstracting over JetStream and Core NATS delivery.
pub enum UnifiedMessage {
    /// A JetStream message that must be ACKed or NAKed by the caller.
    JetStream(async_nats::jetstream::Message),
    /// A Core NATS message — fire-and-forget, no acknowledgement needed.
    Core(async_nats::Message),
}

/// Manages the NATS connection and forwards incoming messages to a channel.
pub struct NatsConsumer {
    client: async_nats::Client,
    config: ConsumerConfig,
}

fn consumer_backoff_options() -> BackoffOptions {
    BackoffOptions::new()
        .with_init_delay(Duration::from_millis(500))
        .with_cutoff(2)
        .with_delay_step(Duration::from_secs(2))
        .with_max_delay(Duration::from_secs(30))
}

impl NatsConsumer {
    /// Connect to the NATS server using the provided config.
    /// Returns an error if the connection cannot be established.
    pub async fn connect(config: ConsumerConfig) -> Result<Self, async_nats::Error> {
        info!("Connecting to NATS at {}...", config.nats_url);

        let connection_timeout = config.connection_timeout;
        let request_timeout = config.request_timeout;
        let backoff = consumer_backoff_options();

        let options = async_nats::ConnectOptions::new()
            .connection_timeout(connection_timeout)
            .request_timeout(Some(request_timeout))
            .retry_on_initial_connect()
            .event_callback(|event| async move {
                match event {
                    async_nats::Event::Connected => info!("NATS connection established"),
                    async_nats::Event::Disconnected => warn!("NATS connection lost"),
                    async_nats::Event::LameDuckMode => warn!("NATS server entering lame duck mode"),
                    async_nats::Event::SlowConsumer(_) => warn!("NATS slow consumer detected"),
                    _ => (),
                }
            })
            .reconnect_delay_callback(move |attempts| backoff.delay(attempts as u32));
        let client = options.connect(config.nats_url.as_str()).await?;
        Ok(Self { client, config })
    }

    /// Start consuming messages from NATS and forward them to `tx`.
    /// Dispatches to JetStream pull consumer or Core NATS depending on `config.jetstream_enabled`.
    pub async fn subscribe(self, tx: mpsc::Sender<UnifiedMessage>) -> anyhow::Result<()> {
        if self.config.jetstream_enabled {
            info!("JetStream enabled — starting pull consumer");
            self.setup_jetstream(tx).await
        } else {
            info!("JetStream disabled — using Core NATS subscription");
            self.setup_core_nats(tx);
            Ok(())
        }
    }

    // Wires together consumer acquisition and the message pump.
    async fn setup_jetstream(self, tx: mpsc::Sender<UnifiedMessage>) -> anyhow::Result<()> {
        let consumer = self.create_pull_consumer().await?;
        tokio::spawn(run_jetstream_loop(consumer, tx));
        Ok(())
    }

    // Connects to the JetStream stream and creates (or attaches to) the durable pull consumer.
    // Retries with backoff; fails after max_retries.
    async fn create_pull_consumer(&self) -> anyhow::Result<Consumer<pull::Config>> {
        let js = async_nats::jetstream::new(self.client.clone());
        let stream_name = &self.config.jetstream_stream_name;
        let consumer_name = &self.config.jetstream_consumer_name;
        let subject = &self.config.subject_filter;
        let request_timeout = self.config.request_timeout;
        let backoff = consumer_backoff_options();
        let mut tries = 0u32;

        loop {
            if self.client.connection_state() != async_nats::connection::State::Connected {
                warn!(
                    tries,
                    max = backoff.max_retries,
                    "NATS client not connected; retrying"
                );
                if tries >= backoff.max_retries {
                    bail!(
                        "JetStream setup failed after {} attempts: client disconnected",
                        tries + 1
                    );
                }
                backoff_with_options(&mut tries, &backoff).await;
                continue;
            }

            match tokio::time::timeout(request_timeout, js.get_stream(stream_name)).await {
                Ok(Ok(stream)) => {
                    match stream
                        .get_or_create_consumer(
                            consumer_name,
                            pull::Config {
                                durable_name: Some(consumer_name.to_string()),
                                filter_subject: subject.to_string(),
                                max_deliver: self.config.jetstream_max_deliver,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(consumer) => return Ok(consumer),
                        Err(e) => {
                            error!(tries, max = backoff.max_retries, error = %e, "JetStream consumer creation failed");
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!(tries, max = backoff.max_retries, error = %e, "JetStream API error");
                }
                Err(_) => {
                    error!(
                        tries,
                        max = backoff.max_retries,
                        "JetStream metadata discovery timed out"
                    );
                }
            }

            if tries >= backoff.max_retries {
                bail!("Unable to set up JetStream after {} attempts", tries + 1);
            }
            warn!(
                tries,
                max = backoff.max_retries,
                "JetStream setup failed; retrying"
            );
            backoff_with_options(&mut tries, &backoff).await;
        }
    }

    // Core NATS Subscription Setup with Resilient Reconnection Logic.
    fn setup_core_nats(self, tx: mpsc::Sender<UnifiedMessage>) {
        let client = self.client;
        let subject = self.config.subject_filter;

        tokio::spawn(async move {
            let backoff = consumer_backoff_options();
            let mut tries = 0u32;

            loop {
                match client.subscribe(subject.clone()).await {
                    Ok(mut subscription) => {
                        tries = 0;
                        while let Some(message) = subscription.next().await {
                            if tx.send(UnifiedMessage::Core(message)).await.is_err() {
                                info!("Consumer channel closed; shutting down Core NATS loop");
                                return;
                            }
                        }

                        if tries >= backoff.max_retries {
                            error!(
                                max = backoff.max_retries,
                                "Core NATS subscription failed after too many recovery attempts; giving up"
                            );
                            return;
                        }
                        warn!(
                            tries,
                            "Core NATS subscription ended unexpectedly; re-subscribing"
                        );
                        backoff_with_options(&mut tries, &backoff).await;
                    }
                    Err(e) => {
                        error!(tries, error = %e, "Core NATS subscribe failed; retrying");
                        if tries >= backoff.max_retries {
                            error!(
                                max = backoff.max_retries,
                                "Core NATS subscription failed; giving up"
                            );
                            return;
                        }
                        backoff_with_options(&mut tries, &backoff).await;
                    }
                }
            }
        });
    }
}

// Pumps messages from a JetStream pull consumer into `tx`.
// Rebinds the message iterator on transient errors; gives up after max_retries consecutive failures.
async fn run_jetstream_loop(consumer: Consumer<pull::Config>, tx: mpsc::Sender<UnifiedMessage>) {
    let backoff = consumer_backoff_options();
    let mut recovery_tries = 0u32;

    loop {
        let mut messages = match consumer.messages().await {
            Ok(msgs) => {
                recovery_tries = 0;
                msgs
            }
            Err(e) => {
                error!(recovery_tries, error = %e, "Failed to get JetStream message iterator; retrying");
                if recovery_tries >= backoff.max_retries {
                    error!(
                        max = backoff.max_retries,
                        "JetStream consumer failed to bind; giving up"
                    );
                    return;
                }
                backoff_with_options(&mut recovery_tries, &backoff).await;
                continue;
            }
        };

        while let Some(msg_result) = messages.next().await {
            match msg_result {
                Ok(message) => {
                    if tx.send(UnifiedMessage::JetStream(message)).await.is_err() {
                        info!("Consumer channel closed; shutting down JetStream loop");
                        return;
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error reading JetStream message; re-binding consumer");
                    break;
                }
            }
        }

        if recovery_tries >= backoff.max_retries {
            error!(
                max = backoff.max_retries,
                "JetStream stream failed after too many recovery attempts; giving up"
            );
            return;
        }
        warn!(
            recovery_tries,
            "JetStream stream ended unexpectedly; re-binding"
        );
        backoff_with_options(&mut recovery_tries, &backoff).await;
    }
}
