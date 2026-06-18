mod consumer;

pub use consumer::{NatsConsumer, UnifiedMessage};

/// Configuration for the NATS event consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// NATS server URL.
    pub nats_url: url::Url,
    /// Subscribe using a JetStream pull consumer instead of Core NATS.
    pub jetstream_enabled: bool,
    /// NATS subject filter, e.g. `"events.>"`.
    pub subject_filter: String,
    /// Timeout for the initial TCP connection to NATS.
    pub connection_timeout: std::time::Duration,
    /// Timeout for NATS request/response operations and JetStream metadata discovery.
    pub request_timeout: std::time::Duration,
    /// Durable name for the JetStream pull consumer.
    pub jetstream_consumer_name: String,
    /// JetStream stream name to subscribe from.
    pub jetstream_stream_name: String,
    /// Maximum number of delivery attempts per message before JetStream gives up.
    /// Set to -1 for unlimited redeliveries.
    pub jetstream_max_deliver: i64,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            nats_url: "nats://mayastor-nats:4222".parse().expect("valid NATS URL"),
            jetstream_enabled: false,
            subject_filter: "events.>".into(),
            connection_timeout: std::time::Duration::from_secs(5),
            request_timeout: std::time::Duration::from_secs(10),
            jetstream_consumer_name: "event-consumer".into(),
            jetstream_stream_name: "events-stream".into(),
            jetstream_max_deliver: 3,
        }
    }
}
