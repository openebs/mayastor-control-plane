/// Various common constants used by the control plane
///
/// Default request timeout for any NATS or GRPC request
pub const DEFAULT_REQ_TIMEOUT: &str = "5s";

/// Default connection timeout for a GRPC connection
pub const DEFAULT_CONN_TIMEOUT: &str = "1s";

/// Use a set of minimum timeouts for specific requests
pub const ENABLE_MIN_TIMEOUTS: bool = true;

/// The timeout for all persistent store operations
pub const STORE_OP_TIMEOUT: &str = "5s";
/// The lease lock ttl for the persistent store after which we'll lose the exclusive access
pub const STORE_LEASE_LOCK_TTL: &str = "30s";

/// Io-Engine container image used for testing
pub const IO_ENGINE_IMAGE: &str = "mayadata/mayastor-io-engine:develop";

/// IO-Engine node selector label key.
pub const IO_ENGINE_SELECTOR_KEY: &str = "openebs.io/engine";
/// IO-Engine node selector label value.
pub const IO_ENGINE_SELECTOR_VALUE: &str = "io-engine";

/// Environment variable that points to an io-engine binary
/// This must be in sync with shell.nix
pub const DATA_PLANE_BINARY: &str = "IO_ENGINE_BIN";

/// The period at which a component updates its resource cache
pub const CACHE_POLL_PERIOD: &str = "30s";

/// The key to mark the creation source of a pool in labels
pub const CREATED_BY_KEY: &str = "openebs.io/created-by";

/// The value to mark the creation source of a pool to be disk pool operator in labels
pub const DSP_OPERATOR: &str = "operator-diskpool";

/// The default value to be assigned as GRPC server addr if not overridden
pub const DEFAULT_GRPC_SERVER_ADDR: &str = "https://0.0.0.0:50051";

/// The default value to be assigned as GRPC client addr if not overridden
pub const DEFAULT_GRPC_CLIENT_ADDR: &str = "https://core:50051";

/// The default value to be assigned as JSON GRPC server addr if not overridden
pub const DEFAULT_JSON_GRPC_SERVER_ADDR: &str = "https://0.0.0.0:50052";

/// The default value to be assigned as JSON GRPC client addr if not overridden
pub const DEFAULT_JSON_GRPC_CLIENT_ADDR: &str = "https://jsongrpc:50052";

/// The default value for a concurrency limit.
pub const DEFAULT_GRPC_CLIENT_CONCURRENCY: usize = 25;

/// The default quiet filters in addition to `RUST_LOG`.
pub const RUST_LOG_SILENCE_DEFAULTS: &str =
    "actix_web=info,actix_server=info,h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info,mio=info";

/// The default value to be assigned as cluster agent GRPC server addr if not overridden
pub const DEFAULT_CLUSTER_AGENT_SERVER_ADDR: &str = "https://ha-cluster-agent:11500";

/// The default value to be assigned as node-agent GRPC server addr if not overridden
pub const DEFAULT_NODE_AGENT_SERVER_ADDR: &str = "https://0.0.0.0:11600";
