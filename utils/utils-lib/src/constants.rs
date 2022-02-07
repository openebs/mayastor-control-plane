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

/// Mayastor container image used for testing
pub const MAYASTOR_IMAGE: &str = "mayadata/mayastor:e2e-nightly";

/// Mayastor environment variable that points to a mayastor binary
/// This must be in sync with shell.nix
pub const MAYASTOR_BINARY: &str = "MAYASTOR_BIN";

/// The period at which a component updates its resource cache
pub const CACHE_POLL_PERIOD: &str = "30s";

/// The key to mark the creation source of a pool in labels
pub const OPENEBS_CREATED_BY_KEY: &str = "openebs.io/created-by";

/// The value to mark the creation source of a pool to be msp-operator in labels
pub const MSP_OPERATOR: &str = "msp-operator";

/// The default value to be assigned as GRPC server addr if not overridden
pub const DEFAULT_GRPC_SERVER_ADDR: &str = "https://0.0.0.0:50051";

/// The default value to be assigned as GRPC client addr if not overridden
pub const DEFAULT_GRPC_CLIENT_ADDR: &str = "https://core:50051";
