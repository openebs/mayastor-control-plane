/// Various common constants used by the control plane
///
/// Default request timeout for any NATS or GRPC request
pub const DEFAULT_REQ_TIMEOUT: &str = "5s";

/// Default connection timeout for a GRPC connection
pub const DEFAULT_CONN_TIMEOUT: &str = "1s";

/// Use a set of minimum timeouts for specific requests
pub const ENABLE_MIN_TIMEOUTS: bool = true;

/// Mayastor container image used for testing
pub const MAYASTOR_IMAGE: &str = "mayadata/mayastor:655ffa91eb87";

/// Mayastor environment variable that points to a mayastor binary
/// This must be in sync with shell.nix
pub const MAYASTOR_BINARY: &str = "MAYASTOR_BIN";

/// The period at which a component updates its resource cache
pub const CACHE_POLL_PERIOD: &str = "30s";
