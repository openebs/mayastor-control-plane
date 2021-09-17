pub mod mbus_api;
pub mod store;
pub mod types;

/// Helper to convert from Vec<F> into Vec<T>
pub trait IntoVec<T>: Sized {
    /// Performs the conversion.
    fn into_vec(self) -> Vec<T>;
}

impl<F: Into<T>, T> IntoVec<T> for Vec<F> {
    fn into_vec(self) -> Vec<T> {
        self.into_iter().map(Into::into).collect()
    }
}

/// Helper to convert from Option<F> into Option<T>
pub trait IntoOption<T>: Sized {
    /// Performs the conversion.
    fn into_opt(self) -> Option<T>;
}

impl<F: Into<T>, T> IntoOption<T> for Option<F> {
    fn into_opt(self) -> Option<T> {
        self.map(Into::into)
    }
}

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
