#![allow(clippy::crate_in_macro_def)]

/// Platform specific information, such as the cluster uid which is used as part of the pstor(etcd)
/// key prefix.
pub mod platform;
pub mod store;
pub mod transport_api;
pub mod types;

/// Helper to convert from Vec<F> into Vec<T>.
pub trait IntoVec<T>: Sized {
    /// Performs the conversion.
    fn into_vec(self) -> Vec<T>;
}

impl<F: Into<T>, T> IntoVec<T> for Vec<F> {
    fn into_vec(self) -> Vec<T> {
        self.into_iter().map(Into::into).collect()
    }
}

/// Helper to try to convert from Vec<F> into Vec<T>.
pub trait TryIntoVec<T>: Sized {
    type Error;
    /// Performs the conversion.
    fn try_into_vec(self) -> Result<Vec<T>, Self::Error>;
}

impl<F: TryInto<T>, T> TryIntoVec<T> for Vec<F> {
    type Error = <F as TryInto<T>>::Error;
    fn try_into_vec(self) -> Result<Vec<T>, Self::Error> {
        self.into_iter().map(TryInto::try_into).collect()
    }
}

/// Helper to convert from Option<F> into Option<T>.
pub trait IntoOption<T>: Sized {
    /// Performs the conversion.
    fn into_opt(self) -> Option<T>;
}

impl<F: Into<T>, T> IntoOption<T> for Option<F> {
    fn into_opt(self) -> Option<T> {
        self.map(Into::into)
    }
}

/// Helper to convert from Option<F> into Option<T>.
pub trait TryIntoOption<T>: Sized {
    type Error;
    /// Performs the conversion.
    fn try_into_opt(self) -> Result<Option<T>, Self::Error>;
}

impl<F: TryInto<T>, T> TryIntoOption<T> for Option<F> {
    type Error = <F as TryInto<T>>::Error;
    fn try_into_opt(self) -> Result<Option<T>, Self::Error> {
        match self {
            Some(v) => v.try_into().map(Some),
            None => Ok(None),
        }
    }
}

/// Pre-init the Platform information.
pub use platform::init_cluster_info_or_panic;

/// Prefix for all keys stored in the persistent store (ETCD).
pub const ETCD_KEY_PREFIX: &str = "/openebs.io/mayastor";

/// Enum defining the host access control kind.
#[derive(Debug, Copy, Clone, strum_macros::ToString, strum_macros::EnumString, Eq, PartialEq)]
#[strum(serialize_all = "lowercase")]
pub enum HostAccessControl {
    None,
    Nexuses,
    Replicas,
}
