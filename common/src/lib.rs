pub mod constants;
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

pub use constants::*;
