impl std::error::Error for Error {}

/// Errors associated with the weight scoring.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Error {
    /// Invalid range - should be from 0 - 100.
    Bounds {},
    /// Sum of weights exceeds 100%.
    Heavy {
        /// The sum of weights encountered.
        sum: u64,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Bounds { .. } => {
                write!(f, "Invalid range")
            }
            Error::Heavy { sum } => {
                write!(f, "Sum of weights ({sum}%) exceeds 100%")
            }
        }
    }
}
