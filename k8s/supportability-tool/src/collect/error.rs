use crate::collect::{logs::LogError, resources::ResourceError};
use std::ffi::OsString;

/// Error contains possible errors that can occur while interacting
/// with services in the system
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum Error {
    ResourceError(ResourceError),
    ArchiveError(std::io::Error),
    LogCollectionError(LogError),
    OSStringError(OsString),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::ArchiveError(e)
    }
}

impl From<ResourceError> for Error {
    fn from(e: ResourceError) -> Error {
        Error::ResourceError(e)
    }
}

impl From<LogError> for Error {
    fn from(e: LogError) -> Error {
        Error::LogCollectionError(e)
    }
}

impl From<OsString> for Error {
    fn from(e: OsString) -> Error {
        Error::OSStringError(e)
    }
}
