use crate::collect::{
    k8s_resources::k8s_resource_dump::K8sResourceDumperError, logs::LogError,
    persistent_store::EtcdError, resources::ResourceError,
};
use std::ffi::OsString;

/// Error contains possible errors that can occur while interacting
/// with services in the system
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum Error {
    ResourceError(ResourceError),
    ArchiveError(std::io::Error),
    LogCollectionError(LogError),
    K8sResourceDumperError(K8sResourceDumperError),
    OSStringError(OsString),
    EtcdDumpError(EtcdError),
    MultipleErrors(Vec<Error>),
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

impl From<K8sResourceDumperError> for Error {
    fn from(e: K8sResourceDumperError) -> Error {
        Error::K8sResourceDumperError(e)
    }
}

impl From<EtcdError> for Error {
    fn from(e: EtcdError) -> Self {
        Error::EtcdDumpError(e)
    }
}
