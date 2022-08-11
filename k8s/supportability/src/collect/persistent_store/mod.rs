use crate::collect::k8s_resources::client::K8sResourceError;
use common_lib::types::v0::store::definitions::StoreError;
use std::io::Error;

pub mod etcd;

/// EtcdError holds the errors that can occur while trying to dump information
/// from etcd database
#[derive(Debug)]
pub(crate) enum EtcdError {
    Etcd(StoreError),
    K8sResource(K8sResourceError),
    IOError(std::io::Error),
    Custom(String),
    CreateClient(anyhow::Error),
}

impl From<StoreError> for EtcdError {
    fn from(e: StoreError) -> Self {
        EtcdError::Etcd(e)
    }
}

impl From<std::io::Error> for EtcdError {
    fn from(e: Error) -> Self {
        EtcdError::IOError(e)
    }
}

impl From<K8sResourceError> for EtcdError {
    fn from(e: K8sResourceError) -> Self {
        EtcdError::K8sResource(e)
    }
}
