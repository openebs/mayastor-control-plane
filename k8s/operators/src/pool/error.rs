use kube::core::crd::MergeError;
use openapi::{clients, models::RestJsonError};
use snafu::Snafu;

/// Errors generated during the reconciliation loop
#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Kubernetes client error: {}", source))]
    /// k8s client error
    Kube { source: kube::Error },
    #[snafu(display("HTTP request error: {}", source))]
    Request {
        source: clients::tower::RequestError,
    },
    #[snafu(display("HTTP response error: {}", source))]
    Response {
        source: clients::tower::ResponseError<RestJsonError>,
    },
    #[snafu(display("Invalid cr field : {}", field))]
    InvalidCRField { field: String },
    #[snafu(display("{}", message))]
    Generic { message: String },
    #[snafu(display("CRD merge failed"))]
    CrdMergeError { source: MergeError },
    #[snafu(display("{} for CRD : {}", field, name))]
    CrdFieldMissing { name: String, field: String },
}

impl From<clients::tower::Error<RestJsonError>> for Error {
    fn from(source: clients::tower::Error<RestJsonError>) -> Self {
        match source {
            clients::tower::Error::Request(source) => Error::Request { source },
            clients::tower::Error::Response(source) => Self::Response { source },
        }
    }
}

impl From<kube::Error> for Error {
    fn from(source: kube::Error) -> Self {
        Self::Kube { source }
    }
}
