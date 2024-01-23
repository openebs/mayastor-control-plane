use pstor::error::Error as StoreError;
use pstor_proxy::{
    types::misc::Filter,
    v1::common::{ReplyError, ReplyErrorKind, ResourceKind},
};

#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub), context(suffix(false)))]
pub(crate) enum SvcError {
    #[snafu(display("Storage Error: {source}"))]
    Store { source: StoreError },
    #[snafu(display("Failed to deserialize specs from pstor, {source}"))]
    SpecDeserialize { source: serde_json::Error },
    #[snafu(display("GrpcServer error, {source}"))]
    GrpcServer { source: tonic::transport::Error },
    #[snafu(display("Frontend node not found: {resource_id}"))]
    FrontendNodeNotFound { resource_id: String },
    #[snafu(display("Not a valid filter: {filter}"))]
    InvalidFilter { filter: Filter },
}

impl From<SvcError> for tonic::Status {
    fn from(value: SvcError) -> Self {
        let error_str = value.to_string();
        match value {
            SvcError::Store { .. } => tonic::Status::failed_precondition(error_str),
            SvcError::SpecDeserialize { .. } => tonic::Status::internal(error_str),
            SvcError::GrpcServer { .. } => tonic::Status::internal(error_str),
            SvcError::FrontendNodeNotFound { .. } => tonic::Status::not_found(error_str),
            SvcError::InvalidFilter { .. } => tonic::Status::invalid_argument(error_str),
        }
    }
}

impl From<SvcError> for ReplyError {
    fn from(value: SvcError) -> Self {
        let error_str = value.to_string();
        match value {
            SvcError::Store { .. } => ReplyError {
                kind: ReplyErrorKind::FailedPersist as i32,
                resource: 0,
                source: "pstor".to_string(),
                extra: error_str,
            },
            SvcError::SpecDeserialize { .. } => ReplyError {
                kind: ReplyErrorKind::DeserializeReq as i32,
                resource: 0,
                source: "spec deserialize".to_string(),
                extra: error_str,
            },
            SvcError::GrpcServer { .. } => ReplyError {
                kind: ReplyErrorKind::Internal as i32,
                resource: 0,
                source: "grpc server".to_string(),
                extra: error_str,
            },
            SvcError::FrontendNodeNotFound { .. } => ReplyError {
                kind: ReplyErrorKind::NotFound as i32,
                resource: ResourceKind::FrontendNodeKind as i32,
                source: "frontend node".to_string(),
                extra: error_str,
            },
            SvcError::InvalidFilter { .. } => ReplyError {
                kind: ReplyErrorKind::InvalidArgument as i32,
                resource: 0,
                source: "pstor".to_string(),
                extra: error_str,
            },
        }
    }
}
