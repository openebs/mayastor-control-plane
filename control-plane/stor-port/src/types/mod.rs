use crate::{
    transport_api::{ReplyError, ReplyErrorKind},
    types::v0::{
        openapi::{
            actix::server::RestError,
            apis::actix_server::StatusCode,
            models::{rest_json_error::Kind, CustomErrorInfo, CustomErrorPool, RestJsonError},
        },
        transport::{PoolDiag, SnapshotLossInfo, VolumeLossInfo},
    },
};

pub mod v0;

impl From<ReplyError> for RestError<RestJsonError> {
    fn from(value: ReplyError) -> Self {
        rest_error_from(value, None)
    }
}

/// Convert reply and diag information into a rest error.
pub fn rest_error_from(src: ReplyError, diag: Option<PoolDiag>) -> RestError<RestJsonError> {
    rest_error_from_ex(src, diag, None, None)
}

/// Convert a reply error together with volume/snapshot loss information (computed
/// during a purge pre-flight check) into a rest error. Only non-empty loss info is
/// included in the response.
pub fn rest_error_from_loss(
    src: ReplyError,
    volume_loss: VolumeLossInfo,
    snapshot_loss: SnapshotLossInfo,
) -> RestError<RestJsonError> {
    let volume_loss = (!volume_loss.volumes.is_empty()).then_some(volume_loss);
    let snapshot_loss = (!snapshot_loss.snapshots.is_empty()).then_some(snapshot_loss);
    rest_error_from_ex(src, None, volume_loss, snapshot_loss)
}

fn rest_error_from_ex(
    src: ReplyError,
    diag: Option<PoolDiag>,
    volume_loss: Option<VolumeLossInfo>,
    snapshot_loss: Option<SnapshotLossInfo>,
) -> RestError<RestJsonError> {
    let custom = rest_custom(diag, volume_loss, snapshot_loss);

    let (status, kind) = kind_to_rest(&src);

    let details = src.extra;
    let message = src.source;

    let error = RestJsonError::new_all(details, message, kind, custom);
    RestError::new(status, error)
}

fn kind_to_rest(src: &ReplyError) -> (StatusCode, Kind) {
    match &src.kind {
        ReplyErrorKind::WithMessage => (StatusCode::INTERNAL_SERVER_ERROR, Kind::Internal),
        ReplyErrorKind::DeserializeReq => (StatusCode::BAD_REQUEST, Kind::Deserialize),
        ReplyErrorKind::Internal => (StatusCode::INTERNAL_SERVER_ERROR, Kind::Internal),
        ReplyErrorKind::Timeout => (StatusCode::REQUEST_TIMEOUT, Kind::Timeout),
        ReplyErrorKind::InvalidArgument => (StatusCode::BAD_REQUEST, Kind::InvalidArgument),
        ReplyErrorKind::DeadlineExceeded => (StatusCode::GATEWAY_TIMEOUT, Kind::DeadlineExceeded),
        ReplyErrorKind::NotFound => (StatusCode::NOT_FOUND, Kind::NotFound),
        ReplyErrorKind::AlreadyExists => (StatusCode::UNPROCESSABLE_ENTITY, Kind::AlreadyExists),
        ReplyErrorKind::PermissionDenied => (StatusCode::UNAUTHORIZED, Kind::PermissionDenied),
        ReplyErrorKind::ResourceExhausted => {
            (StatusCode::INSUFFICIENT_STORAGE, Kind::ResourceExhausted)
        }
        ReplyErrorKind::FailedPrecondition => {
            (StatusCode::PRECONDITION_FAILED, Kind::FailedPrecondition)
        }
        ReplyErrorKind::Aborted => (StatusCode::SERVICE_UNAVAILABLE, Kind::Aborted),
        ReplyErrorKind::OutOfRange => (StatusCode::RANGE_NOT_SATISFIABLE, Kind::OutOfRange),
        ReplyErrorKind::Unimplemented => (StatusCode::NOT_IMPLEMENTED, Kind::Unimplemented),
        ReplyErrorKind::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, Kind::Unavailable),
        ReplyErrorKind::Unauthenticated => (StatusCode::UNAUTHORIZED, Kind::Unauthenticated),
        ReplyErrorKind::Unauthorized => (StatusCode::UNAUTHORIZED, Kind::Unauthorized),
        ReplyErrorKind::Conflict => (StatusCode::CONFLICT, Kind::Conflict),
        ReplyErrorKind::FailedPersist => (StatusCode::INSUFFICIENT_STORAGE, Kind::FailedPersist),
        ReplyErrorKind::AlreadyShared => (StatusCode::PRECONDITION_FAILED, Kind::AlreadyShared),
        ReplyErrorKind::NotShared => (StatusCode::PRECONDITION_FAILED, Kind::NotShared),
        ReplyErrorKind::NotPublished => (StatusCode::PRECONDITION_FAILED, Kind::NotPublished),
        ReplyErrorKind::AlreadyPublished => {
            (StatusCode::PRECONDITION_FAILED, Kind::AlreadyPublished)
        }
        ReplyErrorKind::PublishedCtxDiffer => {
            (StatusCode::PRECONDITION_FAILED, Kind::PublishedCtxDiffer)
        }
        ReplyErrorKind::FrontendLimitExceeded => {
            (StatusCode::PRECONDITION_FAILED, Kind::FrontendLimitExceeded)
        }
        ReplyErrorKind::Deleting => (StatusCode::CONFLICT, Kind::Deleting),
        ReplyErrorKind::ReplicaCountAchieved => {
            (StatusCode::PRECONDITION_FAILED, Kind::FailedPrecondition)
        }
        ReplyErrorKind::ReplicaChangeCount => {
            (StatusCode::PRECONDITION_FAILED, Kind::FailedPrecondition)
        }
        ReplyErrorKind::ReplicaIncrease => {
            (StatusCode::PRECONDITION_FAILED, Kind::FailedPrecondition)
        }
        ReplyErrorKind::VolumeNoReplicas => {
            (StatusCode::PRECONDITION_FAILED, Kind::FailedPrecondition)
        }
        ReplyErrorKind::InUse => (StatusCode::CONFLICT, Kind::InUse),
        ReplyErrorKind::ReplicaCreateNumber => {
            (StatusCode::PRECONDITION_FAILED, Kind::FailedPrecondition)
        }
        ReplyErrorKind::CapacityLimitExceeded => (
            StatusCode::INSUFFICIENT_STORAGE,
            Kind::CapacityLimitExceeded,
        ),
        ReplyErrorKind::NotAcceptable => (StatusCode::NOT_ACCEPTABLE, Kind::NotAcceptable),
        ReplyErrorKind::Cancelled => (StatusCode::GATEWAY_TIMEOUT, Kind::Cancelled),
        ReplyErrorKind::DiskNotExtended => (StatusCode::PRECONDITION_FAILED, Kind::DiskNotExtended),
        ReplyErrorKind::DiskRescanFailed => {
            (StatusCode::PRECONDITION_FAILED, Kind::DiskRescanFailed)
        }
        ReplyErrorKind::PoolNotPurgeable => {
            (StatusCode::PRECONDITION_FAILED, Kind::PoolNotPurgeable)
        }
        ReplyErrorKind::PoolNotCordoned => (StatusCode::PRECONDITION_FAILED, Kind::PoolNotCordoned),
        ReplyErrorKind::PoolCordonInsufficient => (
            StatusCode::PRECONDITION_FAILED,
            Kind::PoolCordonInsufficient,
        ),
        ReplyErrorKind::PoolPurgeAcceptRequired => (
            StatusCode::PRECONDITION_FAILED,
            Kind::PoolPurgeAcceptRequired,
        ),
        ReplyErrorKind::PoolPurgeVolumeLossAcceptRequired => (
            StatusCode::PRECONDITION_FAILED,
            Kind::PoolPurgeVolumeLossAcceptRequired,
        ),
        ReplyErrorKind::PoolPurgeSnapshotLossAcceptRequired => (
            StatusCode::PRECONDITION_FAILED,
            Kind::PoolPurgeSnapshotLossAcceptRequired,
        ),
        ReplyErrorKind::NodeIsOnline => (StatusCode::PRECONDITION_FAILED, Kind::NodeIsOnline),
        ReplyErrorKind::NodeNotCordoned => (StatusCode::PRECONDITION_FAILED, Kind::NodeNotCordoned),
        ReplyErrorKind::NodeHasResources => {
            (StatusCode::PRECONDITION_FAILED, Kind::NodeHasResources)
        }
        ReplyErrorKind::NodePurgeAcceptRequired => (
            StatusCode::PRECONDITION_FAILED,
            Kind::NodePurgeAcceptRequired,
        ),
        ReplyErrorKind::NodePurgeVolumeLossAcceptRequired => (
            StatusCode::PRECONDITION_FAILED,
            Kind::NodePurgeVolumeLossAcceptRequired,
        ),
        ReplyErrorKind::NodePurgeSnapshotLossAcceptRequired => (
            StatusCode::PRECONDITION_FAILED,
            Kind::NodePurgeSnapshotLossAcceptRequired,
        ),
        ReplyErrorKind::DiskFault => (StatusCode::EXPECTATION_FAILED, Kind::DiskFault),
    }
}

fn rest_custom(
    diag: Option<PoolDiag>,
    volume_loss: Option<VolumeLossInfo>,
    snapshot_loss: Option<SnapshotLossInfo>,
) -> Option<CustomErrorInfo> {
    if diag.is_none() && volume_loss.is_none() && snapshot_loss.is_none() {
        return None;
    }

    Some(CustomErrorInfo {
        pool: CustomErrorPool {
            diag: diag.map(Into::into),
            volume_loss: volume_loss.map(Into::into),
            snapshot_loss: snapshot_loss.map(Into::into),
        },
    })
}
