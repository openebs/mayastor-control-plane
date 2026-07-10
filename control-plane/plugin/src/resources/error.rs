use snafu::Snafu;

/// All errors returned when resources command fails.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    /// Error when listing block devices fails.
    #[snafu(display("Failed to list blockdevices for node {id}. Error {source}"))]
    GetBlockDevicesError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when get node request fails.
    #[snafu(display("Failed to get node {id}. Error {source}"))]
    GetNodeError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when node cordon request fails.
    #[snafu(display("Failed to get node {id}. Error {source}"))]
    NodeCordonError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when pool cordon request fails.
    #[snafu(display("Failed to cordon pool {id}. Error {source}"))]
    PoolCordonError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    #[snafu(display("Invalid label format: {source}"))]
    NodeLabelFormat { source: TopologyError },
    #[snafu(display("{source}"))]
    NodeLabel { source: OpError },
    #[snafu(display("Invalid label format: {source}"))]
    PoolLabelFormat { source: TopologyError },
    #[snafu(display("{source}"))]
    PoolLabel { source: OpError },
    /// Error when node uncordon request fails.
    #[snafu(display("Failed to uncordon node {id}. Error {source}"))]
    NodeUncordonError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when node uncordon request fails.
    #[snafu(display("Failed to uncordon pool {id}. Error {source}"))]
    PoolUncordonError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when put node drain request fails.
    #[snafu(display("Failed to put node drain {id}. Error {source}"))]
    PutNodeDrainError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when list nodes request fails.
    #[snafu(display("Failed to list nodes. Error {source}"))]
    ListNodesError {
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when get pool request fails.
    #[snafu(display("Failed to get pool {id}. Error {source}"))]
    GetPoolError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    #[snafu(display("Failed to delete pool {id}. Error {source}{}", hint.as_deref().map(|h| format!("\nHint: {h}")).unwrap_or_default()))]
    DeletePoolError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
        /// Optional contextual hint shown when the pool is already being deleted/purged.
        hint: Option<String>,
    },
    #[snafu(display("Failed to delete node {id}. Error {source}"))]
    DeleteNodeError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when get pool request fails.
    #[snafu(display("No state for pool {id}. Please verify if node is online"))]
    PoolStateError { id: String },
    /// Error when list pools request fails.
    #[snafu(display("Failed to list pools. Error {source}"))]
    ListPoolsError {
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when get volume request fails.
    #[snafu(display("Failed to get volume {id}. Error {source}"))]
    GetVolumeError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when delete volume request fails.
    #[snafu(display("Failed to delete volume {id}. Error {source}"))]
    DelVolumeError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when get rebuild history for volume request fails.
    #[snafu(display("Failed to get rebuild history for volume {id}. Error {source}"))]
    GetRebuildHistory {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when scale volume request fails.
    #[snafu(display("Failed to scale volume {id}. Error {source}"))]
    ScaleVolumeError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when expand pool request fails.
    #[snafu(display("Failed to expand pool {id}. Error {source}"))]
    ExpandPoolError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when resize volume request fails.
    #[snafu(display("Failed to resize volume {id}. Error {source}"))]
    ResizeVolumeError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when set volume property request fails.
    #[snafu(display("Failed to set volume {id} property, Error {source}"))]
    SetVolumePropertyError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when list snapshots request fails.
    #[snafu(display("Failed to list volume snapshots. Error {source}"))]
    ListSnapshotsError {
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when delete snapshots request fails.
    #[snafu(display("Failed to delete volume snapshot. Error {source}"))]
    DelSnapshotsError {
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// Error when get pool request fails.
    #[snafu(display(
        "Error while parsing labels `{labels}`. \
        The supported formats for labels is: \
        key1=value1,key2=value2"
    ))]
    LabelNodeFilter { labels: String },
    /// Error when interacting via console dialogue.
    #[snafu(display("{source}"))]
    Dialogue {
        source: inquire::error::InquireError,
    },
    /// User has decided to abort the operation following a dialogue.
    #[snafu(display("Operation was aborted by the user"))]
    DialogueAbort {},
    /// Error when pool clear-errors request fails.
    #[snafu(display("Failed to clear errors for pool {id}. Error {source}"))]
    PoolClearError {
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
    /// A purge/delete precondition was not met.
    #[snafu(display("{reason}"))]
    Purge { reason: PurgeReason },
}

/// Errors related to label topology formats.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum TopologyError {
    #[snafu(display("key must not be an empty string"))]
    KeyIsEmpty {},
    #[snafu(display("value must not be an empty string"))]
    ValueIsEmpty {},
    #[snafu(display("key part must not be more than 63 characters"))]
    KeyTooLong {},
    #[snafu(display("value part must not be more than 63 characters"))]
    ValueTooLong {},
    #[snafu(display("both key and value parts must start with an ascii alphanumeric character"))]
    EdgesNotAlphaNum {},
    #[snafu(display("key can contain at most one `/` character"))]
    KeySlashCount {},
    #[snafu(display(
        "only ascii alphanumeric characters and (`/`,` - `, `_`,`.`) are allowed for the key part"
    ))]
    KeyIsNotAlphaNumericPlus {},
    #[snafu(display(
        "only ascii alphanumeric characters and (`-`,` _ `, `.`) are allowed for the label part"
    ))]
    ValueIsNotAlphaNumericPlus {},
    #[snafu(display("only a single assignment key=value is allowed"))]
    LabelMultiAssign {},
    #[snafu(display(
        "the supported formats are: \
        key=value for adding (example: group=a) \
        and key- for removing (example: group-)"
    ))]
    LabelAssign {},
}

/// Errors related to node label topology operation execution.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub))]
pub enum OpError {
    #[snafu(display("{resource} {id} not unlabelled as it did not contain the label"))]
    LabelNotFound { resource: String, id: String },
    #[snafu(display("{resource} {id} not labelled as the same label already exists"))]
    LabelExists { resource: String, id: String },
    #[snafu(display("{resource} {id} not found"))]
    ResourceNotFound { resource: String, id: String },
    #[snafu(display(
        "{resource} {id} not labelled as the label key already exists, but with a different value and --overwrite is false"
    ))]
    LabelConflict { resource: String, id: String },
    #[snafu(display("Failed to label {resource} {id}. Error {source}"))]
    Generic {
        resource: String,
        id: String,
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
}

/// Known purge/delete precondition failures, with user-facing messages
/// that reference the CLI flags needed to resolve the issue.
#[derive(Debug, strum_macros::Display)]
pub enum PurgeReason {
    #[strum(to_string = "Node is online. Only offline nodes can be deleted.")]
    NodeIsOnline,
    #[strum(to_string = "Node must be cordoned first. Use: cordon node <id> <label>")]
    NodeNotCordoned,
    #[strum(
        to_string = "Node has resources. Use --purge to force-remove the node and all its resources."
    )]
    NodeHasResources,
    #[strum(to_string = "Node has pools with data. Confirm with --yes to proceed.")]
    NodePurgeAcceptRequired,
    #[strum(to_string = "Volumes would lose their last healthy replica. \
                 Use --accept-volume-loss to proceed, or --accept-data-loss \
                 to also accept snapshot loss in a single flag.")]
    NodePurgeVolumeLoss,
    #[strum(to_string = "Snapshots would lose their last replica snapshot. \
                 Use --accept-snapshot-loss to proceed, or --accept-data-loss \
                 to also accept volume loss in a single flag.")]
    NodePurgeSnapshotLoss,
    #[strum(
        to_string = "Pool state is not Offline or Unknown. Only pools with Offline or Unknown state can be purged."
    )]
    PoolNotPurgeable,
    #[strum(
        to_string = "Pool must be cordoned first. Use: cordon pool <id> --replicas --snapshots"
    )]
    PoolNotCordoned,
    #[strum(
        to_string = "Pool cordon must block both replicas and snapshots. Use: cordon pool <id> --replicas --snapshots"
    )]
    PoolCordonInsufficient,
    #[strum(to_string = "Pool has replicas. Confirm with --yes to proceed.")]
    PoolPurgeAcceptRequired,
    #[strum(to_string = "Volumes would lose their last healthy replica. \
                 Use --accept-volume-loss to proceed, or --accept-data-loss \
                 to also accept snapshot loss in a single flag.")]
    PoolPurgeVolumeLoss,
    #[strum(to_string = "Snapshots would lose their last replica snapshot. \
                 Use --accept-snapshot-loss to proceed, or --accept-data-loss \
                 to also accept volume loss in a single flag.")]
    PoolPurgeSnapshotLoss,
}

impl PurgeReason {
    /// Try to extract a known purge reason from an API error.
    pub fn from_api_error(
        source: &openapi::tower::client::Error<openapi::models::RestJsonError>,
    ) -> Option<Self> {
        use openapi::models::rest_json_error::Kind;
        let kind = source.error_body().map(|b| b.kind)?;
        match kind {
            Kind::NodeIsOnline => Some(Self::NodeIsOnline),
            Kind::NodeNotCordoned => Some(Self::NodeNotCordoned),
            Kind::NodeHasResources => Some(Self::NodeHasResources),
            Kind::NodePurgeAcceptRequired => Some(Self::NodePurgeAcceptRequired),
            Kind::NodePurgeVolumeLossAcceptRequired => Some(Self::NodePurgeVolumeLoss),
            Kind::NodePurgeSnapshotLossAcceptRequired => Some(Self::NodePurgeSnapshotLoss),
            Kind::PoolNotPurgeable => Some(Self::PoolNotPurgeable),
            Kind::PoolNotCordoned => Some(Self::PoolNotCordoned),
            Kind::PoolCordonInsufficient => Some(Self::PoolCordonInsufficient),
            Kind::PoolPurgeAcceptRequired => Some(Self::PoolPurgeAcceptRequired),
            Kind::PoolPurgeVolumeLossAcceptRequired => Some(Self::PoolPurgeVolumeLoss),
            Kind::PoolPurgeSnapshotLossAcceptRequired => Some(Self::PoolPurgeSnapshotLoss),
            _ => None,
        }
    }

    /// Whether this purge reason indicates that volume or snapshot data
    /// would be lost, and so the impact should be shown to the user.
    pub fn is_data_loss(&self) -> bool {
        matches!(
            self,
            Self::NodePurgeVolumeLoss
                | Self::NodePurgeSnapshotLoss
                | Self::PoolPurgeVolumeLoss
                | Self::PoolPurgeSnapshotLoss
        )
    }
}

impl From<TopologyError> for Error {
    fn from(source: TopologyError) -> Self {
        Self::NodeLabelFormat { source }
    }
}
impl From<OpError> for Error {
    fn from(source: OpError) -> Self {
        Self::NodeLabel { source }
    }
}
