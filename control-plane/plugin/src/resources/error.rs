use crate::resources::node;
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
    #[snafu(display("Invalid label format: {source}"))]
    NodeLabelFormat { source: node::TopologyError },
    #[snafu(display("{source}"))]
    NodeLabel { source: node::OpError },
    /// Error when node uncordon request fails.
    #[snafu(display("Failed to uncordon node {id}. Error {source}"))]
    NodeUncordonError {
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
    /// Error when list snapshots request fails.
    #[snafu(display("Failed to list volume snapshots. Error {source}"))]
    ListSnapshotsError {
        source: openapi::tower::client::Error<openapi::models::RestJsonError>,
    },
}
