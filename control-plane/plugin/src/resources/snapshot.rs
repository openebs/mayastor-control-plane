use crate::{
    operations::GetSnapshots,
    resources::{utils, SnapshotId, VolumeId},
    rest_wrapper::RestClient,
};
use async_trait::async_trait;

use crate::resources::utils::{CreateRow, GetHeaderRow};
use prettytable::Row;

/// A collection of VolumeSnapshot resource.
#[derive(clap::Args, Debug)]
pub struct VolumeSnapshots {}

#[derive(Debug, Clone, clap::Args)]
/// Volume Snapshot args.
pub struct VolumeSnapshotArgs {
    #[clap(long)]
    /// Uuid of the volume (Optional).
    volume: Option<VolumeId>,
    #[clap(long)]
    /// Uuid of the snapshot (Optional).
    snapshot: Option<SnapshotId>,
}

impl VolumeSnapshotArgs {
    /// Get the volume id from args.
    pub fn volume(&self) -> Option<VolumeId> {
        self.volume
    }

    /// Get the snapshot id from args.
    pub fn snapshot(&self) -> Option<SnapshotId> {
        self.snapshot
    }
}

impl CreateRow for openapi::models::VolumeSnapshot {
    fn row(&self) -> Row {
        let state = &self.state;
        row![
            state.uuid,
            state.creation_timestamp,
            state.size,
            state.source_volume
        ]
    }
}

#[async_trait(?Send)]
impl GetSnapshots for VolumeSnapshots {
    type SourceID = Option<VolumeId>;
    type ResourceID = Option<SnapshotId>;
    async fn get_snapshots(
        volid: &Self::SourceID,
        snapid: &Self::ResourceID,
        output: &utils::OutputFormat,
    ) {
        if let Some(snapshots) = get_snapshots(volid, snapid).await {
            // Print table, json or yaml based on output format.
            utils::print_table(output, snapshots);
        }
    }
}

impl GetHeaderRow for openapi::models::VolumeSnapshot {
    fn get_header_row(&self) -> Row {
        (*utils::SNAPSHOT_HEADERS).clone()
    }
}

async fn get_snapshots(
    volid: &Option<VolumeId>,
    snapid: &Option<SnapshotId>,
) -> Option<Vec<openapi::models::VolumeSnapshot>> {
    // XXX: 500 is a ballpark number. Once pagination is implemented, we'll reduce this
    // to max entries per call.
    let mut snapshots = Vec::with_capacity(500);
    match RestClient::client()
        .snapshots_api()
        .get_volumes_snapshots(volid.as_ref(), snapid.as_ref())
        .await
    {
        Ok(snaps) => {
            let s = snaps.into_body();
            snapshots.extend(s.entries);
        }
        Err(e) => {
            println!("Failed to list volume snapshots. Error {e}");
            return None;
        }
    }

    Some(snapshots)
}
