use std::{collections::HashMap, time::SystemTime};
use stor_port::types::v0::{
    transport,
    transport::{CreatePool, ImportPool, NexusId, PoolDiskError, RebuildHistory},
};
/// Re-export creation types.
pub(crate) use transport::{CreateNexusSnapReplDescr, CreateNexusSnapshot};

/// A response for the nexus snapshot request.
pub struct CreateNexusSnapshotResp {
    /// The nexus involved in the snapshot operation.
    #[allow(unused)]
    pub nexus: transport::Nexus,
    /// Timestamp when the nexus was paused to take the snapshot on all replicas.
    pub snap_time: SystemTime,
    /// Results of snapping each replica as part of this snapshot request.
    pub replicas_status: Vec<CreateNexusSnapshotReplicaStatus>,
    /// Replicas which weren't snapped as part of this request.
    pub skipped: Vec<transport::ReplicaId>,
}

/// Per-replica status of the snapshot operation.
pub struct CreateNexusSnapshotReplicaStatus {
    /// UUID of replica.
    pub replica_uuid: transport::ReplicaId,
    /// Result of snapping this replica.
    pub error: Option<nix::errno::Errno>,
}

/// Rebuild history response.
pub(crate) struct RebuildHistoryResp {
    pub(crate) end_time: Option<prost_types::Timestamp>,
    pub(crate) histories: HashMap<NexusId, RebuildHistory>,
}

/// Create a Snapshot Rebuild on target node where the replica resides on.
#[allow(dead_code)]
pub(crate) struct CreateSnapRebuild {
    /// The uuid of the rebuilding replica.
    pub(crate) replica_uuid: transport::ReplicaId,
    /// The nvmf URI of the source Snapshot.
    pub(crate) source_uri: String,
}
/// The status of the snapshot Rebuild.
#[allow(dead_code)]
pub(crate) type SnapshotRebuildStatus = rpc::v1::snapshot_rebuild::RebuildStatus;
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct SnapshotRebuild {
    /// The uuid of the rebuild job.
    pub(crate) uuid: transport::ReplicaId,
    /// The uuid of the rebuilding replica.
    pub(crate) replica_uuid: transport::ReplicaId,
    /// The uuid of the source snapshot.
    pub(crate) snapshot_uuid: transport::SnapshotId,
    /// The nvmf URI of the source Snapshot from which we:
    /// 1. read allocated data bitmap
    /// 2. read data proper
    pub(crate) snapshot_uri: String,
    /// The nvmf URI of the destination Replica.
    pub(crate) replica_uri: String,
    /// Current status of the rebuild.
    pub(crate) status: SnapshotRebuildStatus,
    /// Total bytes to rebuild.
    pub(crate) total: u64,
    /// Total bytes rebuilt so far.
    pub(crate) rebuilt: u64,
    /// Remaining bytes to rebuild.
    pub(crate) remaining: u64,
    /// Cluster index of the last persistence checkpoint.
    /// All previous clusters have been rebuilt and this
    /// is persisted in the replica metadata.
    pub(crate) persisted_checkpoint: u64,
    /// Timestamp taken at the start of the rebuild.
    pub(crate) start_timestamp: Option<SystemTime>,
    /// Timestamp taken at the end of the rebuild.
    pub(crate) end_timestamp: Option<SystemTime>,
}
/// List all or a specific snapshot rebuild.
#[allow(dead_code)]
pub(crate) struct ListSnapRebuild {
    /// Select a specific target replica.
    pub(crate) uuid: Option<transport::ReplicaId>,
}
/// A list of snapshot rebuilds.
#[allow(dead_code)]
pub(crate) type ListSnapRebuildRsp = Vec<SnapshotRebuild>;
/// Destroy a snapshot rebuild.
#[allow(dead_code)]
pub(crate) struct DestroySnapRebuild {
    /// The uuid of the particular rebuild to destroy, which is also the uuid of the replica
    /// which is being rebuilt.
    pub(crate) uuid: transport::ReplicaId,
}

/// Probe pool disks for errors.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProbePoolRequest {
    /// A request like [`ImportPool`].
    pub(crate) import: ImportPool,
    pub(crate) is_import: bool,
}
impl From<ImportPool> for ProbePoolRequest {
    fn from(import: ImportPool) -> Self {
        Self {
            import,
            is_import: true,
        }
    }
}
impl From<&CreatePool> for ProbePoolRequest {
    fn from(req: &CreatePool) -> Self {
        let import = ImportPool::new(&req.node, &req.id, &req.disks, &req.encryption);
        Self {
            import,
            is_import: false,
        }
    }
}

/// Probe pool disks for errors.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProbePoolResponse {
    /// Specified probes found no issues.
    pub(crate) success: bool,
    pub(crate) unimpl: bool,
    /// All the probe errors (per disk).
    pub(crate) errors: HashMap<String, ProbeDiskInfo>,
}
impl ProbePoolResponse {
    /// The target io-engine does not implement this api.
    pub(crate) fn new_unimpl() -> Self {
        Self {
            unimpl: true,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProbeDiskInfo {
    /// Any errors found by the probe.
    #[allow(unused)]
    pub(crate) error: Vec<PoolDiskError>,
}
