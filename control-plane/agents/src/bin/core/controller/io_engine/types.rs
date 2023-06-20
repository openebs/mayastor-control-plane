use std::time::SystemTime;
use stor_port::types::v0::transport;

/// Re-export creation types.
pub(crate) use transport::{CreateNexusSnapReplDescr, CreateNexusSnapshot};

/// A response for the nexus snapshot request.
pub struct CreateNexusSnapshotResp {
    /// The nexus involved in the snapshot operation.
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
