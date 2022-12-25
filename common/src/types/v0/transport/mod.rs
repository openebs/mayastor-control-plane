pub mod blockdevice;
pub mod child;
pub mod cluster_agent;
pub mod jsongrpc;
pub mod misc;
pub mod nexus;
pub mod node;
pub mod nvme_nqn;
pub mod pool;
pub mod replica;
pub mod spec;
pub mod state;
pub mod volume;
pub mod watch;

pub use blockdevice::*;
pub use child::*;
pub use cluster_agent::*;
pub use jsongrpc::*;
pub use misc::*;
pub use nexus::*;
pub use node::*;
pub use nvme_nqn::{NvmeNqn as HostNqn, NvmeNqnParseError as HostNqnParseError, *};
pub use pool::*;
pub use replica::*;
pub use spec::*;
pub use state::*;
pub use volume::*;
pub use watch::*;

pub use crate::{
    rpc_impl_string_id, rpc_impl_string_id_inner, rpc_impl_string_id_percent_decoding,
    rpc_impl_string_uuid, rpc_impl_string_uuid_inner,
};

use crate::{transport_api::MessageId, types::v0::openapi::*};

use std::fmt::Debug;
use strum_macros::{EnumString, ToString};

pub const VERSION: &str = "v0";

/// Versioned Message Id's.
#[derive(Debug, PartialEq, Clone, ToString, EnumString)]
#[strum(serialize_all = "camelCase")]
pub enum MessageIdVs {
    /// Default.
    Default,
    /// Liveness Probe.
    Liveness,
    /// Update Config.
    ConfigUpdate,
    /// Request current Config.
    ConfigGetCurrent,
    /// Register the io-engine.
    Register,
    /// Deregister the io-engine.
    Deregister,
    /// Node Service
    /// Get all node information.
    GetNodes,
    /// Cordon a node.
    CordonNode,
    /// Pool Service
    ///
    /// Get pools with filter.
    GetPools,
    /// Create Pool.
    CreatePool,
    /// Destroy Pool.
    DestroyPool,
    /// Get replicas with filter.
    GetReplicas,
    /// Create Replica.
    CreateReplica,
    /// Destroy Replica.
    DestroyReplica,
    /// Share Replica.
    ShareReplica,
    /// Unshare Replica.
    UnshareReplica,
    /// Volume Service
    ///
    /// Get nexuses with filter.
    GetNexuses,
    /// Create nexus.
    CreateNexus,
    /// Destroy Nexus.
    DestroyNexus,
    /// Share Nexus.
    ShareNexus,
    /// Unshare Nexus.
    UnshareNexus,
    /// Remove a child from its parent nexus.
    RemoveNexusChild,
    /// Add a child to a nexus.
    AddNexusChild,
    /// Fault the nexus child.
    FaultNexusChild,
    /// Get all volumes.
    GetVolumes,
    /// Create Volume.
    CreateVolume,
    /// Delete Volume.
    DestroyVolume,
    /// Publish Volume.
    PublishVolume,
    /// Republish Volume.
    RepublishVolume,
    /// Unpublish Volume.
    UnpublishVolume,
    /// Share Volume.
    ShareVolume,
    /// Unshare Volume.
    UnshareVolume,
    /// Add nexus to volume.
    AddVolumeNexus,
    /// Remove nexus from volume.
    RemoveVolumeNexus,
    /// Set replica count.
    SetVolumeReplica,
    /// Generic JSON gRPC message.
    JsonGrpc,
    /// Get block devices.
    GetBlockDevices,
    /// Create new Resource Watch.
    CreateWatch,
    /// Get watches.
    GetWatches,
    /// Delete Resource Watch.
    DeleteWatch,
    /// Get Specs.
    GetSpecs,
    /// Get States.
    GetStates,
    /// High Availability Agents.
    RegisterHaNode,
    /// Report failed NVMe paths.
    ReportFailedPaths,
    /// Shutdown Nexus
    ShutdownNexus,
    /// Replace Path
    ReplacePathInfo,
    /// Get Nvme Subsystems.
    GetNvmeSubsystems,
}

impl From<MessageIdVs> for MessageId {
    fn from(id: MessageIdVs) -> Self {
        MessageId::v0(id)
    }
}
