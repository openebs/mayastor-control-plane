pub mod blockdevice;
pub mod child;
pub mod jsongrpc;
pub mod misc;
pub mod nexus;
pub mod node;
pub mod pool;
pub mod replica;
pub mod spec;
pub mod state;
pub mod volume;
pub mod watch;

pub use blockdevice::*;
pub use child::*;
pub use jsongrpc::*;
pub use misc::*;
pub use nexus::*;
pub use node::*;
pub use pool::*;
pub use replica::*;
pub use spec::*;
pub use state::*;
pub use volume::*;
pub use watch::*;

use crate::types::v0::openapi::*;
use std::fmt::Debug;
use strum_macros::{EnumString, ToString};

use crate::transport_api::MessageId;
pub use crate::{
    bus_impl_string_id, bus_impl_string_id_inner, bus_impl_string_id_percent_decoding,
    bus_impl_string_uuid, bus_impl_string_uuid_inner,
};

pub const VERSION: &str = "v0";

/// Versioned Message Id's
#[derive(Debug, PartialEq, Clone, ToString, EnumString)]
#[strum(serialize_all = "camelCase")]
pub enum MessageIdVs {
    /// Default
    Default,
    /// Liveness Probe
    Liveness,
    /// Update Config
    ConfigUpdate,
    /// Request current Config
    ConfigGetCurrent,
    /// Register the io-engine
    Register,
    /// Deregister the io-engine
    Deregister,
    /// Node Service
    /// Get all node information
    GetNodes,
    /// Cordon a node
    CordonNode,
    /// Pool Service
    ///
    /// Get pools with filter
    GetPools,
    /// Create Pool,
    CreatePool,
    /// Destroy Pool,
    DestroyPool,
    /// Get replicas with filter
    GetReplicas,
    /// Create Replica,
    CreateReplica,
    /// Destroy Replica,
    DestroyReplica,
    /// Share Replica,
    ShareReplica,
    /// Unshare Replica,
    UnshareReplica,
    /// Volume Service
    ///
    /// Get nexuses with filter
    GetNexuses,
    /// Create nexus
    CreateNexus,
    /// Destroy Nexus
    DestroyNexus,
    /// Share Nexus
    ShareNexus,
    /// Unshare Nexus
    UnshareNexus,
    /// Remove a child from its parent nexus
    RemoveNexusChild,
    /// Add a child to a nexus
    AddNexusChild,
    /// Get all volumes
    GetVolumes,
    /// Create Volume,
    CreateVolume,
    /// Delete Volume
    DestroyVolume,
    /// Publish Volume,
    PublishVolume,
    /// Unpublish Volume
    UnpublishVolume,
    /// Share Volume
    ShareVolume,
    /// Unshare Volume
    UnshareVolume,
    /// Add nexus to volume
    AddVolumeNexus,
    /// Remove nexus from volume
    RemoveVolumeNexus,
    /// Set replica count
    SetVolumeReplica,
    /// Generic JSON gRPC message
    JsonGrpc,
    /// Get block devices
    GetBlockDevices,
    /// Create new Resource Watch
    CreateWatch,
    /// Get watches
    GetWatches,
    /// Delete Resource Watch
    DeleteWatch,
    /// Get Specs
    GetSpecs,
    /// Get States
    GetStates,
}

impl From<MessageIdVs> for MessageId {
    fn from(id: MessageIdVs) -> Self {
        MessageId::v0(id)
    }
}
