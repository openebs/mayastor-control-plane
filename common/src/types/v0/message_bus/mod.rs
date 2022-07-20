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

use crate::types::Channel;

use crate::types::v0::openapi::*;
use std::fmt::Debug;
use strum_macros::{EnumString, ToString};

use crate::mbus_api::{BusClient, DynBus, MessageIdTimeout, TimeoutOptions};
pub use crate::{
    bus_impl_string_id, bus_impl_string_id_inner, bus_impl_string_id_percent_decoding,
    bus_impl_string_uuid, bus_impl_string_uuid_inner,
};
use std::time::Duration;

pub const VERSION: &str = "v0";

/// Versioned Channels
#[derive(Clone, Debug, EnumString, ToString, PartialEq)]
#[strum(serialize_all = "camelCase")]
pub enum ChannelVs {
    /// Default
    Default,
    /// Registration of io-engine instances with the control plane
    Registry,
    /// Node Service which exposes the registered io-engine instances
    Node,
    /// Pool Service which manages io-engine pools and replicas
    Pool,
    /// Volume Service which manages io-engine volumes
    Volume,
    /// Nexus Service which manages io-engine nexuses
    Nexus,
    /// Keep it In Sync Service
    Kiiss,
    /// Json gRPC Agent
    JsonGrpc,
    /// Core Agent combines Node, Pool and Volume services
    Core,
    /// Watch Agent
    Watch,
}
impl Default for ChannelVs {
    fn default() -> Self {
        ChannelVs::Default
    }
}

impl From<ChannelVs> for Channel {
    fn from(channel: ChannelVs) -> Self {
        Channel::v0(channel)
    }
}

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

impl MessageIdTimeout for MessageIdVs {
    fn timeout_opts(&self, opts: TimeoutOptions, bus: &DynBus) -> TimeoutOptions {
        let timeout = self.timeout(opts.timeout, bus);
        opts.with_timeout(timeout)
    }
    fn timeout(&self, timeout: Duration, bus: &DynBus) -> Duration {
        if let Some(min_timeouts) = bus.timeout_opts().request_timeout() {
            let timeout = Duration::max(
                timeout,
                match self {
                    MessageIdVs::CreateVolume => min_timeouts.replica() * 3 + min_timeouts.nexus(),
                    MessageIdVs::DestroyVolume => min_timeouts.replica() * 3 + min_timeouts.nexus(),
                    MessageIdVs::PublishVolume => min_timeouts.nexus(),
                    MessageIdVs::UnpublishVolume => min_timeouts.nexus(),

                    MessageIdVs::CreateNexus => min_timeouts.nexus(),
                    MessageIdVs::DestroyNexus => min_timeouts.nexus(),

                    MessageIdVs::CreateReplica => min_timeouts.replica(),
                    MessageIdVs::DestroyReplica => min_timeouts.replica(),
                    _ => timeout,
                },
            )
            .min(Duration::from_secs(59));
            match bus.client_name() {
                // the rest server should have some slack to allow for the CoreAgent to timeout
                // first
                BusClient::RestServer => timeout + Duration::from_secs(1),
                BusClient::CoreAgent => timeout,
                _ => timeout,
            }
        } else {
            timeout
        }
    }
}
