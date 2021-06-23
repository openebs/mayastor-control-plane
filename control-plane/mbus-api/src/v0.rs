#![allow(clippy::field_reassign_with_default)]
use super::*;
use serde_json::value::Value;

use types::v0::message_bus::mbus::*;

// Only V0 should export this macro
// This allows the example code to use the v0 default
// Otherwise they have to impl whatever version they require
#[macro_export]
/// Use version 0 of the Message and Channel
macro_rules! impl_channel_id {
    ($I:ident, $C:ident) => {
        fn id(&self) -> MessageId {
            MessageId::v0(MessageIdVs::$I)
        }
        fn channel(&self) -> Channel {
            Channel::v0(ChannelVs::$C)
        }
    };
}

bus_impl_message_all!(Liveness, Liveness, (), Default);

bus_impl_message_all!(ConfigUpdate, ConfigUpdate, (), Kiiss);

bus_impl_message_all!(
    ConfigGetCurrent,
    ConfigGetCurrent,
    ReplyConfig,
    Kiiss,
    GetConfig
);

bus_impl_message_all!(Register, Register, (), Registry);

bus_impl_message_all!(Deregister, Deregister, (), Registry);

bus_impl_vector_request!(Nodes, Node);
bus_impl_message_all!(GetNodes, GetNodes, Nodes, Node);

bus_impl_message_all!(CreatePool, CreatePool, Pool, Pool);

bus_impl_message_all!(DestroyPool, DestroyPool, (), Pool);

bus_impl_vector_request!(Pools, Pool);
bus_impl_message_all!(GetPools, GetPools, Pools, Pool);

bus_impl_vector_request!(Replicas, Replica);
bus_impl_message_all!(GetReplicas, GetReplicas, Replicas, Pool);
bus_impl_message_all!(CreateReplica, CreateReplica, Replica, Pool);

bus_impl_message_all!(DestroyReplica, DestroyReplica, (), Pool);

bus_impl_message_all!(ShareReplica, ShareReplica, String, Pool);

bus_impl_message_all!(UnshareReplica, UnshareReplica, (), Pool);

bus_impl_vector_request!(Nexuses, Nexus);
bus_impl_message_all!(GetNexuses, GetNexuses, Nexuses, Nexus);

bus_impl_message_all!(CreateNexus, CreateNexus, Nexus, Nexus);

bus_impl_message_all!(DestroyNexus, DestroyNexus, (), Nexus);

bus_impl_message_all!(ShareNexus, ShareNexus, String, Nexus);

bus_impl_message_all!(UnshareNexus, UnshareNexus, (), Nexus);

bus_impl_message_all!(RemoveNexusChild, RemoveNexusChild, (), Nexus);

bus_impl_message_all!(AddNexusChild, AddNexusChild, Child, Nexus);

bus_impl_vector_request!(Volumes, Volume);
bus_impl_message_all!(GetVolumes, GetVolumes, Volumes, Volume);

bus_impl_message_all!(CreateVolume, CreateVolume, Volume, Volume);

bus_impl_message_all!(ShareVolume, ShareVolume, String, Volume);

bus_impl_message_all!(UnshareVolume, UnshareVolume, (), Volume);

bus_impl_message_all!(PublishVolume, PublishVolume, String, Volume);

bus_impl_message_all!(UnpublishVolume, UnpublishVolume, (), Volume);

bus_impl_message_all!(DestroyVolume, DestroyVolume, (), Volume);

bus_impl_message_all!(AddVolumeNexus, AddVolumeNexus, Nexus, Volume);

bus_impl_message_all!(RemoveVolumeNexus, RemoveVolumeNexus, (), Volume);

bus_impl_message_all!(JsonGrpcRequest, JsonGrpc, Value, JsonGrpc);

bus_impl_vector_request!(BlockDevices, BlockDevice);
bus_impl_message_all!(GetBlockDevices, GetBlockDevices, BlockDevices, Node);

bus_impl_message_all!(CreateWatch, CreateWatch, (), Watcher);

bus_impl_vector_request!(Watches, Watch);

bus_impl_message_all!(GetWatchers, GetWatches, Watches, Watcher);

bus_impl_message_all!(DeleteWatch, DeleteWatch, (), Watcher);

bus_impl_message_all!(GetSpecs, GetSpecs, Specs, Registry);
