#![allow(clippy::field_reassign_with_default)]
use super::*;

use crate::{
    bus_impl_message, bus_impl_vector_request, bus_impl_vector_request_token,
    types::v0::message_bus::*,
};

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

bus_impl_message!(Liveness, Liveness, (), Default);

bus_impl_message!(Register, Register, (), Registry);

bus_impl_message!(Deregister, Deregister, (), Registry);

bus_impl_vector_request!(Nodes, Node);
bus_impl_message!(GetNodes, GetNodes, Nodes, Node);

bus_impl_message!(CreatePool, CreatePool, Pool, Pool);

bus_impl_message!(DestroyPool, DestroyPool, (), Pool);

bus_impl_vector_request!(Pools, Pool);
bus_impl_message!(GetPools, GetPools, Pools, Pool);

bus_impl_vector_request!(Replicas, Replica);
bus_impl_message!(GetReplicas, GetReplicas, Replicas, Pool);
bus_impl_message!(CreateReplica, CreateReplica, Replica, Pool);

bus_impl_message!(DestroyReplica, DestroyReplica, (), Pool);

bus_impl_message!(ShareReplica, ShareReplica, String, Pool);

bus_impl_message!(UnshareReplica, UnshareReplica, (), Pool);

bus_impl_vector_request!(Nexuses, Nexus);
bus_impl_message!(GetNexuses, GetNexuses, Nexuses, Nexus);

bus_impl_message!(CreateNexus, CreateNexus, Nexus, Nexus);

bus_impl_message!(DestroyNexus, DestroyNexus, (), Nexus);

bus_impl_message!(ShareNexus, ShareNexus, String, Nexus);

bus_impl_message!(UnshareNexus, UnshareNexus, (), Nexus);

bus_impl_message!(RemoveNexusChild, RemoveNexusChild, (), Nexus);

bus_impl_message!(AddNexusChild, AddNexusChild, Child, Nexus);

bus_impl_vector_request_token!(Volumes, Volume);
bus_impl_message!(GetVolumes, GetVolumes, Volumes, Volume);

bus_impl_message!(CreateVolume, CreateVolume, Volume, Volume);

bus_impl_message!(ShareVolume, ShareVolume, String, Volume);

bus_impl_message!(UnshareVolume, UnshareVolume, (), Volume);

bus_impl_message!(PublishVolume, PublishVolume, Volume, Volume);

bus_impl_message!(UnpublishVolume, UnpublishVolume, Volume, Volume);

bus_impl_message!(DestroyVolume, DestroyVolume, (), Volume);

bus_impl_message!(AddVolumeNexus, AddVolumeNexus, Nexus, Volume);
bus_impl_message!(RemoveVolumeNexus, RemoveVolumeNexus, (), Volume);

bus_impl_message!(SetVolumeReplica, SetVolumeReplica, Volume, Volume);

bus_impl_message!(JsonGrpcRequest, JsonGrpc, Value, JsonGrpc);

bus_impl_vector_request!(BlockDevices, BlockDevice);
bus_impl_message!(GetBlockDevices, GetBlockDevices, BlockDevices, Node);

bus_impl_message!(CreateWatch, CreateWatch, (), Watch);

bus_impl_vector_request!(Watches, Watch);

bus_impl_message!(GetWatches, GetWatches, Watches, Watch);

bus_impl_message!(DeleteWatch, DeleteWatch, (), Watch);

bus_impl_message!(GetSpecs, GetSpecs, Specs, Registry);

bus_impl_message!(GetStates, GetStates, States, Registry);
