#![allow(clippy::field_reassign_with_default)]
use super::*;

use crate::{
    impl_message, impl_vector_request, impl_vector_request_token, types::v0::transport::*,
};

// Only V0 should export this macro
// This allows the example code to use the v0 default
// Otherwise they have to impl whatever version they require
#[macro_export]
/// Use version 0 of the Message
macro_rules! impl_channel_id {
    ($I:ident) => {
        fn id(&self) -> MessageId {
            MessageId::v0(MessageIdVs::$I)
        }
    };
}

impl_message!(Liveness);
impl_message!(Register);
impl_message!(Deregister);

impl_vector_request!(Nodes, Node);
impl_message!(GetNodes);

impl_message!(CreatePool);
impl_message!(DestroyPool);
impl_vector_request!(Pools, Pool);
impl_message!(GetPools);

impl_vector_request!(NvmeSubsystems, NvmeSubsystem);

impl_vector_request!(Replicas, Replica);
impl_message!(GetReplicas);
impl_message!(CreateReplica);
impl_message!(DestroyReplica);
impl_message!(ShareReplica);
impl_message!(UnshareReplica);

impl_vector_request!(Nexuses, Nexus);
impl_message!(GetNexuses);
impl_message!(CreateNexus);
impl_message!(DestroyNexus);
impl_message!(ShareNexus);
impl_message!(UnshareNexus);
impl_message!(RemoveNexusChild);
impl_message!(AddNexusChild);
impl_message!(FaultNexusChild);
impl_message!(ShutdownNexus);

impl_vector_request_token!(Volumes, Volume);
impl_message!(GetVolumes);
impl_message!(CreateVolume);
impl_message!(ShareVolume);
impl_message!(UnshareVolume);
impl_message!(PublishVolume);
impl_message!(UnpublishVolume);
impl_message!(DestroyVolume);
impl_message!(AddVolumeNexus);
impl_message!(RemoveVolumeNexus);
impl_message!(SetVolumeReplica);

impl_message!(JsonGrpcRequest, JsonGrpc);

impl_vector_request!(BlockDevices, BlockDevice);
impl_message!(GetBlockDevices);

impl_vector_request!(Watches, Watch);
impl_message!(CreateWatch);
impl_message!(GetWatches);
impl_message!(DeleteWatch);

impl_message!(GetSpecs);
impl_message!(GetStates);
