/// translate between message bus and gRPC
pub mod msg_translation;

use mbus_api::{
    bus, bus_impl_all, bus_impl_message, bus_impl_message_all, bus_impl_publish, bus_impl_request,
    impl_channel_id, v0, BusResult, Channel, DynBus, Message, MessageId, MessagePublish,
    MessageRequest, TimeoutOptions,
};
use serde::{Deserialize, Serialize};
use store::types::v0::{
    nexus::NexusSpec, pool::PoolSpec, replica::ReplicaSpec, volume::VolumeSpec,
};

/// Retrieve all specs from core agent
/// For testing and debugging only
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetSpecs {}
bus_impl_message_all!(GetSpecs, GetSpecs, Specs, Registry);

/// Specs for testing and debugging only
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Specs {
    /// volume specs
    pub volumes: Vec<VolumeSpec>,
    /// nexus specs
    pub nexuses: Vec<NexusSpec>,
    /// pool specs
    pub pools: Vec<PoolSpec>,
    /// replica specs
    pub replicas: Vec<ReplicaSpec>,
}
