use super::{ResourceMutex, ResourceUid};
use stor_port::types::v0::{
    store::replica::{ReplicaSpec, ReplicaState},
    transport::ReplicaId,
};

impl ResourceMutex<ReplicaSpec> {
    /// Get the resource uuid.
    pub fn uuid(&self) -> &ReplicaId {
        &self.immutable_ref().uuid
    }
}

impl ResourceUid for ReplicaSpec {
    type Uid = ReplicaId;
    fn uid(&self) -> &Self::Uid {
        &self.uuid
    }
    fn rkind(&self) -> stor_port::transport_api::ResourceKind {
        stor_port::transport_api::ResourceKind::Replica
    }
}

impl ResourceUid for ReplicaState {
    type Uid = ReplicaId;
    fn uid(&self) -> &Self::Uid {
        &self.replica.uuid
    }
    fn rkind(&self) -> stor_port::transport_api::ResourceKind {
        stor_port::transport_api::ResourceKind::Replica
    }
}
