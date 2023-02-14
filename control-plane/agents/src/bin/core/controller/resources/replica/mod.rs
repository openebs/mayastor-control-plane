use super::{ResourceMutex, ResourceUid};
use stor_port::types::v0::{
    store::replica::{ReplicaSpec, ReplicaState},
    transport::ReplicaId,
};

impl ResourceMutex<ReplicaSpec> {
    /// Get the resource uuid.
    pub fn uuid(&mut self) -> &ReplicaId {
        &self.immutable_ref().uuid
    }
}

impl ResourceUid for ReplicaSpec {
    type Uid = ReplicaId;
    fn uid(&self) -> &Self::Uid {
        &self.uuid
    }
}

impl ResourceUid for ReplicaState {
    type Uid = ReplicaId;
    fn uid(&self) -> &Self::Uid {
        &self.replica.uuid
    }
}
