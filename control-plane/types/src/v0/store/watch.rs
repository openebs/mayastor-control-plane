use crate::v0::{
    message_bus::mbus::WatchResourceId,
    store::definitions::{ObjectKey, StorableObjectType},
};

impl ObjectKey for WatchResourceId {
    fn key_type(&self) -> StorableObjectType {
        match &self {
            WatchResourceId::Node(_) => StorableObjectType::Node,
            WatchResourceId::Pool(_) => StorableObjectType::Pool,
            WatchResourceId::Replica(_) => StorableObjectType::Replica,
            WatchResourceId::ReplicaState(_) => StorableObjectType::ReplicaState,
            WatchResourceId::ReplicaSpec(_) => StorableObjectType::ReplicaSpec,
            WatchResourceId::Nexus(_) => StorableObjectType::Nexus,
            WatchResourceId::Volume(_) => StorableObjectType::Volume,
        }
    }
    fn key_uuid(&self) -> String {
        match &self {
            WatchResourceId::Node(i) => i.to_string(),
            WatchResourceId::Pool(i) => i.to_string(),
            WatchResourceId::Replica(i) => i.to_string(),
            WatchResourceId::ReplicaState(i) => i.to_string(),
            WatchResourceId::ReplicaSpec(i) => i.to_string(),
            WatchResourceId::Nexus(i) => i.to_string(),
            WatchResourceId::Volume(i) => i.to_string(),
        }
    }
}
