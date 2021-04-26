use crate::store::{ObjectKey, StorableObjectType};
use mbus_api::v0;

impl ObjectKey for v0::WatchResourceId {
    fn key_type(&self) -> StorableObjectType {
        match &self {
            v0::WatchResourceId::Node(_) => StorableObjectType::Node,
            v0::WatchResourceId::Pool(_) => StorableObjectType::Pool,
            v0::WatchResourceId::Replica(_) => StorableObjectType::Replica,
            v0::WatchResourceId::ReplicaState(_) => StorableObjectType::ReplicaState,
            v0::WatchResourceId::ReplicaSpec(_) => StorableObjectType::ReplicaSpec,
            v0::WatchResourceId::Nexus(_) => StorableObjectType::Nexus,
            v0::WatchResourceId::Volume(_) => StorableObjectType::Volume,
        }
    }
    fn key_uuid(&self) -> String {
        match &self {
            v0::WatchResourceId::Node(i) => i.to_string(),
            v0::WatchResourceId::Pool(i) => i.to_string(),
            v0::WatchResourceId::Replica(i) => i.to_string(),
            v0::WatchResourceId::ReplicaState(i) => i.to_string(),
            v0::WatchResourceId::ReplicaSpec(i) => i.to_string(),
            v0::WatchResourceId::Nexus(i) => i.to_string(),
            v0::WatchResourceId::Volume(i) => i.to_string(),
        }
    }
}
