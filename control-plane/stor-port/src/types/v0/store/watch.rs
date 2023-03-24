use crate::types::v0::{
    store::definitions::{ObjectKey, StorableObjectType},
    transport::WatchResourceId,
};
use pstor::ApiVersion;

impl ObjectKey for WatchResourceId {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

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
