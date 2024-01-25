use strum_macros::{AsRefStr, Display, EnumString};

/// All types of objects which are storable in our store.
#[derive(Display, AsRefStr, Copy, Clone, Debug)]
#[allow(dead_code)]
pub enum StorableObjectType {
    WatchConfig,
    Volume,
    Nexus,
    NexusSpec,
    NexusState,
    NexusInfo,
    Node,
    NodeSpec,
    Pool,
    PoolSpec,
    Replica,
    ReplicaState,
    ReplicaSpec,
    ReplicaSnapshot,
    VolumeSpec,
    VolumeState,
    VolumeSnapshot,
    ChildSpec,
    ChildState,
    CoreRegistryConfig,
    StoreLeaseLock,
    StoreLeaseOwner,
    SwitchOver,
    AppNodeSpec,
}

/// Control plane api versions.
#[derive(EnumString, Display)]
#[strum(serialize_all = "lowercase")]
pub enum ApiVersion {
    V0,
}
