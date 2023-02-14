use crate::{
    context::Context,
    misc::traits::{StringValue, ValidateRequestTypes},
    watch,
    watch::watch_resource_id,
};
use std::convert::TryFrom;
use stor_port::{
    transport_api::{v0::Watches, ReplyError, ResourceKind},
    types::v0::transport::{
        CreateWatch, DeleteWatch, GetWatches, NexusId, ReplicaId, VolumeId, Watch, WatchCallback,
        WatchResourceId, WatchType,
    },
};

/// All watch crud operations to be a part of the WatchOperations trait
#[tonic::async_trait]
pub trait WatchOperations: Send + Sync {
    /// Create a watch
    async fn create(&self, req: &dyn WatchInfo, ctx: Option<Context>) -> Result<(), ReplyError>;
    /// Get watches
    async fn get(
        &self,
        req: &dyn GetWatchInfo,
        ctx: Option<Context>,
    ) -> Result<Watches, ReplyError>;
    /// Destroy a watch
    async fn destroy(&self, req: &dyn WatchInfo, ctx: Option<Context>) -> Result<(), ReplyError>;
}

/// WatchInfo trait for the watch creation to be implemented by entities which want to avail
/// this operation
pub trait WatchInfo: Send + Sync + std::fmt::Debug {
    /// id of the resource to watch on
    fn id(&self) -> WatchResourceId;
    /// callback used to notify the watch of a change
    fn callback(&self) -> WatchCallback;
    /// type of watch
    fn watch_type(&self) -> WatchType;
}

/// GetWatchInfo trait for the get watch operation to be implemented by entities which want to
/// avail this operation
pub trait GetWatchInfo: Send + Sync + std::fmt::Debug {
    /// id of the resource to get
    fn resource_id(&self) -> WatchResourceId;
}

impl From<WatchResourceId> for watch::WatchResourceId {
    fn from(resource_id: WatchResourceId) -> Self {
        match resource_id {
            WatchResourceId::Node(node_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::NodeId(node_id.to_string())),
            },
            WatchResourceId::Pool(pool_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::PoolId(pool_id.to_string())),
            },
            WatchResourceId::Replica(replica_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::ReplicaId(
                    replica_id.to_string(),
                )),
            },
            WatchResourceId::ReplicaState(replica_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::ReplicaStateId(
                    replica_id.to_string(),
                )),
            },
            WatchResourceId::ReplicaSpec(replica_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::ReplicaSpecId(
                    replica_id.to_string(),
                )),
            },
            WatchResourceId::Nexus(nexus_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::NexusId(nexus_id.to_string())),
            },
            WatchResourceId::Volume(volume_id) => Self {
                resource_id: Some(watch_resource_id::ResourceId::VolumeId(
                    volume_id.to_string(),
                )),
            },
        }
    }
}

impl TryFrom<watch::WatchResourceId> for WatchResourceId {
    type Error = ReplyError;

    fn try_from(value: watch::WatchResourceId) -> Result<Self, Self::Error> {
        match value.resource_id {
            Some(resource_id) => Ok(match resource_id {
                watch_resource_id::ResourceId::NodeId(id) => WatchResourceId::Node(id.into()),
                watch_resource_id::ResourceId::PoolId(id) => WatchResourceId::Pool(id.into()),
                watch_resource_id::ResourceId::ReplicaId(id) => {
                    WatchResourceId::Replica(ReplicaId::try_from(StringValue(Some(id)))?)
                }
                watch_resource_id::ResourceId::ReplicaStateId(id) => {
                    WatchResourceId::ReplicaSpec(ReplicaId::try_from(StringValue(Some(id)))?)
                }
                watch_resource_id::ResourceId::ReplicaSpecId(id) => {
                    WatchResourceId::ReplicaState(ReplicaId::try_from(StringValue(Some(id)))?)
                }
                watch_resource_id::ResourceId::NexusId(id) => {
                    WatchResourceId::Nexus(NexusId::try_from(StringValue(Some(id)))?)
                }
                watch_resource_id::ResourceId::VolumeId(id) => {
                    WatchResourceId::Volume(VolumeId::try_from(StringValue(Some(id)))?)
                }
            }),
            None => Err(ReplyError::invalid_argument(
                ResourceKind::Watch,
                "watch_resource_id",
                "".to_string(),
            )),
        }
    }
}

impl From<WatchCallback> for watch::WatchCallback {
    fn from(value: WatchCallback) -> Self {
        match value {
            WatchCallback::Uri(uri) => Self {
                callback: Some(watch::watch_callback::Callback::Uri(watch::Uri {
                    content: uri,
                })),
            },
        }
    }
}

impl TryFrom<watch::WatchCallback> for WatchCallback {
    type Error = ReplyError;

    fn try_from(value: watch::WatchCallback) -> Result<Self, Self::Error> {
        match value.callback {
            Some(watch_callback) => match watch_callback {
                watch::watch_callback::Callback::Uri(uri) => Ok(Self::Uri(uri.content)),
            },
            None => Err(ReplyError::invalid_argument(
                ResourceKind::Watch,
                "watch_callback",
                "".to_string(),
            )),
        }
    }
}

impl From<WatchType> for watch::WatchType {
    fn from(value: WatchType) -> Self {
        match value {
            WatchType::Desired => Self::Desired,
            WatchType::Actual => Self::Actual,
            WatchType::All => Self::All,
        }
    }
}

impl From<watch::WatchType> for WatchType {
    fn from(value: watch::WatchType) -> Self {
        match value {
            watch::WatchType::Desired => Self::Desired,
            watch::WatchType::Actual => Self::Actual,
            watch::WatchType::All => Self::All,
        }
    }
}

impl From<Watch> for watch::Watch {
    fn from(value: Watch) -> Self {
        let watch_type: watch::WatchType = value.watch_type.into();
        Self {
            id: Some(value.id.into()),
            callback: Some(value.callback.into()),
            watch_type: watch_type as i32,
        }
    }
}

impl TryFrom<watch::Watch> for Watch {
    type Error = ReplyError;

    fn try_from(value: watch::Watch) -> Result<Self, Self::Error> {
        Ok(Self {
            id: match value.id {
                Some(id) => WatchResourceId::try_from(id)?,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Watch,
                        "watch_resource_id",
                        "".to_string(),
                    ))
                }
            },
            callback: match value.callback {
                Some(callback) => WatchCallback::try_from(callback)?,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Watch,
                        "watch_callback",
                        "".to_string(),
                    ))
                }
            },
            watch_type: watch::WatchType::from_i32(value.watch_type)
                .ok_or_else(|| {
                    ReplyError::invalid_argument(ResourceKind::Watch, "watch_type", "".to_string())
                })?
                .into(),
        })
    }
}

impl TryFrom<watch::Watches> for Watches {
    type Error = ReplyError;
    fn try_from(grpc_watches_type: watch::Watches) -> Result<Self, Self::Error> {
        let mut watches: Vec<Watch> = vec![];
        for watch in grpc_watches_type.watches {
            watches.push(Watch::try_from(watch.clone())?)
        }
        Ok(Watches(watches))
    }
}

impl From<Watches> for watch::Watches {
    fn from(watches: Watches) -> Self {
        watch::Watches {
            watches: watches
                .into_inner()
                .iter()
                .map(|watch| watch.clone().into())
                .collect(),
        }
    }
}

impl WatchInfo for CreateWatch {
    fn id(&self) -> WatchResourceId {
        self.id.clone()
    }

    fn callback(&self) -> WatchCallback {
        self.callback.clone()
    }

    fn watch_type(&self) -> WatchType {
        self.watch_type.clone()
    }
}

impl WatchInfo for DeleteWatch {
    fn id(&self) -> WatchResourceId {
        self.id.clone()
    }

    fn callback(&self) -> WatchCallback {
        self.callback.clone()
    }

    fn watch_type(&self) -> WatchType {
        self.watch_type.clone()
    }
}

/// Intermediate structure that validates the conversion to grpc Watch type
#[derive(Debug)]
pub struct ValidatedWatchRequest {
    id: WatchResourceId,
    callback: WatchCallback,
    watch_type: WatchType,
}

impl WatchInfo for ValidatedWatchRequest {
    fn id(&self) -> WatchResourceId {
        self.id.clone()
    }

    fn callback(&self) -> WatchCallback {
        self.callback.clone()
    }

    fn watch_type(&self) -> WatchType {
        self.watch_type.clone()
    }
}

impl ValidateRequestTypes for watch::Watch {
    type Validated = ValidatedWatchRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedWatchRequest {
            id: WatchResourceId::try_from(match self.id {
                Some(id) => id,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Watch,
                        "watch_resource_id",
                        "".to_string(),
                    ))
                }
            })?,
            callback: WatchCallback::try_from(match self.callback {
                Some(callback) => callback,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Watch,
                        "watch_callback",
                        "".to_string(),
                    ))
                }
            })?,
            watch_type: match watch::WatchType::from_i32(self.watch_type) {
                Some(watch_type) => watch_type.into(),
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Watch,
                        "watch_type",
                        "".to_string(),
                    ))
                }
            },
        })
    }
}

impl From<&dyn WatchInfo> for Watch {
    fn from(data: &dyn WatchInfo) -> Self {
        Self {
            id: data.id(),
            callback: data.callback(),
            watch_type: data.watch_type(),
        }
    }
}

impl From<&dyn WatchInfo> for DeleteWatch {
    fn from(data: &dyn WatchInfo) -> Self {
        Self {
            id: data.id(),
            callback: data.callback(),
            watch_type: data.watch_type(),
        }
    }
}

impl From<&dyn WatchInfo> for watch::Watch {
    fn from(data: &dyn WatchInfo) -> Self {
        let watch_type: watch::WatchType = data.watch_type().into();
        Self {
            id: Some(data.id().into()),
            callback: Some(data.callback().into()),
            watch_type: watch_type as i32,
        }
    }
}

impl GetWatchInfo for GetWatches {
    fn resource_id(&self) -> WatchResourceId {
        self.resource.clone()
    }
}

/// Intermediate structure that validates the conversion to GetWatchesRequest type
#[derive(Debug)]
pub struct ValidatedGetWatchesRequest {
    resource: WatchResourceId,
}

impl GetWatchInfo for ValidatedGetWatchesRequest {
    fn resource_id(&self) -> WatchResourceId {
        self.resource.clone()
    }
}

impl ValidateRequestTypes for watch::GetWatchesRequest {
    type Validated = ValidatedGetWatchesRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedGetWatchesRequest {
            resource: WatchResourceId::try_from(match self.resource {
                Some(id) => id,
                None => {
                    return Err(ReplyError::invalid_argument(
                        ResourceKind::Watch,
                        "watch_resource_id",
                        "".to_string(),
                    ))
                }
            })?,
        })
    }
}

impl From<&dyn GetWatchInfo> for GetWatches {
    fn from(data: &dyn GetWatchInfo) -> Self {
        Self {
            resource: data.resource_id(),
        }
    }
}

impl From<&dyn GetWatchInfo> for watch::GetWatchesRequest {
    fn from(data: &dyn GetWatchInfo) -> Self {
        Self {
            resource: Some(data.resource_id().into()),
        }
    }
}
