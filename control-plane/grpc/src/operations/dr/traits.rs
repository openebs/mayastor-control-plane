use crate::{common, context::Context, dr};
use std::time::Duration;
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{
        CreateDrClusterPair, DestroyDrClusterPair, DisableDrVolume, DrClusterPair, DrClusterPairs,
        DrId, DrPolicy, DrRole, DrS3Credentials, DrS3Keys, DrS3Transport, DrTransport, DrVolume,
        DrVolumes, EnableDrVolume, GetDrClusterPair, GetDrVolume, VolumeId,
    },
};

/// All dr agent operations.
///
/// One trait for one agent: the liveness probe every service here has, the cluster pairs
/// it holds, and the volumes replicated over them. Failover joins them as it is built.
#[tonic::async_trait]
pub trait DrOperations: Send + Sync {
    /// Liveness probe for the dr service.
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
    /// Create a pair, or adopt one that already exists with the same terms.
    async fn create_pair(
        &self,
        req: &CreateDrClusterPair,
        ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError>;
    /// Fetch a single pair.
    async fn get_pair(
        &self,
        req: &GetDrClusterPair,
        ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError>;
    /// List the pairs.
    async fn list_pairs(&self, ctx: Option<Context>) -> Result<DrClusterPairs, ReplyError>;
    /// Remove a pair.
    async fn destroy_pair(
        &self,
        req: &DestroyDrClusterPair,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
    /// Enable replication on a volume, or amend the policy of one already enabled.
    async fn enable_volume(
        &self,
        req: &EnableDrVolume,
        ctx: Option<Context>,
    ) -> Result<DrVolume, ReplyError>;
    /// Fetch a single volume's entry.
    async fn get_volume(
        &self,
        req: &GetDrVolume,
        ctx: Option<Context>,
    ) -> Result<DrVolume, ReplyError>;
    /// List every volume's entry.
    async fn list_volumes(&self, ctx: Option<Context>) -> Result<DrVolumes, ReplyError>;
    /// Disable replication on a volume.
    async fn disable_volume(
        &self,
        req: &DisableDrVolume,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
}

// ---- transport -> grpc -------------------------------------------------------------------

impl From<DrS3Transport> for dr::S3Transport {
    fn from(src: DrS3Transport) -> Self {
        Self {
            endpoint: src.endpoint,
            bucket: src.bucket,
            prefix: src.prefix,
            region: src.region,
            credentials: src.credentials.map(Into::into),
        }
    }
}

impl From<DrS3Credentials> for dr::S3Credentials {
    fn from(src: DrS3Credentials) -> Self {
        Self {
            credentials: Some(match src {
                DrS3Credentials::Keys(keys) => dr::s3_credentials::Credentials::Keys(dr::S3Keys {
                    access_key: keys.access_key,
                    secret_key: keys.secret_key,
                }),
                DrS3Credentials::Named(name) => {
                    dr::s3_credentials::Credentials::Named(dr::NamedCredentials { name })
                }
            }),
        }
    }
}

impl From<DrTransport> for dr::Transport {
    fn from(src: DrTransport) -> Self {
        let DrTransport::S3(s3) = src;
        Self {
            transport: Some(dr::transport::Transport::S3(s3.into())),
        }
    }
}

impl From<DrClusterPair> for dr::ClusterPair {
    fn from(src: DrClusterPair) -> Self {
        Self {
            id: src.id.to_string(),
            cluster: src.cluster.to_string(),
            cluster_uid: src.cluster_uid.to_string(),
            peer: src.peer.to_string(),
            transport: Some(src.transport.into()),
        }
    }
}

// ---- grpc -> transport -------------------------------------------------------------------

impl From<dr::S3Transport> for DrS3Transport {
    fn from(src: dr::S3Transport) -> Self {
        Self {
            endpoint: src.endpoint,
            bucket: src.bucket,
            prefix: src.prefix,
            region: src.region,
            credentials: src.credentials.and_then(credentials),
        }
    }
}

/// A credentials envelope with no variant set names no way to authenticate, which is the
/// same as not having said anything: the agent's own environment.
fn credentials(src: dr::S3Credentials) -> Option<DrS3Credentials> {
    match src.credentials? {
        dr::s3_credentials::Credentials::Keys(keys) => Some(DrS3Credentials::Keys(DrS3Keys {
            access_key: keys.access_key,
            secret_key: keys.secret_key,
        })),
        dr::s3_credentials::Credentials::Named(named) => Some(DrS3Credentials::Named(named.name)),
    }
}

impl TryFrom<dr::Transport> for DrTransport {
    type Error = ReplyError;
    fn try_from(src: dr::Transport) -> Result<Self, Self::Error> {
        match src.transport {
            Some(dr::transport::Transport::S3(s3)) => Ok(DrTransport::S3(s3.into())),
            // A transport with no variant set is a caller that did not say where the store
            // is, which there is no sensible default for.
            None => Err(ReplyError::missing_argument(
                ResourceKind::DrClusterPair,
                "transport",
            )),
        }
    }
}

impl TryFrom<dr::ClusterPair> for DrClusterPair {
    type Error = ReplyError;
    fn try_from(src: dr::ClusterPair) -> Result<Self, Self::Error> {
        Ok(Self {
            id: src.id.into(),
            cluster: src.cluster.into(),
            cluster_uid: src.cluster_uid.into(),
            peer: src.peer.into(),
            transport: src
                .transport
                .ok_or_else(|| {
                    ReplyError::missing_argument(ResourceKind::DrClusterPair, "transport")
                })?
                .try_into()?,
        })
    }
}

impl TryFrom<dr::ClusterPairs> for DrClusterPairs {
    type Error = ReplyError;
    fn try_from(src: dr::ClusterPairs) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: src
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, ReplyError>>()?,
        })
    }
}

impl From<&CreateDrClusterPair> for dr::CreateClusterPairRequest {
    fn from(src: &CreateDrClusterPair) -> Self {
        Self {
            id: src.id.to_string(),
            cluster: src.cluster.to_string(),
            peer: src.peer.to_string(),
            transport: Some(src.transport.clone().into()),
        }
    }
}

impl TryFrom<dr::CreateClusterPairRequest> for CreateDrClusterPair {
    type Error = ReplyError;
    fn try_from(src: dr::CreateClusterPairRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: src.id.into(),
            cluster: src.cluster.into(),
            peer: src.peer.into(),
            transport: src
                .transport
                .ok_or_else(|| {
                    ReplyError::missing_argument(ResourceKind::DrClusterPair, "transport")
                })?
                .try_into()?,
        })
    }
}

// ---- dr volumes --------------------------------------------------------------------------

/// A volume uuid off the wire.
pub fn volume_id(field: &str, src: &str) -> Result<VolumeId, ReplyError> {
    VolumeId::try_from(src).map_err(|error| {
        ReplyError::invalid_argument(ResourceKind::DrVolume, field, error.to_string())
    })
}

/// A relationship's uuid off the wire.
fn dr_id(src: &str) -> Result<DrId, ReplyError> {
    DrId::try_from(src).map_err(|error| {
        ReplyError::invalid_argument(ResourceKind::DrVolume, "dr", error.to_string())
    })
}

/// A duration on the wire, saturating rather than wrapping: neither end has a use for
/// one past `i64::MAX` seconds.
fn duration(src: Duration) -> prost_types::Duration {
    prost_types::Duration::try_from(src).unwrap_or(prost_types::Duration {
        seconds: i64::MAX,
        nanos: 0,
    })
}

/// A duration off the wire, which has to be stated and cannot be negative.
fn duration_from(field: &str, src: Option<prost_types::Duration>) -> Result<Duration, ReplyError> {
    let src = src.ok_or_else(|| ReplyError::missing_argument(ResourceKind::DrVolume, field))?;
    Duration::try_from(src).map_err(|error| {
        ReplyError::invalid_argument(ResourceKind::DrVolume, field, error.to_string())
    })
}

impl From<DrRole> for dr::Role {
    fn from(src: DrRole) -> Self {
        match src {
            DrRole::Source => Self::Source,
            DrRole::Destination => Self::Destination,
        }
    }
}

impl From<dr::Role> for DrRole {
    fn from(src: dr::Role) -> Self {
        match src {
            dr::Role::Source => Self::Source,
            dr::Role::Destination => Self::Destination,
        }
    }
}

/// A role off the wire, where an unknown value is a newer peer's role this side cannot
/// act on.
fn role(src: i32) -> Result<DrRole, ReplyError> {
    dr::Role::try_from(src).map(Into::into).map_err(|_| {
        ReplyError::invalid_argument(
            ResourceKind::DrVolume,
            "role",
            format!("unknown role {src}"),
        )
    })
}

impl From<DrPolicy> for dr::Policy {
    fn from(src: DrPolicy) -> Self {
        Self {
            auto_resync: src.auto_resync,
            schedule: Some(duration(src.schedule)),
            ttl: Some(duration(src.ttl)),
            max_lag: src.max_lag.map(duration),
        }
    }
}

impl TryFrom<dr::Policy> for DrPolicy {
    type Error = ReplyError;
    fn try_from(src: dr::Policy) -> Result<Self, Self::Error> {
        Ok(Self {
            auto_resync: src.auto_resync,
            schedule: duration_from("policy.schedule", src.schedule)?,
            ttl: duration_from("policy.ttl", src.ttl)?,
            max_lag: src
                .max_lag
                .map(|lag| duration_from("policy.max_lag", Some(lag)))
                .transpose()?,
        })
    }
}

/// A policy off the wire, which has to be stated: there is no schedule to default to.
fn policy(src: Option<dr::Policy>) -> Result<DrPolicy, ReplyError> {
    src.ok_or_else(|| ReplyError::missing_argument(ResourceKind::DrVolume, "policy"))?
        .try_into()
}

impl From<DrVolume> for dr::Volume {
    fn from(src: DrVolume) -> Self {
        Self {
            volume: src.volume.to_string(),
            dr: src.dr.to_string(),
            pair: src.pair.to_string(),
            source_volume: src.source_volume.to_string(),
            target_volume: src.target_volume.map(|target| target.to_string()),
            role: dr::Role::from(src.role) as i32,
            policy: Some(src.policy.into()),
            status: common::SpecStatus::from(&src.status) as i32,
        }
    }
}

impl TryFrom<dr::Volume> for DrVolume {
    type Error = ReplyError;
    fn try_from(src: dr::Volume) -> Result<Self, Self::Error> {
        Ok(Self {
            volume: volume_id("volume", &src.volume)?,
            dr: dr_id(&src.dr)?,
            pair: src.pair.into(),
            source_volume: volume_id("source_volume", &src.source_volume)?,
            target_volume: src
                .target_volume
                .map(|target| volume_id("target_volume", &target))
                .transpose()?,
            role: role(src.role)?,
            policy: policy(src.policy)?,
            status: common::SpecStatus::try_from(src.status)
                .map_err(|_| {
                    ReplyError::invalid_argument(
                        ResourceKind::DrVolume,
                        "status",
                        format!("unknown status {}", src.status),
                    )
                })?
                .into(),
        })
    }
}

impl TryFrom<dr::Volumes> for DrVolumes {
    type Error = ReplyError;
    fn try_from(src: dr::Volumes) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: src
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, ReplyError>>()?,
        })
    }
}

impl From<&EnableDrVolume> for dr::EnableVolumeRequest {
    fn from(src: &EnableDrVolume) -> Self {
        Self {
            volume: src.volume.to_string(),
            pair: src.pair.to_string(),
            role: dr::Role::from(src.role) as i32,
            policy: Some(src.policy.into()),
        }
    }
}

impl TryFrom<dr::EnableVolumeRequest> for EnableDrVolume {
    type Error = ReplyError;
    fn try_from(src: dr::EnableVolumeRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            volume: volume_id("volume", &src.volume)?,
            pair: src.pair.into(),
            role: role(src.role)?,
            policy: policy(src.policy)?,
        })
    }
}
