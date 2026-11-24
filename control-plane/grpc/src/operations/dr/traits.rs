use crate::{context::Context, dr};
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{
        CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrClusterPairs, DrS3Credentials,
        DrS3Keys, DrS3Transport, DrTransport, GetDrClusterPair,
    },
};

/// All dr agent operations.
///
/// One trait for one agent: the liveness probe every service here has, and the cluster
/// pairs it holds. The operations that manage relationships and failover join them as
/// they are built.
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
