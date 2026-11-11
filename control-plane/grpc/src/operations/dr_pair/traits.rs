use crate::{context::Context, dr_pair};
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{
        CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrClusterPairs, DrPairId,
        DrS3Transport, DrTransport, GetDrClusterPair,
    },
};

/// All dr cluster pair operations.
///
/// Scaffolding for the operator to reconcile onto: the pair as a resource, and nothing
/// about what the agent then does with it.
#[tonic::async_trait]
pub trait DrPairOperations: Send + Sync {
    /// Create a pair, or adopt one that already exists with the same terms.
    async fn create(
        &self,
        req: &CreateDrClusterPair,
        ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError>;
    /// Fetch a single pair.
    async fn get(
        &self,
        req: &GetDrClusterPair,
        ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError>;
    /// List the pairs.
    async fn list(&self, ctx: Option<Context>) -> Result<DrClusterPairs, ReplyError>;
    /// Remove a pair.
    async fn destroy(
        &self,
        req: &DestroyDrClusterPair,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError>;
}

// ---- transport -> grpc -------------------------------------------------------------------

impl From<DrS3Transport> for dr_pair::DrS3Transport {
    fn from(src: DrS3Transport) -> Self {
        Self {
            endpoint: src.endpoint,
            bucket: src.bucket,
            prefix: src.prefix,
            region: src.region,
            access_key: src.access_key,
            secret_key: src.secret_key,
        }
    }
}

impl From<DrTransport> for dr_pair::DrTransport {
    fn from(src: DrTransport) -> Self {
        let DrTransport::S3(s3) = src;
        Self {
            transport: Some(dr_pair::dr_transport::Transport::S3(s3.into())),
        }
    }
}

impl From<DrClusterPair> for dr_pair::DrClusterPair {
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

impl From<dr_pair::DrS3Transport> for DrS3Transport {
    fn from(src: dr_pair::DrS3Transport) -> Self {
        Self {
            endpoint: src.endpoint,
            bucket: src.bucket,
            prefix: src.prefix,
            region: src.region,
            access_key: src.access_key,
            secret_key: src.secret_key,
        }
    }
}

impl TryFrom<dr_pair::DrTransport> for DrTransport {
    type Error = ReplyError;
    fn try_from(src: dr_pair::DrTransport) -> Result<Self, Self::Error> {
        match src.transport {
            Some(dr_pair::dr_transport::Transport::S3(s3)) => Ok(DrTransport::S3(s3.into())),
            // A transport with no variant set is a caller that did not say where the store
            // is, which there is no sensible default for.
            None => Err(ReplyError::missing_argument(
                ResourceKind::DrClusterPair,
                "transport",
            )),
        }
    }
}

impl TryFrom<dr_pair::DrClusterPair> for DrClusterPair {
    type Error = ReplyError;
    fn try_from(src: dr_pair::DrClusterPair) -> Result<Self, Self::Error> {
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

impl TryFrom<dr_pair::DrClusterPairs> for DrClusterPairs {
    type Error = ReplyError;
    fn try_from(src: dr_pair::DrClusterPairs) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: src
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, ReplyError>>()?,
            next_token: src.next_token,
        })
    }
}

impl From<&CreateDrClusterPair> for dr_pair::CreateDrClusterPairRequest {
    fn from(src: &CreateDrClusterPair) -> Self {
        Self {
            id: src.id.to_string(),
            cluster: src.cluster.to_string(),
            peer: src.peer.to_string(),
            transport: Some(src.transport.clone().into()),
        }
    }
}

impl TryFrom<dr_pair::CreateDrClusterPairRequest> for CreateDrClusterPair {
    type Error = ReplyError;
    fn try_from(src: dr_pair::CreateDrClusterPairRequest) -> Result<Self, Self::Error> {
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

/// The pair id carried by a request.
pub trait DrPairInfo: Send + Sync {
    /// The pair being addressed.
    fn id(&self) -> DrPairId;
}

impl DrPairInfo for GetDrClusterPair {
    fn id(&self) -> DrPairId {
        self.id.clone()
    }
}

impl DrPairInfo for DestroyDrClusterPair {
    fn id(&self) -> DrPairId {
        self.id.clone()
    }
}
