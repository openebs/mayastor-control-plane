mod host;
mod nexus;
mod pool;
mod replica;
mod snap_rebuild;
mod translation;

use crate::controller::io_engine::{ApiVersion, GrpcContext};
use agents::errors::{GrpcConnect, SvcError};

use snafu::ResultExt;
use tonic::transport::Channel;

/// V1 HostClient.
type HostClient = rpc::v1::host::host_rpc_client::HostRpcClient<Channel>;
/// V1 ReplicaClient.
type ReplicaClient = rpc::v1::replica::replica_rpc_client::ReplicaRpcClient<Channel>;
/// V1 NexusClient.
type NexusClient = rpc::v1::nexus::nexus_rpc_client::NexusRpcClient<Channel>;
/// The V1 PoolClient.
type PoolClient = rpc::v1::pool::pool_rpc_client::PoolRpcClient<Channel>;
/// The V1 SnapshotClient.
type SnapshotClient = rpc::v1::snapshot::snapshot_rpc_client::SnapshotRpcClient<Channel>;
/// The V1 SnapshotRebuildClient.
type SnapshotRebuildClient =
    rpc::v1::snapshot_rebuild::snapshot_rebuild_rpc_client::SnapshotRebuildRpcClient<Channel>;

/// A collection of all clients for the Io-Engine V1 services.
#[derive(Clone, Debug)]
pub(crate) struct RpcClient {
    host: HostClient,
    replica: ReplicaClient,
    nexus: NexusClient,
    pool: PoolClient,
    snapshot: SnapshotClient,
    snapshot_rebuild: SnapshotRebuildClient,
    context: GrpcContext,
}

impl RpcClient {
    /// Create a new grpc client with a context.
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        let channel = context
            .tonic_endpoint()
            .connect()
            .await
            .context(GrpcConnect {
                node_id: context.node().to_owned(),
                endpoint: context.endpoint().to_string(),
            })?;

        Ok(Self {
            host: HostClient::new(channel.clone()),
            replica: ReplicaClient::new(channel.clone()),
            nexus: NexusClient::new(channel.clone()),
            pool: PoolClient::new(channel.clone()),
            snapshot: SnapshotClient::new(channel.clone()),
            snapshot_rebuild: SnapshotRebuildClient::new(channel),
            context: context.clone(),
        })
    }
    /// Get the v1 replica client.
    fn replica(&self) -> ReplicaClient {
        self.replica.clone()
    }
    /// Get the v1 nexus client.
    fn nexus(&self) -> NexusClient {
        self.nexus.clone()
    }
    /// Get the v1 host client.
    fn host(&self) -> HostClient {
        self.host.clone()
    }
    /// Get the v1 pool client.
    fn pool(&self) -> PoolClient {
        self.pool.clone()
    }
    /// Get the v1 snapshot client.
    fn snapshot(&self) -> SnapshotClient {
        self.snapshot.clone()
    }
    /// Get the v1 snapshot rebuild client.
    fn snap_rebuild(&self) -> SnapshotRebuildClient {
        self.snapshot_rebuild.clone()
    }

    async fn fetcher_client(&self) -> Result<Self, SvcError> {
        let mut context = self.context.clone();
        context.override_timeout(None);
        Self::new(&context).await
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::NodeApi for RpcClient {
    fn api_version(&self) -> ApiVersion {
        ApiVersion::V1
    }
}
