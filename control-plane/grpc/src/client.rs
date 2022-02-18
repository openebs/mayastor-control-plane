use crate::operations::{
    pool::{client::PoolClient, traits::PoolOperations},
    replica::{client::ReplicaClient, traits::ReplicaOperations},
    volume::{client::VolumeClient, traits::VolumeOperations},
};
use common_lib::mbus_api::TimeoutOptions;
use tonic::transport::Uri;

/// CoreClient encapsulates all the individual clients needed for gRPC transport
pub struct CoreClient {
    pool: PoolClient,
    replica: ReplicaClient,
    volume: VolumeClient,
}

/// implement the CoreClient
impl CoreClient {
    /// generates a new CoreClient to get the individual clients
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let timeout_opts = opts.into();
        let pool_client = PoolClient::new(addr.clone(), timeout_opts.clone()).await;
        let replica_client = ReplicaClient::new(addr.clone(), timeout_opts.clone()).await;
        let volume_client = VolumeClient::new(addr, timeout_opts).await;
        Self {
            pool: pool_client,
            replica: replica_client,
            volume: volume_client,
        }
    }
    /// retrieve the corresponding pool client
    pub fn pool(&self) -> impl PoolOperations {
        self.pool.clone()
    }
    /// retrieve the corresponding replica client
    pub fn replica(&self) -> impl ReplicaOperations {
        self.replica.clone()
    }
    /// retrieve the corresponding volume client
    pub fn volume(&self) -> impl VolumeOperations {
        self.volume.clone()
    }
}
