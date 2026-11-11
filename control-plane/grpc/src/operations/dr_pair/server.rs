use crate::{
    dr_pair,
    dr_pair::{dr_cluster_pair_reply, dr_cluster_pairs_reply, dr_pair_grpc_server},
    operations::dr_pair::traits::DrPairOperations,
};

use std::sync::Arc;
use stor_port::types::v0::transport::{DestroyDrClusterPair, GetDrClusterPair};
use tonic::Response;

/// RPC Dr cluster pair Server
#[derive(Clone)]
pub struct DrPairServer {
    /// Service which executes the operations.
    service: Arc<dyn DrPairOperations>,
}

impl DrPairServer {
    /// Returns a new dr pair server with the service implementing the operations.
    pub fn new(service: Arc<dyn DrPairOperations>) -> Self {
        Self { service }
    }
    /// Converts the server into its corresponding grpc server type.
    pub fn into_grpc_server(self) -> dr_pair_grpc_server::DrPairGrpcServer<DrPairServer> {
        dr_pair_grpc_server::DrPairGrpcServer::new(self)
    }
}

/// A single-pair reply, carrying either the pair or why there is none.
///
/// Errors travel in the payload rather than as a `tonic::Status`, so the kind and resource
/// survive the hop instead of being flattened into a string.
fn pair_reply(
    result: Result<
        stor_port::types::v0::transport::DrClusterPair,
        stor_port::transport_api::ReplyError,
    >,
) -> dr_pair::DrClusterPairReply {
    dr_pair::DrClusterPairReply {
        reply: Some(match result {
            Ok(pair) => dr_cluster_pair_reply::Reply::Pair(pair.into()),
            Err(error) => dr_cluster_pair_reply::Reply::Error(error.into()),
        }),
    }
}

#[tonic::async_trait]
impl dr_pair_grpc_server::DrPairGrpc for DrPairServer {
    async fn create_dr_cluster_pair(
        &self,
        request: tonic::Request<dr_pair::CreateDrClusterPairRequest>,
    ) -> Result<tonic::Response<dr_pair::DrClusterPairReply>, tonic::Status> {
        let result = match request.into_inner().try_into() {
            Ok(req) => self.service.create(&req, None).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(pair_reply(result)))
    }

    async fn get_dr_cluster_pair(
        &self,
        request: tonic::Request<dr_pair::GetDrClusterPairRequest>,
    ) -> Result<tonic::Response<dr_pair::DrClusterPairReply>, tonic::Status> {
        let req = GetDrClusterPair {
            id: request.into_inner().id.into(),
        };
        Ok(Response::new(pair_reply(
            self.service.get(&req, None).await,
        )))
    }

    async fn list_dr_cluster_pairs(
        &self,
        _request: tonic::Request<dr_pair::ListDrClusterPairsRequest>,
    ) -> Result<tonic::Response<dr_pair::DrClusterPairsReply>, tonic::Status> {
        let reply = match self.service.list(None).await {
            Ok(pairs) => dr_cluster_pairs_reply::Reply::Pairs(dr_pair::DrClusterPairs {
                entries: pairs.entries.into_iter().map(Into::into).collect(),
                next_token: pairs.next_token,
            }),
            Err(error) => dr_cluster_pairs_reply::Reply::Error(error.into()),
        };
        Ok(Response::new(dr_pair::DrClusterPairsReply {
            reply: Some(reply),
        }))
    }

    async fn destroy_dr_cluster_pair(
        &self,
        request: tonic::Request<dr_pair::DestroyDrClusterPairRequest>,
    ) -> Result<tonic::Response<dr_pair::DestroyDrClusterPairReply>, tonic::Status> {
        let req = DestroyDrClusterPair {
            id: request.into_inner().id.into(),
        };
        Ok(Response::new(dr_pair::DestroyDrClusterPairReply {
            error: self.service.destroy(&req, None).await.err().map(Into::into),
        }))
    }
}
