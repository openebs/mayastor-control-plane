use crate::controller::registry::Registry;
use agents::errors::SvcError;
use grpc::{context::Context, operations::dr_pair::traits::DrPairOperations};
use stor_port::{
    transport_api::ReplyError,
    types::v0::{
        store::dr_pair::DrClusterPairSpec,
        transport::{
            CreateDrClusterPair, DestroyDrClusterPair, DrClusterPair, DrClusterPairs,
            GetDrClusterPair,
        },
    },
};

/// Dr cluster pair service.
#[derive(Debug, Clone)]
pub(crate) struct Service {
    registry: Registry,
}

/// The stored spec, as the transport sees it.
fn as_transport(spec: DrClusterPairSpec) -> DrClusterPair {
    DrClusterPair {
        id: spec.id,
        cluster: spec.cluster,
        cluster_uid: spec.cluster_uid,
        peer: spec.peer,
        transport: spec.transport,
    }
}

impl Service {
    /// Creates a new dr cluster pair service.
    pub(crate) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    async fn create(&self, req: &CreateDrClusterPair) -> Result<DrClusterPair, SvcError> {
        // A pair whose two ends carry the same name cannot be told apart in the store, so
        // every key this side wrote would also be the key it expects its peer to have
        // written.
        if req.cluster == req.peer {
            return Err(SvcError::InvalidDrClusterPair {
                error: format!(
                    "a pair's cluster and peer names must differ, both are '{}'",
                    req.cluster
                ),
            });
        }
        let spec = self
            .registry
            .specs()
            .create_dr_pair(&self.registry, req)
            .await?;
        Ok(as_transport(spec))
    }

    async fn get(&self, req: &GetDrClusterPair) -> Result<DrClusterPair, SvcError> {
        self.registry
            .specs()
            .dr_pair_spec(&req.id)
            .map(as_transport)
            .ok_or_else(|| SvcError::DrClusterPairNotFound {
                id: req.id.to_string(),
            })
    }

    async fn list(&self) -> Result<DrClusterPairs, SvcError> {
        Ok(DrClusterPairs {
            entries: self
                .registry
                .specs()
                .dr_pair_specs()
                .into_iter()
                .map(as_transport)
                .collect(),
            next_token: None,
        })
    }

    async fn destroy(&self, req: &DestroyDrClusterPair) -> Result<(), SvcError> {
        self.registry
            .specs()
            .destroy_dr_pair(&self.registry, &req.id)
            .await
    }
}

#[tonic::async_trait]
impl DrPairOperations for Service {
    async fn create(
        &self,
        req: &CreateDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        Ok(self.create(req).await?)
    }

    async fn get(
        &self,
        req: &GetDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<DrClusterPair, ReplyError> {
        Ok(self.get(req).await?)
    }

    async fn list(&self, _ctx: Option<Context>) -> Result<DrClusterPairs, ReplyError> {
        Ok(self.list().await?)
    }

    async fn destroy(
        &self,
        req: &DestroyDrClusterPair,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        Ok(self.destroy(req).await?)
    }
}
