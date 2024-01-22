use crate::controller::registry::Registry;
use agents::errors::SvcError;
use grpc::{
    context::Context,
    operations::{
        frontend_node::traits::{
            FrontendNodeInfo, FrontendNodeOperations, FrontendNodeRegisterInfo,
        },
        Pagination,
    },
};
use stor_port::{
    transport_api::{v0::FrontendNodes, ReplyError},
    types::v0::transport::{DeregisterFrontendNode, Filter, FrontendNode, RegisterFrontendNode},
};

/// Frontend node service.
#[derive(Debug, Clone)]
pub(crate) struct Service {
    registry: Registry,
}

impl Service {
    /// Creates a new frontend node service.
    pub(crate) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Registers a frontend node.
    async fn register(&self, registration: &RegisterFrontendNode) -> Result<(), SvcError> {
        self.registry
            .register_frontend_node_spec(registration)
            .await?;
        Ok(())
    }

    /// Deregisters a frontend node.
    async fn deregister(&self, deregistration: &DeregisterFrontendNode) -> Result<(), SvcError> {
        self.registry
            .deregister_frontend_node_spec(deregistration)
            .await?;
        Ok(())
    }

    /// Gets a frontend node.
    async fn get_frontend_node(&self, filter: Filter) -> Result<FrontendNode, SvcError> {
        match filter {
            Filter::FrontendNode(id) => {
                let frontend_node = self.registry.get_frontend_node(&id)?;
                Ok(frontend_node)
            }
            _ => Err(SvcError::InvalidFilter { filter }),
        }
    }

    /// Gets all frontend nodes.
    async fn list_frontend_nodes(
        &self,
        filter: Filter,
        pagination: Option<Pagination>,
    ) -> Result<FrontendNodes, SvcError> {
        let mut last_result = true;
        let filtered_frontend_nodes = match filter {
            Filter::None => match pagination {
                None => self.registry.frontend_nodes(),
                Some(ref pagination) => {
                    let paginated_frontend_nodes =
                        self.registry.paginated_frontend_nodes(pagination);
                    last_result = paginated_frontend_nodes.last();
                    paginated_frontend_nodes.result()
                }
            },
            _ => {
                return Err(SvcError::InvalidFilter { filter });
            }
        };

        Ok(FrontendNodes {
            entries: filtered_frontend_nodes,
            next_token: match last_result {
                true => None,
                false => pagination
                    .clone()
                    .map(|p| p.starting_token() + p.max_entries()),
            },
        })
    }
}

#[tonic::async_trait]
impl FrontendNodeOperations for Service {
    async fn get(&self, req: Filter, _ctx: Option<Context>) -> Result<FrontendNode, ReplyError> {
        let frontend_nodes = self.get_frontend_node(req).await?;
        Ok(frontend_nodes)
    }

    async fn list(
        &self,
        req: Filter,
        pagination: Option<Pagination>,
        _ctx: Option<Context>,
    ) -> Result<FrontendNodes, ReplyError> {
        let frontend_nodes = self.list_frontend_nodes(req, pagination).await?;
        Ok(frontend_nodes)
    }

    async fn register_frontend_node(
        &self,
        registration: &dyn FrontendNodeRegisterInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = registration.into();
        let service = self.clone();
        Context::spawn(async move { service.register(&req).await }).await??;
        Ok(())
    }

    async fn deregister_frontend_node(
        &self,
        deregistration: &dyn FrontendNodeInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = deregistration.into();
        let service = self.clone();
        Context::spawn(async move { service.deregister(&req).await }).await??;
        Ok(())
    }
}
