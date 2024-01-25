use crate::controller::registry::Registry;
use agents::errors::SvcError;
use grpc::{
    context::Context,
    operations::{
        app_node::traits::{AppNodeInfo, AppNodeOperations, AppNodeRegisterInfo},
        Pagination,
    },
};
use stor_port::{
    transport_api::{v0::AppNodes, ReplyError},
    types::v0::transport::{AppNode, DeregisterAppNode, Filter, RegisterAppNode},
};

/// App node service.
#[derive(Debug, Clone)]
pub(crate) struct Service {
    registry: Registry,
}

impl Service {
    /// Creates a new app node service.
    pub(crate) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Registers an app node.
    async fn register(&self, registration: &RegisterAppNode) -> Result<(), SvcError> {
        self.registry.register_app_node(registration).await?;
        Ok(())
    }

    /// Deregisters an app node.
    async fn deregister(&self, deregistration: &DeregisterAppNode) -> Result<(), SvcError> {
        self.registry.deregister_app_node(deregistration).await?;
        Ok(())
    }

    /// Gets an app node.
    async fn get_app_node(&self, filter: Filter) -> Result<AppNode, SvcError> {
        match filter {
            Filter::AppNode(id) => {
                let app_node = self.registry.app_node(&id)?;
                Ok(app_node)
            }
            _ => Err(SvcError::InvalidFilter { filter }),
        }
    }

    /// Gets all app nodes.
    async fn list_app_nodes(&self, pagination: Option<Pagination>) -> Result<AppNodes, SvcError> {
        let mut last_result = true;
        let filtered_app_nodes = match pagination {
            None => self.registry.app_nodes(),
            Some(ref pagination) => {
                let paginated_app_nodes = self.registry.paginated_app_nodes(pagination);
                last_result = paginated_app_nodes.last();
                paginated_app_nodes.result()
            }
        };

        Ok(AppNodes {
            entries: filtered_app_nodes,
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
impl AppNodeOperations for Service {
    async fn get(&self, req: Filter, _ctx: Option<Context>) -> Result<AppNode, ReplyError> {
        let app_nodes = self.get_app_node(req).await?;
        Ok(app_nodes)
    }

    async fn list(
        &self,
        pagination: Option<Pagination>,
        _ctx: Option<Context>,
    ) -> Result<AppNodes, ReplyError> {
        let app_nodes = self.list_app_nodes(pagination).await?;
        Ok(app_nodes)
    }

    async fn register_app_node(
        &self,
        registration: &dyn AppNodeRegisterInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = registration.into();
        let service = self.clone();
        Context::spawn(async move { service.register(&req).await }).await??;
        Ok(())
    }

    async fn deregister_app_node(
        &self,
        deregistration: &dyn AppNodeInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = deregistration.into();
        let service = self.clone();
        Context::spawn(async move { service.deregister(&req).await }).await??;
        Ok(())
    }
}
