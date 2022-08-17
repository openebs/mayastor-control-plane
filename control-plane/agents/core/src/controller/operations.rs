use crate::controller::registry::Registry;
use common::errors::SvcError;

/// Resource Lifecycle Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceLifecycle {
    type Create: Sync + Send;
    type CreateOutput: Sync + Send + Sized;
    type Destroy: Sync + Send;
    /// Create the `Self` Resource itself.
    async fn create(
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError>;
    /// Destroy the resource itself.
    async fn destroy(&self, registry: &Registry, request: &Self::Destroy) -> Result<(), SvcError>;
}

/// Resource Sharing Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceSharing {
    type Share: Sync + Send;
    type ShareOutput: Sync + Send;
    type Unshare: Sync + Send;
    type UnshareOutput: Sync + Send;

    /// Share the resource.
    async fn share(
        &self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<Self::ShareOutput, SvcError>;
    /// Unshare the resource.
    async fn unshare(
        &self,
        registry: &Registry,
        request: &Self::Unshare,
    ) -> Result<Self::UnshareOutput, SvcError>;
}

/// Resource Publishing Operations.
#[async_trait::async_trait]
pub(crate) trait ResourcePublishing {
    type Publish: Sync + Send;
    type PublishOutput: Sync + Send;
    type Unpublish: Sync + Send;

    /// Publish the resource.
    async fn publish(
        &self,
        registry: &Registry,
        request: &Self::Publish,
    ) -> Result<Self::PublishOutput, SvcError>;
    /// Unpublish the resource.
    async fn unpublish(
        &self,
        registry: &Registry,
        request: &Self::Unpublish,
    ) -> Result<(), SvcError>;
}

/// Resource Replica Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceReplicas {
    type Request: Sync + Send;

    /// Set the resource's replica count.
    async fn set_replica(
        &self,
        registry: &Registry,
        request: &Self::Request,
    ) -> Result<(), SvcError>;
}

/// Resource Children/Offspring Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceOffspring {
    type Add: Sync + Send;
    type AddOutput: Sync + Send;
    type Remove: Sync + Send;

    /// Add a child to the resource.
    async fn add_child(
        &self,
        registry: &Registry,
        request: &Self::Add,
    ) -> Result<Self::AddOutput, SvcError>;
    /// Remove a child from the resource.
    async fn remove_child(
        &self,
        registry: &Registry,
        request: &Self::Remove,
    ) -> Result<(), SvcError>;
}
