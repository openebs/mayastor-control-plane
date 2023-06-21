use crate::controller::registry::Registry;
use agents::errors::SvcError;

/// Resource Cordon Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceCordon {
    type CordonOutput: Sync + Send + Sized;
    type UncordonOutput: Sync + Send + Sized;

    /// Cordon the resource.
    async fn cordon(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::CordonOutput, SvcError>;
    /// Uncordon the resource.
    async fn uncordon(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::UncordonOutput, SvcError>;
}

/// Resource Drain Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceDrain {
    type DrainOutput: Sync + Send + Sized;
    /// Drain the resource.
    async fn drain(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::DrainOutput, SvcError>;
    /// Mark the resource as drained.
    async fn set_drained(&mut self, registry: &Registry) -> Result<Self::DrainOutput, SvcError>;
}

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
    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError>;
}

/// Resource Lifecycle Operations for types with lifetimes.
#[async_trait::async_trait]
pub(crate) trait ResourceLifecycleWithLifetime {
    type Create<'a>: Sync + Send;
    type CreateOutput: Sync + Send + Sized;
    type Destroy: Sync + Send;
    /// Create the `Self` Resource itself.
    async fn create(
        registry: &Registry,
        request: &Self::Create<'_>,
    ) -> Result<Self::CreateOutput, SvcError>;
    /// Destroy the resource itself.
    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError>;
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
        &mut self,
        registry: &Registry,
        request: &Self::Share,
    ) -> Result<Self::ShareOutput, SvcError>;
    /// Unshare the resource.
    async fn unshare(
        &mut self,
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
    type Republish: Sync + Send;

    /// Publish the resource.
    async fn publish(
        &mut self,
        registry: &Registry,
        request: &Self::Publish,
    ) -> Result<Self::PublishOutput, SvcError>;
    /// Unpublish the resource.
    async fn unpublish(
        &mut self,
        registry: &Registry,
        request: &Self::Unpublish,
    ) -> Result<(), SvcError>;
    /// Republish the resource by shutting down existing dependents.
    async fn republish(
        &mut self,
        registry: &Registry,
        request: &Self::Republish,
    ) -> Result<Self::PublishOutput, SvcError>;
}

/// Resource Replica Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceReplicas {
    type Request: Sync + Send;
    type MoveRequest: Sync + Send;
    type MoveResp: Sync + Send;

    /// Set the resource's replica count.
    async fn set_replica(
        &mut self,
        registry: &Registry,
        request: &Self::Request,
    ) -> Result<(), SvcError>;

    async fn move_replica(
        &mut self,
        registry: &Registry,
        request: &Self::MoveRequest,
    ) -> Result<Self::MoveResp, SvcError>;
}

/// Resource Children/Offspring Operations.
#[async_trait::async_trait]
pub(crate) trait ResourceOffspring {
    type Add: Sync + Send;
    type AddOutput: Sync + Send;
    type Remove: Sync + Send;
    type Fault: Sync + Send;

    /// Add a child to the resource.
    async fn add_child(
        &mut self,
        registry: &Registry,
        request: &Self::Add,
    ) -> Result<Self::AddOutput, SvcError>;
    /// Remove a child from the resource.
    async fn remove_child(
        &mut self,
        registry: &Registry,
        request: &Self::Remove,
    ) -> Result<(), SvcError>;
}

/// Update this resource's owners list.
#[async_trait::async_trait]
pub(crate) trait ResourceOwnerUpdate {
    /// The updated resource owners.
    type Update: Sync + Send;

    /// Update the owners list.
    async fn remove_owners(
        &mut self,
        registry: &Registry,
        request: &Self::Update,
        // pre-update the actual spec anyway since this is a removal,
        update_on_commit: bool,
    ) -> Result<(), SvcError>;
}

/// Resource shutdown related operations.
#[async_trait::async_trait]
pub(crate) trait ResourceShutdownOperations {
    type RemoveShutdownTargets: Sync + Send;
    type Shutdown: Sync + Send;

    /// Shutdown the resource itself.
    async fn shutdown(
        &mut self,
        registry: &Registry,
        request: &Self::Shutdown,
    ) -> Result<(), SvcError>;

    /// Remove the shutdown targets.
    async fn remove_shutdown_targets(
        &mut self,
        registry: &Registry,
        request: &Self::RemoveShutdownTargets,
    ) -> Result<(), SvcError>;
}

/// Resource Snapshot Operations.
/// This can be used in combination with `ResourceLifecycle` where the parent resource can use
/// the `ResourceLifecycle` to manage the snapshot resource.
#[async_trait::async_trait]
pub(crate) trait ResourceSnapshotting {
    type Create: Sync + Send;
    type CreateOutput: Sync + Send + Sized;
    type Destroy: Sync + Send;
    type List: Sync + Send;
    type ListOutput: Sync + Send + Sized;

    /// Create a snapshot for the `Self` resource.
    async fn create_snap(
        &mut self,
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError>;
    /// List snapshots from the `Self` resource.
    async fn list_snaps(
        &self,
        registry: &Registry,
        request: &Self::List,
    ) -> Result<Self::ListOutput, SvcError>;
    /// Destroy the snapshot from the `Self` resource.
    async fn destroy_snap(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError>;
}

/// Resource Pruning Operations.
/// This is used to prune unused resources owned by this resource.
#[async_trait::async_trait]
pub(crate) trait ResourcePruning {
    /// Prune unused resources owned by this resource.
    async fn prune(
        &mut self,
        registry: &Registry,
        max_prune_limit: Option<usize>,
    ) -> Result<(), SvcError>;
}
