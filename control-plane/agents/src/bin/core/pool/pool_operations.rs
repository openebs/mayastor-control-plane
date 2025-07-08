use crate::{
    controller::{
        io_engine::PoolApi,
        registry::Registry,
        resources::{
            operations::{ResourceCordon, ResourceLabel, ResourceLifecycle},
            operations_helper::{GuardedOperationsHelper, OnCreateFail, OperationSequenceGuard},
            OperationGuardArc,
        },
    },
    pool::operations_helper::devlink_preflight_checks,
};
use agents::errors::{SvcError, SvcError::CordonedNode};
use grpc::operations::pool::traits::PoolCordonRequest;
use std::collections::HashMap;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::pool::{PoolCordonOp, PoolOperation, PoolSpec},
        transport::{CreatePool, CtrlPoolState, DestroyPool, Pool},
    },
};
use utils::dsp_created_by_key;

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<PoolSpec> {
    type Create = CreatePool;
    type CreateOutput = Pool;
    type Destroy = DestroyPool;

    async fn create(
        registry: &Registry,
        request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        let specs = registry.specs();

        if registry.node_cordoned(&request.node)? {
            return Err(CordonedNode {
                node_id: request.node.to_string(),
            });
        }

        if request.disks.len() != 1 {
            return Err(SvcError::InvalidPoolDeviceNum {
                disks: request.disks.clone(),
            });
        }

        let pool_get_result = registry.specs().pool(&request.id);
        if let Ok(pool) = &pool_get_result {
            if pool.status.created() {
                return Err(SvcError::AlreadyExists {
                    kind: ResourceKind::Pool,
                    id: request.id.to_string(),
                });
            }
        }

        let node = registry.node_wrapper(&request.node).await?;
        // todo: issue rpc to the node to find out?
        if !node.read().await.is_online() {
            return Err(SvcError::NodeNotOnline {
                node: request.node.clone(),
            });
        }

        if pool_get_result.is_err() {
            // If the devlink for a device is used for multiple pools, reject new creation.
            devlink_preflight_checks(request, node.clone(), registry).await?
        }

        let mut pool = specs
            .get_or_create_pool(request)
            .operation_guard_wait()
            .await?;
        let _ = pool.start_create(registry, request).await?;

        let result = node.create_pool(request).await;
        let on_fail = OnCreateFail::on_pool_create_err(&result);
        let state = pool.complete_create(result, registry, on_fail).await?;
        let spec = pool.lock().clone();
        Ok(Pool::new(spec, Some(CtrlPoolState::new(state))))
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        // what if the node is never coming back?
        // do we need a way to forcefully "delete" things?
        let node = registry.node_wrapper(&request.node).await?;

        self.start_destroy(registry).await?;

        // We may want to prevent this in some situations, example: if a disk URI has changed, we
        // may want to ensure it really is deleted.
        // For now, if there's nothing provisioned on the pool anyway, just allow it..
        // TODO: pass this via REST
        let allow_not_found = self.validate_destroy(registry).is_ok();
        let result = match node.destroy_pool(request).await {
            Ok(_) => Ok(()),
            Err(SvcError::PoolNotFound { .. }) => {
                match node.import_pool(&self.as_ref().into()).await {
                    Ok(_) => node.destroy_pool(request).await,
                    Err(error) => match error.tonic_code() {
                        tonic::Code::NotFound if allow_not_found => Ok(()),
                        tonic::Code::InvalidArgument => Ok(()),
                        _other => Err(error),
                    },
                }
            }
            Err(error) => Err(error),
        };
        self.complete_destroy(result, registry).await
    }
}

#[async_trait::async_trait]
impl ResourceLifecycle for Option<OperationGuardArc<PoolSpec>> {
    type Create = CreatePool;
    type CreateOutput = Pool;
    type Destroy = DestroyPool;

    async fn create(
        _registry: &Registry,
        _request: &Self::Create,
    ) -> Result<Self::CreateOutput, SvcError> {
        unimplemented!()
    }

    async fn destroy(
        &mut self,
        registry: &Registry,
        request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        if let Some(pool) = self {
            pool.destroy(registry, request).await
        } else {
            // todo: add flag to handle bypassing calls to io-engine!
            Err(SvcError::PoolNotFound {
                pool_id: request.id.clone(),
            })
        }
    }
}

/// Resource Label Operations.
#[async_trait::async_trait]
impl ResourceLabel for OperationGuardArc<PoolSpec> {
    type LabelOutput = PoolSpec;
    type UnlabelOutput = PoolSpec;

    /// Label a node via operation guard functions.
    async fn label(
        &mut self,
        registry: &Registry,
        labels: HashMap<String, String>,
        overwrite: bool,
    ) -> Result<Self::LabelOutput, SvcError> {
        let cloned_pool_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_pool_spec,
                PoolOperation::Label((labels, overwrite).into()),
            )
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    /// Unlabel a node via operation guard functions.
    async fn unlabel(
        &mut self,
        registry: &Registry,
        label_key: String,
    ) -> Result<Self::UnlabelOutput, SvcError> {
        if label_key == dsp_created_by_key() {
            return Err(SvcError::ForbiddenUnlabelKey {
                labels: label_key,
                resource_kind: ResourceKind::Pool,
            });
        }
        let cloned_pool_spec = self.lock().clone();
        let spec_clone = self
            .start_update(
                registry,
                &cloned_pool_spec,
                PoolOperation::Unlabel(label_key.into()),
            )
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }
}

/// Resource Cordon Operations.
#[async_trait::async_trait]
impl ResourceCordon for OperationGuardArc<PoolSpec> {
    type CordonOutput = PoolSpec;
    type UncordonOutput = PoolSpec;
    type Request = PoolCordonRequest;

    async fn cordon(
        &mut self,
        registry: &Registry,
        request: PoolCordonRequest,
    ) -> Result<Self::CordonOutput, SvcError> {
        let request = PoolCordonOp {
            replicas: request.replicas,
            snapshots: request.snapshots,
            restores: request.restores,
            import: request.import,
        };
        let spec_clone = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &spec_clone, PoolOperation::Cordon(request))
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }

    async fn uncordon(
        &mut self,
        registry: &Registry,
        request: PoolCordonRequest,
    ) -> Result<Self::UncordonOutput, SvcError> {
        let request = PoolCordonOp {
            replicas: request.replicas,
            snapshots: request.snapshots,
            restores: request.restores,
            import: request.import,
        };
        let spec_clone = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &spec_clone, PoolOperation::Uncordon(request))
            .await?;

        self.complete_update(registry, Ok(()), spec_clone).await?;
        Ok(self.as_ref().clone())
    }
}
