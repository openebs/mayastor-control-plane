use super::{
    diskpool::crd::v1beta3::{CrPoolState, DiskPool, DiskPoolStatus},
    dsp_api,
    error::Error,
};
use crate::diskpool::crd::v1beta3::{
    EncryptionSecretConfig, EncryptionSource, PoolError, PoolErrorCode,
};
use openapi::{
    apis::StatusCode,
    clients, models,
    models::{rest_json_error::Kind, CreatePoolBody, Encryption, EncryptionSecret, Pool},
};
use tracing::{debug, error, info};
use utils::{disk::normalize_disk, dsp_created_by_key};

use chrono::Utc;
use k8s_openapi::{api::core::v1::Event, apimachinery::pkg::apis::meta::v1::MicroTime};
use kube::{
    api::{Api, ObjectMeta, Patch, PatchParams, PostParams},
    runtime::{controller::Action, finalizer},
    Client, Resource, ResourceExt,
};
use openapi::models::SpecStatus;
use serde_json::json;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

const WHO_AM_I: &str = "DiskPool Operator";
const WHO_AM_I_SHORT: &str = "dsp-operator";

/// Additional per resource context during the runtime; it is volatile
#[derive(Clone)]
pub(crate) struct ResourceContext {
    /// The latest CRD known to us
    inner: Arc<DiskPool>,
    /// Counter that keeps track of how many times the reconcile loop has run
    /// within the current state
    num_retries: u32,
    /// Reference to the operator context
    ctx: Arc<OperatorContext>,
    event_info: Arc<Mutex<Vec<String>>>,
}

impl Deref for ResourceContext {
    type Target = DiskPool;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Data we want access to in error/reconcile calls
pub(crate) struct OperatorContext {
    /// Reference to our k8s client
    k8s: Client,
    /// Hashtable of name and the full last seen CRD
    inventory: tokio::sync::RwLock<HashMap<String, ResourceContext>>,
    /// HTTP client
    http: clients::tower::ApiClient,
    /// Interval
    interval: u64,
}

impl OperatorContext {
    /// Constructor for Operator context.
    pub(crate) fn new(
        k8s: Client,
        inventory: tokio::sync::RwLock<HashMap<String, ResourceContext>>,
        http: clients::tower::ApiClient,
        interval: u64,
    ) -> Self {
        Self {
            k8s,
            inventory,
            http,
            interval,
        }
    }

    /// Checks if dsp object is present in inventory.
    pub(crate) async fn inventory_contains(&self, key: String) -> bool {
        self.inventory.read().await.contains_key(&key)
    }

    /// Upsert the potential new CRD into the operator context. If an existing
    /// resource with the same name is present, the old resource is
    /// returned.
    pub(crate) async fn upsert(
        &self,
        ctx: Arc<OperatorContext>,
        dsp: Arc<DiskPool>,
    ) -> ResourceContext {
        let resource = ResourceContext {
            inner: dsp,
            num_retries: 0,
            event_info: Default::default(),
            ctx,
        };

        let mut i = self.inventory.write().await;
        debug!(count = i.keys().count(), "current number of CRs");

        match i.get_mut(&resource.name_any()) {
            Some(p) => {
                if p.resource_version() == resource.resource_version() {
                    if matches!(
                        resource.status,
                        Some(DiskPoolStatus {
                            cr_state: CrPoolState::Created,
                            ..
                        })
                    ) {
                        return p.clone();
                    }

                    debug!(status = ?resource.status, "duplicate event or long running operation");

                    // The status should be the same here as well
                    assert_eq!(&p.status, &resource.status);
                    p.num_retries += 1;
                    return p.clone();
                }

                // Its a new resource version which means we will swap it out
                // to reset the counter.
                let p = i
                    .insert(resource.name_any(), resource.clone())
                    .expect("existing resource should be present");
                info!(name = ?p.name_any(), "new resource_version inserted");
                resource
            }

            None => {
                let p = i.insert(resource.name_any(), resource.clone());
                assert!(p.is_none());
                resource
            }
        }
    }
    /// Remove the resource from the operator
    pub(crate) async fn remove(&self, name: String) -> Option<ResourceContext> {
        let mut i = self.inventory.write().await;
        if let Some(removed) = i.remove(&name) {
            info!(name = ?removed.name_any(), "removed from inventory");
            return Some(removed);
        }
        None
    }
}

impl ResourceContext {
    /// Called when putting our finalizer on top of the resource.
    #[tracing::instrument(fields(name = _dsp.name_any()))]
    pub(crate) async fn put_finalizer(_dsp: Arc<DiskPool>) -> Result<Action, Error> {
        Ok(Action::await_change())
    }

    /// Remove pool from control plane if exist, Then delete it from map.
    #[tracing::instrument(fields(name = resource.name_any()), skip(resource))]
    pub(crate) async fn delete_finalizer(
        resource: ResourceContext,
        attempt_delete: bool,
    ) -> Result<Action, Error> {
        let ctx = resource.ctx.clone();
        if attempt_delete {
            resource.delete_pool().await?;
        }
        if ctx.remove(resource.name_any()).await.is_none() {
            // In an unlikely event where we cant remove from inventory. We will requeue and
            // reattempt again in 10 seconds.
            error!("Failed to remove from inventory");
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
        Ok(Action::await_change())
    }

    /// Clone the inner value of this resource
    fn inner(&self) -> Arc<DiskPool> {
        self.inner.clone()
    }

    /// Construct an API handle for the resource
    fn api(&self) -> Api<DiskPool> {
        dsp_api(&self.ctx.k8s, &self.namespace().unwrap())
    }

    /// Construct an API handle for the k8s secret
    fn secret_api(&self) -> Api<k8s_openapi::api::core::v1::Secret> {
        Api::namespaced(self.ctx.k8s.clone(), &self.namespace().unwrap())
    }

    /// Control plane pool handler.
    fn pools_api(&self) -> &dyn openapi::apis::pools_api::tower::client::Pools {
        self.ctx.http.pools_api()
    }

    /// Control plane block device handler.
    fn block_devices_api(
        &self,
    ) -> &dyn openapi::apis::block_devices_api::tower::client::BlockDevices {
        self.ctx.http.block_devices_api()
    }

    /// Patch the given dsp status to the state provided.
    async fn patch_status(&self, status: DiskPoolStatus) -> Result<DiskPool, Error> {
        let status = json!({ "status": status });

        let ps = PatchParams::apply(WHO_AM_I);

        let o = self
            .api()
            .patch_status(&self.name_any(), &ps, &Patch::Merge(&status))
            .await
            .map_err(|source| Error::Kube { source })?;

        debug!(name = o.name_any(), old = ?self.status, new = ?o.status, "status changed");
        Ok(o)
    }

    /// Create a pool when there is no status found. When no status is found for
    /// this resource it implies that it does not exist yet, and so we create
    /// it. We set the state of the object to Creating, such that we
    /// can track its progress.
    pub(crate) async fn init_cr(&self) -> Result<Action, Error> {
        let _ = self.patch_status(DiskPoolStatus::default()).await?;
        Ok(Action::await_change())
    }

    /// Mark Pool state as None as couldn't find already provisioned pool in control plane.
    async fn mark_pool_error(&self, error: Option<PoolError>) -> Result<Action, Error> {
        self.patch_status(DiskPoolStatus::not_found(&self.inner, error))
            .await?;
        Ok(Action::requeue(Duration::from_secs(30)))
    }

    /// Mark Pool state as None and error as DiskNotFound as we couldn't find the
    /// pool disk during the creation/import attempt.
    async fn mark_disk_not_found(&self) -> Result<(), Error> {
        self.patch_status(DiskPoolStatus::disk_not_found(&self.inner))
            .await?;
        Ok(())
    }

    /// Mark Pool as Creating as couldn't complete the creation attempt.
    async fn mark_pool_creating(&self, pool: Pool) -> Result<(), Error> {
        let msg = "Timed out whilst trying to create the diskpool";
        self.k8s_notify("Create/Import", "Timeout", msg, "Warning")
            .await;
        let mut pool = DiskPoolStatus::from(pool).with_conditions(self);
        pool.cr_state = CrPoolState::Creating;
        error!(?pool, msg);
        self.patch_status(pool).await?;
        Ok(())
    }

    /// Removes pool expand annotation from the CR.
    async fn remove_expand_annotation(&self) -> Result<(), Error> {
        let patch = Patch::Merge(json!({
            "metadata": {
                "annotations": {
                    "openebs.io/expand": null
                }
            }
        }));
        let ps = PatchParams::default();
        let _o: kube::api::PartialObjectMeta<DiskPool> = self
            .api()
            .patch_metadata(&self.name_any(), &ps, &patch)
            .await
            .map_err(|source| Error::Kube { source })?;
        Ok(())
    }

    /// Patch the resource state to creating.
    async fn is_missing(&self) -> Result<Action, Error> {
        self.patch_status(DiskPoolStatus::default()).await?;
        Ok(Action::await_change())
    }

    /// Patch the resource state to terminating.
    async fn mark_terminating_when_unknown(&self) -> Result<Action, Error> {
        self.patch_status(DiskPoolStatus::terminating_when_unknown().with_conditions(self))
            .await?;
        Ok(Action::requeue(Duration::from_secs(self.ctx.interval)))
    }

    /// Used to patch the DiskPool when the data plane state is Unknown.
    /// The diagnostics may or may not be set.
    async fn mark_unknown_state(&self, pool: Pool) -> Result<Action, Error> {
        self.patch_status(DiskPoolStatus::from(pool).with_conditions(self))
            .await?;
        Ok(Action::requeue(Duration::from_secs(self.ctx.interval)))
    }

    /// Create or import the pool, on failure try again.
    #[tracing::instrument(fields(name = self.name_any(), status = ?self.status), skip(self))]
    pub(crate) async fn create_or_import(self) -> Result<Action, Error> {
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert(dsp_created_by_key(), String::from(utils::DSP_OPERATOR));
        if let Some(topology) = self.spec.topology() {
            for (label_key, label_value) in topology.labelled.iter() {
                labels.insert(label_key.to_string(), label_value.to_string());
            }
        }

        let encryption = match self.spec.encryption_config() {
            None => None,
            Some(config) => match config.source {
                EncryptionSource::Secret(secret_config) => {
                    Some(self.validate_encryption_secret(secret_config).await?)
                }
            },
        };

        let cluster_size = match self.spec.cluster_size() {
            Some(ref c) => {
                let parsed = parse_size::parse_size(c).map_err(|_| Error::Generic {
                    message: format!("Couldn't parse pool cluster size: {c:?}"),
                })?;
                Some(parsed)
            }
            None => None,
        };

        let body = CreatePoolBody::new_all(
            self.spec.disks(),
            labels,
            encryption,
            cluster_size,
            self.spec.max_expansion(),
        );
        if let Err(error) = self
            .pools_api()
            .put_node_pool(&self.spec.node(), &self.name_any(), body)
            .await
        {
            self.handle_create_failed(error).await?;
        }

        self.k8s_notify(
            "Create/Import",
            "Created",
            "Created or imported pool",
            "Normal",
        )
        .await;

        self.pool_created().await
    }

    async fn handle_create_failed(
        &self,
        error: openapi::tower::client::Error<models::RestJsonError>,
    ) -> Result<(), Error> {
        if let Ok(pool) = self
            .pools_api()
            .get_node_pool(&self.spec.node, &self.name_any())
            .await
        {
            let pool = pool.into_body();
            let spec_state = pool.spec.as_ref().map(|s| s.status);
            match spec_state.unwrap_or_default() {
                SpecStatus::Creating => {
                    // Backend device is probably too large / too slow...
                    self.mark_pool_creating(pool).await?;
                    return Err(error.into());
                }
                SpecStatus::Created => {
                    // somehow the pool got created meanwhile, so let's go ahead with success.
                    return Ok(());
                }
                // unexpected, let's handle it generically below
                SpecStatus::Deleting | SpecStatus::Deleted | SpecStatus::Purging => {}
            }
        }

        match &error {
            clients::tower::Error::Response(response)
                if response.status() == StatusCode::NOT_FOUND =>
            {
                let error_str = response.to_string();
                // The current API does not specify this via the type system, so we have to resort to this hackery...
                if error_str.contains("NodeNotFound") {
                    return Err(self
                        .handle_node_offline(PoolErrorCode::NodeIsUnknown, error)
                        .await?);
                } else {
                    // probably disk not found, but we're not sure yet...?
                }
            }
            clients::tower::Error::Response(response)
                if response.status() == StatusCode::PRECONDITION_FAILED =>
            {
                return Err(self
                    .handle_node_offline(PoolErrorCode::NodeIsOffline, error)
                    .await?);
            }
            _ => return Err(self.handle_create_error(error).await?),
        }

        // todo: do we really need this or is the is the NOT_FOUND status sufficient?
        let response = match self
            .block_devices_api()
            .get_node_block_devices(&self.spec.node(), Some(true))
            .await
        {
            Ok(response) => response.into_body(),
            Err(clients::tower::Error::Response(response))
                if response.status() == StatusCode::PRECONDITION_FAILED =>
            {
                return Err(self
                    .handle_node_offline(PoolErrorCode::NodeIsOffline, error)
                    .await?)
            }
            Err(clients::tower::Error::Response(response))
                if response.status() == StatusCode::NOT_FOUND =>
            {
                return Err(self
                    .handle_node_offline(PoolErrorCode::NodeIsUnknown, error)
                    .await?)
            }
            _ => return Err(self.handle_create_error(error).await?),
        };

        if !response.into_iter().any(|b| {
            b.devname == normalize_disk(&self.spec.disks()[0])
                || b.devlinks
                    .iter()
                    .any(|d| *d == normalize_disk(&self.spec.disks()[0]))
        }) {
            self.k8s_notify(
                "Create/Import",
                "DiskNotFound",
                &format!(
                    "The block device(s): {} can not be found",
                    &self.spec.disks()[0]
                ),
                "Warning",
            )
            .await;
            error!(
                "The block device(s): {} can not be found",
                &self.spec.disks()[0]
            );
            self.mark_disk_not_found().await?;
        } else {
            let error_str = match error.error_body() {
                None => format!(
                    "Unable to create or import pool, cause: {:?}",
                    error.status(),
                ),
                Some(body) => format!(
                    "Unable to create or import pool, cause: {:?}: {}",
                    error.status(),
                    body.message
                ),
            };
            self.k8s_notify("Create/Import", "Failure", &error_str, "Critical")
                .await;
            error!(%error, "Unable to create or import pool");
        }
        Err(error.into())
    }

    async fn handle_node_offline(
        &self,
        code: PoolErrorCode,
        error: openapi::tower::client::Error<models::RestJsonError>,
    ) -> Result<Error, Error> {
        self.k8s_notify("Create/Import", code.as_ref(), "", "Warning")
            .await;
        self.mark_pool_error(Some(PoolError {
            code,
            message: None,
        }))
        .await?;
        error!("Unable to find io-engine node {}", self.spec.node);
        Err(error.into())
    }

    async fn handle_create_error(
        &self,
        error: openapi::tower::client::Error<models::RestJsonError>,
    ) -> Result<Error, Error> {
        let error_str = match error.error_body() {
            None => format!(
                "Unable to create or import pool, cause: {:?}",
                error.status(),
            ),
            Some(body) => format!(
                "Unable to create or import pool, cause: {:?}: {}",
                error.status(),
                body.message
            ),
        };
        self.k8s_notify("Create/Import", "Failure", &error_str, "Critical")
            .await;
        self.mark_pool_error(Some(PoolError {
            code: PoolErrorCode::Unknown,
            message: Some(error.to_string()),
        }))
        .await?;
        error!(%error, "Unable to create or import pool");
        Err(error.into())
    }

    /// Delete the pool from the io-engine instance
    #[tracing::instrument(fields(name = self.name_any(), status = ?self.status), skip(self))]
    async fn delete_pool(&self) -> Result<Action, Error> {
        let res = self
            .pools_api()
            .del_node_pool(&self.spec.node(), &self.name_any(), None)
            .await;

        match res {
            Ok(_) => {
                self.k8s_notify(
                    "Destroy",
                    "Destroyed",
                    "The pool has been destroyed",
                    "Normal",
                )
                .await;
                Ok(Action::await_change())
            }
            Err(clients::tower::Error::Response(response))
                if response.status() == StatusCode::NOT_FOUND =>
            {
                self.k8s_notify(
                    "Destroy",
                    "AlreadyDestroyed",
                    "The pool was already destroyed",
                    "Normal",
                )
                .await;
                Ok(Action::await_change())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Gets pool from control plane and sets state as applicable.
    #[tracing::instrument(fields(name = self.name_any(), status = ?self.status), skip(self))]
    async fn pool_created(self) -> Result<Action, Error> {
        let pool = self
            .pools_api()
            .get_node_pool(&self.spec.node(), &self.name_any())
            .await?
            .into_body();

        if pool.state.is_some() {
            let _ = self
                .patch_status(DiskPoolStatus::from(pool).with_conditions(&self))
                .await?;

            self.k8s_notify(
                "Create/Import",
                "Online",
                "Pool online and ready to roll!",
                "Normal",
            )
            .await;

            Ok(Action::await_change())
        } else {
            // the pool does not have a status yet reschedule the operation
            Ok(Action::requeue(Duration::from_secs(3)))
        }
    }

    /// Check the state of the pool.
    ///
    /// If "openebs.io/expand" is set, then attempt the pool expansion by invoking put_pool_expand.
    /// Removes annotation in 3 failure cases:
    /// 1. If pool device is extended beyond max_expandable_size, expansion fails with OutOfRange.
    /// 2. If device is not resized before adding expand annotation.
    /// 3. Device rescan fails if Pool device is detached from the node. It fails even if it comes back.
    /// We need to handle that scenario. Until then, we will not retry this operation.
    ///
    /// Get the pool information from the control plane and use this to set the state of the CRD
    /// accordingly. If the control plane returns a pool state, set the CRD to 'Online'. If the
    /// control plane does not return a pool state (occurs when a node is missing), set the CRD to
    /// 'Unknown' and let the reconciler retry later.
    #[tracing::instrument(fields(name = self.name_any(), status = ?self.status), skip(self))]
    pub(crate) async fn pool_check(&self) -> Result<Action, Error> {
        let name = self.name_any();
        if let Some(annotation) = self.metadata.annotations.clone() {
            if Some("true") == annotation.get("openebs.io/expand").map(|s| s.as_str()) {
                info!("Attempting to expand DiskPool");
                match self.pools_api().put_pool_expand(&name).await {
                    Err(e) => {
                        if matches!(
                            e.error_body(),
                            Some(body) if matches!(body.kind, Kind::OutOfRange | Kind::DiskNotExtended | Kind::DiskRescanFailed)
                        ) {
                            error!("DiskPool expansion failed, Stopping reconciliation err: {e:?}");
                            let _ = self.remove_expand_annotation().await;
                        } else {
                            error!("DiskPool expansion failed, {e:?}");
                        }
                    }
                    Ok(_) => {
                        info!("DiskPool expanded successfully");
                        let _ = self.remove_expand_annotation().await;
                    }
                }
            }
        }

        let pool = match self
            .pools_api()
            .get_node_pool(&self.spec.node(), &name)
            .await
        {
            Ok(response) => response,
            Err(clients::tower::Error::Response(response)) => {
                return if response.status() == clients::tower::StatusCode::NOT_FOUND {
                    if self.metadata.deletion_timestamp.is_some() {
                        tracing::debug!("DiskPool deleted, exiting pool_check");
                        Ok(Action::await_change())
                    } else {
                        let message = "The pool has disappeared from the control-plane";
                        tracing::warn!("deleted by external event NOT recreating");
                        self.k8s_notify("PoolCheck", "PoolNotFound", message, "Error")
                            .await;
                        // We expected the control plane to have a spec for this pool. It didn't so
                        // set the pool_status in CRD to None.
                        self.mark_pool_error(Some(PoolError {
                            code: PoolErrorCode::PoolDeleted,
                            message: Some(message.to_string()),
                        }))
                        .await
                    }
                } else if response.status() == clients::tower::StatusCode::SERVICE_UNAVAILABLE
                    || response.status() == clients::tower::StatusCode::REQUEST_TIMEOUT
                {
                    let message =
                        "Could not reach Rest API service. Please check control plane health";
                    // Probably grpc server is not yet up
                    self.k8s_notify("PoolCheck", "ApiUnreachable", message, "Warning")
                        .await;
                    self.mark_pool_error(Some(PoolError {
                        code: PoolErrorCode::Unreachable,
                        message: Some(message.to_string()),
                    }))
                    .await
                } else {
                    let message = match response.error_body() {
                        None => format!(
                            "The pool information is not available, cause {}",
                            response.status(),
                        ),
                        Some(body) => format!(
                            "The pool information is not available, cause {}: {}",
                            response.status(),
                            body.message
                        ),
                    };
                    self.k8s_notify("PoolCheck", "UnexpectedApiError", &message, "Warning")
                        .await;
                    // what is this covering? Is leaving the object in default state the correct thing?
                    self.is_missing().await
                };
            }
            Err(clients::tower::Error::Request(req)) => {
                // Probably grpc server is not yet up
                let message = format!("The pool information is not available: {req}");
                self.k8s_notify("PoolCheck", "ApiClientError", &message, "Warning")
                    .await;
                return self
                    .mark_pool_error(Some(PoolError {
                        code: PoolErrorCode::Unreachable,
                        message: Some("Failed to send request".to_string()),
                    }))
                    .await;
            }
        }
        .into_body();
        // As pool exists, set the status based on the presence of pool state.
        self.set_status_or_unknown(pool).await
    }

    /// If the pool, has a state we set that status to the CR and if it does not have a state
    /// we set the status as unknown so that we can try again later.
    async fn set_status_or_unknown(&self, pool: Pool) -> Result<Action, Error> {
        if pool.state.is_none() {
            return if self.metadata.deletion_timestamp.is_some() {
                self.mark_terminating_when_unknown().await
            } else {
                self.mark_unknown_state(pool).await
            };
        }

        if let Some(status) = &self.status {
            let mut new_status = DiskPoolStatus::from(pool);
            if self.metadata.deletion_timestamp.is_some() {
                new_status.cr_state = CrPoolState::Terminating;
            }
            if status != &new_status {
                // update the usage state such that users can see the values changes
                // as replicas are added and/or removed.
                let _ = self.patch_status(new_status).await;
            }
        }

        // always reschedule though
        Ok(Action::requeue(Duration::from_secs(self.ctx.interval)))
    }

    /// Post an event, typically these events are used to indicate that
    /// something happened. They should not be used to "log" generic
    /// information. Events are GC-ed by k8s automatically.
    ///
    /// action:
    ///     What action was taken/failed regarding the object.
    /// reason:
    ///     This should be a short, machine understandable string that gives the
    ///     reason for the transition into the object's current status.
    /// message:
    ///     A human-readable description of the status of this operation.
    /// type_:
    ///     Type of this event (Normal, Warning), new types could be added in
    ///     the future
    async fn k8s_notify(&self, action: &str, reason: &str, message: &str, type_: &str) {
        let client = self.ctx.k8s.clone();
        let ns = self.namespace().expect("must be namespaced");
        let e: Api<Event> = Api::namespaced(client.clone(), &ns);
        let pp = PostParams::default();
        let time = Utc::now();
        if self
            .event_info
            .lock()
            .unwrap()
            .contains(&message.to_string())
        {
            return;
        }
        self.event_info.lock().unwrap().push(message.to_string());
        let metadata = ObjectMeta {
            // the name must be unique for all events we post
            generate_name: Some(format!("{}.{:x}", self.name_any(), time.timestamp())),
            namespace: Some(ns),
            ..Default::default()
        };

        _ = e
            .create(
                &pp,
                &Event {
                    event_time: Some(MicroTime(time)),
                    involved_object: self.object_ref(&()),
                    action: Some(action.into()),
                    reason: Some(reason.into()),
                    type_: Some(type_.into()),
                    metadata,
                    reporting_component: Some(WHO_AM_I_SHORT.into()),
                    reporting_instance: Some(
                        std::env::var("MY_POD_NAME")
                            .ok()
                            .unwrap_or_else(|| WHO_AM_I_SHORT.into()),
                    ),
                    message: Some(message.into()),
                    ..Default::default()
                },
            )
            .await
            .inspect_err(|error| error!(?error, "Failed to create event"));
    }

    /// Callback hooks for the finalizers
    pub(crate) async fn finalizer(&self) -> Result<Action, Error> {
        let _ = finalizer(
            &self.api(),
            &utils::constants::dsp_finalizer(),
            self.inner(),
            |event| async move {
                match event {
                    finalizer::Event::Apply(dsp) => Self::put_finalizer(dsp).await,
                    finalizer::Event::Cleanup(dsp) => match self
                        .pools_api()
                        .get_node_pool(&self.spec.node(), &self.name_any())
                        .await
                    {
                        Ok(pool) => {
                            if dsp.status.as_ref().map(|d| d.cr_state)
                                != Some(CrPoolState::Terminating)
                            {
                                let new_status =
                                    DiskPoolStatus::terminating(&dsp, pool.into_body());
                                let _ = self.patch_status(new_status).await?;
                            }
                            Self::delete_finalizer(self.clone(), true).await
                        }
                        Err(clients::tower::Error::Response(response))
                            if response.status() == StatusCode::NOT_FOUND =>
                        {
                            Self::delete_finalizer(self.clone(), false).await
                        }
                        Err(error) => Err(error.into()),
                    },
                }
            },
        )
        .await
        .map_err(|e| error!(?e));
        Ok(Action::await_change())
    }

    async fn validate_encryption_secret(
        &self,
        config: EncryptionSecretConfig,
    ) -> Result<Encryption, Error> {
        let name = config.name;
        if let Err(error) = self.secret_api().get(&name).await {
            self.k8s_notify(
                "Create/Import",
                "SecretValidation",
                &format!("Failed to get k8s secret for encryption {name}, error: {error}"),
                "Critical",
            )
            .await;
            self.mark_pool_error(Some(PoolError {
                code: PoolErrorCode::EncryptionSecretError,
                message: Some(format!("Failed to get encryption secret: {name}")),
            }))
            .await?;
            return Err(Error::Kube { source: error });
        }

        Ok(Encryption::secret(EncryptionSecret { name }))
    }
}
