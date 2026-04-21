use super::{
    diskpool::crd::v1beta3::{CrPoolState, DiskPool, DiskPoolStatus},
    dsp_api,
    error::Error,
};
use crate::diskpool::crd::v1beta3::{
    EncryptionSecretConfig, EncryptionSource, PoolError, PoolErrorCode, PoolStatus,
};
use openapi::{
    apis::StatusCode,
    clients, models,
    models::{
        rest_json_error::Kind, CreatePoolBody, DeletePoolBody, Encryption, EncryptionSecret, Pool,
    },
};
use tracing::{debug, error, info, trace};
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

/// Annotation key for declarative pool deletion options.
///
/// The value is a comma-separated list of `key=value` pairs. Bare keys
/// (without `=`) are treated as `key=true`. Options are processed left to
/// right and latter options take precedence.
///
/// Supported keys:
///   purge                - Force removal without contacting io-engine.
///   accept               - Confirm deletion of replicas owned by volumes.
///   accept-volume-loss   - Confirm volume data loss.
///   accept-snapshot-loss - Confirm snapshot data loss.
///   accept-data-loss     - Shorthand: sets both accept-volume-loss and
///                          accept-snapshot-loss.
///   purge-accept-all     - Shorthand: sets purge, accept, accept-volume-loss,
///                          and accept-snapshot-loss.
///
/// Examples:
/// ```
/// openebs.io/delete-opts: purge=true,accept=true,accept-data-loss=true
/// openebs.io/delete-opts: purge-accept-all
/// openebs.io/delete-opts: purge-accept-all,accept-volume-loss=false
/// ```
const DELETE_OPTS_ANNOTATION: &str = "openebs.io/delete-opts";

/// Recognised keys in the `openebs.io/delete-opts` annotation value.
#[derive(Debug)]
enum DeleteOptKey {
    /// Force removal without contacting io-engine.
    Purge,
    /// Confirm deletion of replicas owned by volumes.
    Accept,
    /// Confirm volume data loss.
    AcceptVolumeLoss,
    /// Confirm snapshot data loss.
    AcceptSnapshotLoss,
    /// Shorthand: sets both `AcceptVolumeLoss` and `AcceptSnapshotLoss`.
    AcceptDataLoss,
    /// Shorthand: sets `Purge`, `Accept`, `AcceptVolumeLoss`, and `AcceptSnapshotLoss`.
    PurgeAcceptAll,
}

impl DeleteOptKey {
    fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "purge" => Ok(Self::Purge),
            "accept" => Ok(Self::Accept),
            "accept-volume-loss" => Ok(Self::AcceptVolumeLoss),
            "accept-snapshot-loss" => Ok(Self::AcceptSnapshotLoss),
            "accept-data-loss" => Ok(Self::AcceptDataLoss),
            "purge-accept-all" => Ok(Self::PurgeAcceptAll),
            _ => Err(Error::Generic {
                message: format!(
                    "unknown key '{s}' in '{DELETE_OPTS_ANNOTATION}' annotation. \
                     Valid keys: purge, accept, accept-volume-loss, accept-snapshot-loss, \
                     accept-data-loss, purge-accept-all"
                ),
            }),
        }
    }
}

/// Parse the `openebs.io/delete-opts` annotation value into a `DeletePoolBody`.
///
/// Returns `Some(body)` only when `purge` is set to `true`.
/// A normal (non-purge) deletion must not send a request body, so any
/// annotation that omits `purge` or sets it to `false` yields `None`.
fn parse_delete_opts_value(input: &str) -> Result<Option<DeletePoolBody>, Error> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(None);
    }

    let mut purge = None;
    let mut accept = None;
    let mut accept_volume_loss = None;
    let mut accept_snapshot_loss = None;

    for token in input.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        let (key_str, value) = match token.split_once('=') {
            Some((k, v)) => (k.trim(), v.trim()),
            None => (token, "true"),
        };

        let key = DeleteOptKey::parse(key_str)?;

        let bool_val = value.parse::<bool>().map_err(|_| Error::Generic {
            message: format!(
                "invalid value '{value}' for key '{key_str}' in '{DELETE_OPTS_ANNOTATION}' \
                 annotation: expected 'true' or 'false'"
            ),
        })?;

        match key {
            DeleteOptKey::PurgeAcceptAll => {
                purge = Some(bool_val);
                accept = Some(bool_val);
                accept_volume_loss = Some(bool_val);
                accept_snapshot_loss = Some(bool_val);
            }
            DeleteOptKey::Purge => purge = Some(bool_val),
            DeleteOptKey::Accept => accept = Some(bool_val),
            DeleteOptKey::AcceptDataLoss => {
                accept_volume_loss = Some(bool_val);
                accept_snapshot_loss = Some(bool_val);
            }
            DeleteOptKey::AcceptVolumeLoss => accept_volume_loss = Some(bool_val),
            DeleteOptKey::AcceptSnapshotLoss => accept_snapshot_loss = Some(bool_val),
        }
    }

    // Only produce a body when purge is explicitly requested.
    // Normal deletion expects no body (server returns 204).
    if purge != Some(true) {
        return Ok(None);
    }

    Ok(Some(DeletePoolBody {
        purge,
        accept,
        accept_volume_loss,
        accept_snapshot_loss,
    }))
}

/// Guidance for the user when a pool delete fails with a recoverable error.
struct PurgeGuidance {
    /// Short, bounded message for K8s events: just the error cause,
    /// no newlines, no remediation commands, no variable-length data.
    event: String,
    /// Detailed message and remediation for operator logs.
    log: PurgeGuidanceLog,
}

/// Log-side content for purge error guidance.
struct PurgeGuidanceLog {
    /// Human-readable error cause, may include variable-length details
    /// such as affected volume and snapshot lists from the REST error body.
    msg: String,
    /// Suggested kubectl command to fix the issue, emitted as a
    /// structured tracing field for visibility.
    example_command: Option<String>,
}

/// Return user-facing guidance when a pool delete fails with a recoverable
/// error that the user can fix by updating the annotation.
///
/// Returns `None` for errors that are not actionable (transient failures,
/// internal errors, etc.) — those are logged by the normal error path.
fn purge_error_guidance(
    pool_name: &str,
    ns: &str,
    error: &clients::tower::Error<models::RestJsonError>,
) -> Option<PurgeGuidance> {
    // Compile-time validated event message formatter.
    // Asserts that the literal fits within the K8s event message size limit.
    // Event messages are static strings (no pool_name/ns interpolation).
    macro_rules! event_msg {
        ($fmt:literal) => {{
            const _: () = assert!(
                $fmt.len() <= MAX_EVENT_MESSAGE_BYTES,
                "event message template exceeds K8s event size limit"
            );
            $fmt.to_string()
        }};
    }

    let body = error.error_body()?;
    let details = if body.details.is_empty() {
        String::new()
    } else {
        format!("\n  {}", body.details)
    };

    match body.kind {
        Kind::Unavailable => {
            let event = event_msg!(
                "Pool is offline, normal deletion is not possible: \
                 to purge, annotate the DiskPool CR with delete options."
            );
            let msg = format!(
                "Pool is offline, normal deletion is not possible. \
                 To purge, annotate the DiskPool CR with delete options:\n \
                 -------------------\n \
                 Example annotation:\n \
                 -------------------\n   \
                 {DELETE_OPTS_ANNOTATION}: purge=true\n\n",
            );
            let example_command = Some(format!(
                "kubectl annotate dsp {pool_name} -n {ns} \
                 {DELETE_OPTS_ANNOTATION}=purge=true --overwrite"
            ));
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command,
                },
            })
        }
        Kind::PoolNotPurgeable => {
            let event = event_msg!(
                "Cannot purge: pool state is known (Online/Degraded/Faulted/Suspected). \
                 Purge requires Unknown or Offline state. \
                 Remove the delete options annotation to use normal deletion."
            );
            let msg = "Cannot purge: pool state is known (Online/Degraded/Faulted/Suspected). \
                 Purge requires Unknown or Offline state. \
                 Remove the annotation to use normal deletion:\n"
                .to_string();
            let example_command = Some(format!(
                "kubectl annotate dsp {pool_name} -n {ns} {DELETE_OPTS_ANNOTATION}-"
            ));
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command,
                },
            })
        }
        Kind::PoolNotCordoned => {
            let event = event_msg!(
                "Cannot purge: pool is not cordoned. \
                 The operator should have cordoned the pool automatically \
                 — this will be retried."
            );
            let msg = event.clone();
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command: None,
                },
            })
        }
        Kind::PoolCordonInsufficient => {
            let event = event_msg!(
                "Cannot purge: pool cordon does not block both replica \
                 placement and snapshot creation. The operator should have \
                 cordoned the pool automatically — this will be retried."
            );
            let msg = event.clone();
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command: None,
                },
            })
        }
        Kind::PoolPurgeAcceptRequired => {
            let event = event_msg!(
                "Cannot purge: pool has replicas owned by volumes. \
                 Set 'accept=true' in the delete options annotation."
            );
            let msg = format!(
                "Cannot purge: pool has replicas owned by volumes. \
                 Set 'accept=true' in the delete options annotation:\n \
                 -------------------\n \
                 Example annotation:\n \
                 -------------------\n   \
                 {DELETE_OPTS_ANNOTATION}: purge=true,accept=true\n\n"
            );
            let example_command = Some(format!(
                "kubectl annotate dsp {pool_name} -n {ns} \
                 {DELETE_OPTS_ANNOTATION}=purge=true,accept=true --overwrite"
            ));
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command,
                },
            })
        }
        Kind::PoolPurgeVolumeLossAcceptRequired => {
            let event = event_msg!(
                "Cannot purge: volumes would lose their last healthy replica. \
                 Set 'accept-volume-loss=true' to accept this volume data loss \
                 and move ahead with pool purge. (Alternatively, set \
                 'accept-data-loss=true' to accept the loss of volume data and \
                 snapshot data)."
            );
            let msg = format!(
                "Cannot purge: volumes would lose their last healthy replica.{details}\n\
                 Set 'accept-volume-loss=true' to accept this volume data loss \
                 and move ahead with pool purge. (Alternatively, set \
                 'accept-data-loss=true' to accept the loss of volume data and \
                 snapshot data):\n \
                 -------------------\n \
                 Example annotation:\n \
                 -------------------\n   \
                 {DELETE_OPTS_ANNOTATION}: purge=true,accept=true,accept-data-loss=true\n\n"
            );
            let example_command = Some(format!(
                "kubectl annotate dsp {pool_name} -n {ns} \
                 {DELETE_OPTS_ANNOTATION}=purge=true,accept=true,accept-data-loss=true --overwrite"
            ));
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command,
                },
            })
        }
        Kind::PoolPurgeSnapshotLossAcceptRequired => {
            let event = event_msg!(
                "Cannot purge: snapshots would lose their last replica snapshot. \
                 Set 'accept-snapshot-loss=true' to accept this snapshot data loss \
                 and move ahead with pool purge. (Alternatively, set \
                 'accept-data-loss=true' to accept the loss of volume data and \
                 snapshot data)."
            );
            let msg = format!(
                "Cannot purge: snapshots would lose their last replica snapshot.{details}\n\
                 Set 'accept-snapshot-loss=true' to accept this snapshot data loss \
                 and move ahead with pool purge. (Alternatively, set \
                 'accept-data-loss=true' to accept the loss of volume data and \
                 snapshot data):\n \
                 -------------------\n \
                 Example annotation:\n \
                 -------------------\n   \
                 {DELETE_OPTS_ANNOTATION}: purge=true,accept=true,accept-data-loss=true\n\n"
            );
            let example_command = Some(format!(
                "kubectl annotate dsp {pool_name} -n {ns} \
                 {DELETE_OPTS_ANNOTATION}=purge=true,accept=true,accept-data-loss=true --overwrite"
            ));
            Some(PurgeGuidance {
                event,
                log: PurgeGuidanceLog {
                    msg,
                    example_command,
                },
            })
        }
        _ => None,
    }
}

/// Maximum byte length for a K8s event message.
/// The API server rejects events whose message exceeds ~1 KiB.
const MAX_EVENT_MESSAGE_BYTES: usize = 1024;

/// Truncate a message to fit within the K8s event message size limit.
/// If truncated, appends "..." to indicate the message was cut.
///
/// Applied inside `k8s_notify` as a runtime safety net for dynamic messages.
/// Handwritten guidance event templates are validated at compile time via
/// `event_msg!`. This function catches dynamic messages (e.g. the fallback
/// `"Pool deletion failed: {error}"`) that could exceed the limit.
fn truncate_for_event(msg: &str) -> String {
    if msg.len() <= MAX_EVENT_MESSAGE_BYTES {
        return msg.to_string();
    }
    let mut end = MAX_EVENT_MESSAGE_BYTES - 3;
    // Avoid splitting a multi-byte UTF-8 character.
    while end > 0 && !msg.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &msg[..end])
}

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

    /// Delete pool from the control plane (if it exists) and remove from inventory.
    #[tracing::instrument(fields(name = resource.name_any()), skip(resource))]
    pub(crate) async fn delete_finalizer(
        resource: ResourceContext,
        delete_body: Option<DeletePoolBody>,
    ) -> Result<Action, Error> {
        let ctx = resource.ctx.clone();
        resource.delete_pool(delete_body).await?;
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

    /// Update pool with creation failed, but with diagnostic information.
    async fn mark_pool_creat_diag(
        &self,
        error: PoolError,
        status: PoolStatus,
    ) -> Result<Action, Error> {
        self.patch_status(DiskPoolStatus::create_error_diag(
            &self.inner,
            error,
            status,
        ))
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

    /// Parse the `openebs.io/delete-opts` annotation from a DiskPool resource.
    ///
    /// Takes a `&DiskPool` explicitly so the caller can pass the freshest copy
    /// (e.g. `dsp` from a finalizer Cleanup event rather than a potentially stale
    /// `self`).
    ///
    /// Returns `Some(DeletePoolBody)` if the annotation is present, valid, and
    /// contains `purge=true`. Returns `None` if the annotation is absent or if
    /// `purge` is not set to `true` — in both cases the caller should proceed
    /// with a normal (body-less) deletion.
    ///
    /// Returns an error if the annotation is present but contains invalid syntax.
    /// The caller must not silently fall back to normal deletion on error —
    /// if the user set the annotation they intended purge, and a malformed
    /// annotation should block deletion until the user fixes it.
    fn parse_delete_opts(dsp: &DiskPool) -> Result<Option<DeletePoolBody>, Error> {
        let annotations = match dsp.metadata.annotations.as_ref() {
            Some(a) => a,
            None => return Ok(None),
        };
        let value = match annotations.get(DELETE_OPTS_ANNOTATION) {
            Some(v) => v,
            None => return Ok(None),
        };
        let body = parse_delete_opts_value(value)?;
        if let Some(ref b) = body {
            trace!(name = dsp.name_any(), ?b, "parsed delete-opts annotation");
        }
        Ok(body)
    }

    /// Cordon the pool for replicas and snapshots before a purge deletion.
    ///
    /// Purge requires the pool to be cordoned so no new replicas or snapshots
    /// are scheduled while resources are being cleaned up. The operator
    /// handles this automatically so the user does not need to run a separate
    /// cordon command.
    ///
    /// Idempotent: if the pool is already cordoned with the required
    /// constraints, the `AlreadyExists` response is treated as success.
    async fn cordon_for_purge(&self) -> Result<(), Error> {
        let pool_name = self.name_any();
        let body = models::PoolCordonReq::new_all(
            true,  // replicas
            true,  // snapshots
            false, // restores (implicitly blocked by replica placement block)
            false, // import
        );
        match self.pools_api().put_pool_cordon(&pool_name, body).await {
            Ok(_) => {
                info!(
                    pool.name = %pool_name,
                    "cordoned pool for replicas and snapshots before purge"
                );
                self.k8s_notify(
                    "Destroy",
                    "Cordoned",
                    "Pool cordoned for replicas and snapshots before purge",
                    "Normal",
                )
                .await;
                Ok(())
            }
            Err(error) if error.error_body().map(|b| b.kind) == Some(Kind::AlreadyExists) => {
                info!(
                    pool.name = %pool_name,
                    "pool already cordoned, proceeding with purge"
                );
                Ok(())
            }
            Err(error) => {
                let msg = format!("Failed to cordon pool before purge: {error}");
                error!(pool.name = %pool_name, "{msg}");
                self.k8s_notify("Destroy", "CordonFailed", &msg, "Warning")
                    .await;
                Err(error.into())
            }
        }
    }

    /// Mark pool as Deleted when its spec has been removed from the control plane.
    async fn mark_deleted(&self) -> Result<Action, Error> {
        self.patch_status(DiskPoolStatus::deleted().with_conditions(self))
            .await?;
        Ok(Action::requeue(Duration::from_secs(self.ctx.interval)))
    }

    /// Remove the operator's finalizer from the CR.
    ///
    /// Called when the CR is in `Deleted` state — the pool spec is already gone
    /// from the control plane, so there is nothing for the finalizer to clean up.
    /// Stripping it allows the user to `kubectl delete dsp` without the
    /// operator needing to be running.
    pub(crate) async fn strip_finalizer(&self) -> Result<Action, Error> {
        let finalizer_name = utils::constants::dsp_finalizer();
        let current = self.metadata.finalizers.as_deref().unwrap_or_default();
        if !current.contains(&finalizer_name) {
            return Ok(Action::requeue(Duration::from_secs(300)));
        }
        let remaining: Vec<&String> = current.iter().filter(|f| **f != finalizer_name).collect();
        let patch = Patch::Merge(json!({
            "metadata": {
                "finalizers": remaining
            }
        }));
        let ps = PatchParams::default();
        match self.api().patch(&self.name_any(), &ps, &patch).await {
            Ok(_) => {
                info!(
                    name = self.name_any(),
                    "stripped finalizer from orphaned CR"
                );
            }
            // The user may delete the DiskPool CR immediately after the pool
            // is removed. It's expected that the CR can disappear between
            // reconciles, so a 404 here is not an error.
            Err(kube::Error::Api(e)) if e.code == StatusCode::NOT_FOUND => {
                tracing::warn!(
                    name = self.name_any(),
                    "CR already deleted, finalizer strip not needed"
                );
            }
            Err(source) => return Err(Error::Kube { source }),
        }
        Ok(Action::requeue(Duration::from_secs(300)))
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
        if let clients::tower::Error::Response(response) = &error {
            if let Some(diag) = response
                .error_body()
                .and_then(|e| e.custom_info.as_ref().and_then(|i| i.pool.diag.as_ref()))
            {
                if let Some(probe) = diag.error.as_ref() {
                    self.handle_error_diag(probe, diag).await?;
                    return Err(error.into());
                }
            }
        }

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

    async fn handle_error_diag(
        &self,
        diag_error: &models::PoolProbeError,
        diag: &models::PoolDiag,
    ) -> Result<(), Error> {
        let message = diag_error.message.as_deref().unwrap_or("");
        let code = PoolErrorCode::from(diag_error.code);
        self.k8s_notify("Create/Import", code.as_ref(), message, "Warning")
            .await;
        self.mark_pool_creat_diag(
            PoolError {
                code,
                message: diag_error.message.clone(),
            },
            diag.status.into(),
        )
        .await?;
        Ok(())
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

    /// Delete the pool via the REST API.
    ///
    /// Two modes:
    /// - Normal deletion (`delete_body` is `None`): the control plane contacts
    ///   the io-engine to destroy the pool on disk. Returns 204 on success.
    /// - Purge deletion (`delete_body` contains `purge=true`): the control plane
    ///   removes the pool spec from etcd without contacting the io-engine, for
    ///   offline/faulted pools. Returns 200 with impact details on success.
    ///
    /// On errors, a K8s event is emitted. For actionable purge errors the event
    /// contains just the cause; remediation details (example annotation and
    /// kubectl command) are emitted as structured tracing fields in the operator
    /// logs. The error is still propagated so the operator retries on the next
    /// reconcile.
    #[tracing::instrument(fields(name = self.name_any(), status = ?self.status), skip(self, delete_body))]
    async fn delete_pool(&self, delete_body: Option<DeletePoolBody>) -> Result<Action, Error> {
        let is_purge = delete_body.as_ref().and_then(|b| b.purge).unwrap_or(false);

        let res = self
            .pools_api()
            .del_node_pool(&self.spec.node(), &self.name_any(), delete_body)
            .await;

        match res {
            Ok(_) => {
                let (reason, message) = if is_purge {
                    ("Purged", "The pool has been purged")
                } else {
                    ("Destroyed", "The pool has been destroyed")
                };
                self.k8s_notify("Destroy", reason, message, "Normal").await;
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
            Err(error) => {
                let pool_name = self.name_any();
                let ns = self.namespace().unwrap_or_default();
                match purge_error_guidance(&pool_name, &ns, &error) {
                    Some(guidance) => {
                        match guidance.log.example_command {
                            Some(cmd) => {
                                error!(example_command = %cmd, pool.name = %pool_name, "{}", guidance.log.msg)
                            }
                            None => error!(pool.name = %pool_name, "{}", guidance.log.msg),
                        }
                        self.k8s_notify("Destroy", "PurgeBlocked", &guidance.event, "Warning")
                            .await;
                    }
                    None => {
                        let msg = format!("Pool deletion failed: {error}");
                        error!(pool.name = %pool_name, "{msg}");
                        self.k8s_notify("Destroy", "DeleteFailed", &msg, "Warning")
                            .await;
                    }
                }
                Err(error.into())
            }
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
                        // Pool spec is gone from the control plane but the CR still exists.
                        // This can happen when the pool was deleted externally (via REST
                        // API or kubectl plugin) without also deleting the CR. Mark the
                        // CR as Deleted so users see a clear state when listing DiskPool
                        // resources.
                        let message = "The pool has disappeared from the control-plane";
                        tracing::warn!(
                            "pool spec gone (purged or deleted externally), marking CR as Deleted"
                        );
                        self.k8s_notify("PoolCheck", "PoolNotFound", message, "Warning")
                            .await;
                        self.mark_deleted().await
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
    ///     Truncated to `MAX_EVENT_MESSAGE_BYTES` if too long.
    /// type_:
    ///     Type of this event (Normal, Warning), new types could be added in
    ///     the future
    async fn k8s_notify(&self, action: &str, reason: &str, message: &str, type_: &str) {
        let message = truncate_for_event(message);
        let client = self.ctx.k8s.clone();
        let ns = self.namespace().expect("must be namespaced");
        let e: Api<Event> = Api::namespaced(client.clone(), &ns);
        let pp = PostParams::default();
        let time = Utc::now();
        if self.event_info.lock().unwrap().contains(&message) {
            return;
        }
        self.event_info.lock().unwrap().push(message.clone());
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
                    message: Some(message),
                    ..Default::default()
                },
            )
            .await
            .inspect_err(|error| error!(?error, "Failed to create event"));
    }

    /// Callback hooks for the finalizers.
    pub(crate) async fn finalizer(&self) -> Result<Action, Error> {
        let _ = finalizer(
            &self.api(),
            &utils::constants::dsp_finalizer(),
            self.inner(),
            |event| async move {
                match event {
                    finalizer::Event::Apply(dsp) => Self::put_finalizer(dsp).await,
                    finalizer::Event::Cleanup(dsp) => {
                        // Check the fresh `dsp` (not `self`, which may be stale) for
                        // the openebs.io/delete-opts annotation.
                        //
                        // - No annotation (or purge not set): normal deletion — the
                        //   control plane contacts io-engine to destroy the pool on disk.
                        // - Annotation with purge=true: purge deletion — the control
                        //   plane removes the pool spec without contacting io-engine.
                        //
                        // If the annotation is present but malformed, this is a hard
                        // error: the user intended purge, and silently falling back to
                        // normal deletion could fail (503 for offline pool) or destroy
                        // data the user wanted to preserve via the accept flags.
                        // A K8s event is emitted so the user can see the error via
                        // `kubectl describe`.
                        let delete_body = match Self::parse_delete_opts(&dsp) {
                            Ok(body) => body,
                            Err(e) => {
                                let msg = format!(
                                    "Invalid {DELETE_OPTS_ANNOTATION} annotation: {e}. \
                                     Fix the annotation to proceed with deletion."
                                );
                                error!(pool.name = %self.name_any(), "{msg}");
                                self.k8s_notify("Destroy", "InvalidDeleteOpts", &msg, "Warning")
                                    .await;
                                return Err(e);
                            }
                        };

                        // If purge is requested, automatically cordon the pool
                        // for replicas and snapshots before proceeding. This
                        // removes the manual cordon step for the user.
                        let is_purge = delete_body.as_ref().and_then(|b| b.purge).unwrap_or(false);
                        if is_purge {
                            self.cordon_for_purge().await?;
                        }

                        if dsp.status.as_ref().map(|d| d.cr_state) != Some(CrPoolState::Terminating)
                        {
                            // Best-effort: if the pool is reachable, update the CR
                            // with its real state while terminating. If not, leave
                            // the current CR status as-is and proceed to delete.
                            if let Ok(pool) = self
                                .pools_api()
                                .get_node_pool(&self.spec.node(), &self.name_any())
                                .await
                            {
                                let _ = self
                                    .patch_status(DiskPoolStatus::terminating(
                                        &dsp,
                                        pool.into_body(),
                                    ))
                                    .await?;
                            }
                        }

                        // delete_pool handles all response codes:
                        //   - success (200/204): pool destroyed or purged
                        //   - 404: pool already gone ("AlreadyDestroyed")
                        //   - other errors: propagated, operator retries
                        Self::delete_finalizer(self.clone(), delete_body).await
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_opts_parsing() {
        // Normal deletes must not produce a body (server returns 204).
        assert!(parse_delete_opts_value("accept=true").unwrap().is_none());
        assert!(parse_delete_opts_value("purge=false,accept=true")
            .unwrap()
            .is_none());
        assert!(parse_delete_opts_value("").unwrap().is_none());
        assert!(parse_delete_opts_value("  ").unwrap().is_none());

        // purge=true must produce a body.
        let body = parse_delete_opts_value("purge=true").unwrap().unwrap();
        assert_eq!(body.purge, Some(true));
        assert_eq!(body.accept, None);

        // accept-data-loss fans out to both volume and snapshot loss.
        let body = parse_delete_opts_value("purge=true,accept-data-loss=true")
            .unwrap()
            .unwrap();
        assert_eq!(body.accept_volume_loss, Some(true));
        assert_eq!(body.accept_snapshot_loss, Some(true));

        // Latter option takes precedence over earlier shorthand.
        let body =
            parse_delete_opts_value("purge=true,accept-data-loss=true,accept-volume-loss=false")
                .unwrap()
                .unwrap();
        assert_eq!(body.accept_volume_loss, Some(false));
        assert_eq!(body.accept_snapshot_loss, Some(true));

        // purge-accept-all sets everything.
        let body = parse_delete_opts_value("purge-accept-all")
            .unwrap()
            .unwrap();
        assert_eq!(body.purge, Some(true));
        assert_eq!(body.accept, Some(true));
        assert_eq!(body.accept_volume_loss, Some(true));
        assert_eq!(body.accept_snapshot_loss, Some(true));

        // purge-accept-all=true also works.
        let body = parse_delete_opts_value("purge-accept-all=true")
            .unwrap()
            .unwrap();
        assert_eq!(body.purge, Some(true));

        // purge-accept-all can be overridden by later options.
        let body = parse_delete_opts_value("purge-accept-all,accept-volume-loss=false")
            .unwrap()
            .unwrap();
        assert_eq!(body.accept_volume_loss, Some(false));
        assert_eq!(body.accept_snapshot_loss, Some(true));

        // Bare key without = is treated as true.
        let body = parse_delete_opts_value("purge,accept").unwrap().unwrap();
        assert_eq!(body.purge, Some(true));
        assert_eq!(body.accept, Some(true));

        // Unknown key is an error.
        assert!(parse_delete_opts_value("purge=true,bogus=true").is_err());

        // Invalid value is an error.
        assert!(parse_delete_opts_value("purge=yes").is_err());
    }
}
