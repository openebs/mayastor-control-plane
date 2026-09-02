//! Definition of pool types that can be saved to the persistent store.

use crate::{
    types::v0::{
        openapi::models::{self, PoolSpecEncryption},
        store::{
            definitions::{ObjectKey, StorableObject, StorableObjectType},
            AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction,
        },
        transport::{
            self, CreatePool, ImportPool, NodeId, PoolDeviceUri, PoolDiag, PoolId, ReplicaId,
            VolumeId,
        },
    },
    IntoOption,
};

pub const POOL_BS_CLUSTER_SIZE_DEFAULT: u32 = 4194304;

pub fn default_pool_cluster_size() -> u32 {
    POOL_BS_CLUSTER_SIZE_DEFAULT
}

// PoolLabel is the type for the labels
pub type PoolLabel = HashMap<String, String>;

use pstor::ApiVersion;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::From,
    fmt::Debug,
    ops::{Deref, DerefMut},
    time::{Duration, SystemTime},
};

/// Pool data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Pool {
    /// Current state of the pool.
    pub state: Option<PoolState>,
    /// Desired pool specification.
    pub spec: Option<PoolSpec>,
}

/// Runtime state of the pool.
/// This should eventually satisfy the PoolSpec.
#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
pub struct PoolState {
    /// Pool information returned by the Io-Engine.
    pub pool: transport::PoolState,
}

impl From<transport::PoolState> for PoolState {
    fn from(pool: transport::PoolState) -> Self {
        Self { pool }
    }
}

/// Status of the Pool Spec
pub type PoolSpecStatus = SpecStatus<transport::PoolStatus>;

impl From<&CreatePool> for PoolSpec {
    fn from(request: &CreatePool) -> Self {
        Self {
            spec: PoolUSpec {
                node: request.node.clone(),
                id: request.id.clone(),
                disks: request.disks.clone(),
                status: PoolSpecStatus::Creating,
                labels: request.labels.clone(),
                sequencer: OperationSequence::new(),
                operation: None,
                creat_tsc: None,
                encryption: request.encryption.clone(),
                cordon_drain: None,
                // Default is 4MiB today.
                cluster_size: request.cluster_size.unwrap_or(POOL_BS_CLUSTER_SIZE_DEFAULT),
                max_expansion: request.max_expansion.clone(),
            },
            metadata: PoolMetadata {
                runtime: PoolRuntimeMetadata {
                    snapshot_count: Some(0),
                    replica_count: Some(0),
                    ..Default::default()
                },
                ..Default::default()
            },
        }
    }
}
impl From<&PoolSpec> for CreatePool {
    fn from(pool: &PoolSpec) -> Self {
        Self {
            node: pool.node.clone(),
            id: pool.id.clone(),
            disks: pool.disks.clone(),
            labels: pool.labels.clone(),
            encryption: pool.encryption.clone(),
            cluster_size: Some(pool.cluster_size),
            max_expansion: pool.max_expansion.clone(),
        }
    }
}

impl PartialEq<CreatePool> for PoolSpec {
    fn eq(&self, other: &CreatePool) -> bool {
        let mut other = PoolSpec::from(other);
        other.status = self.status.clone();
        other.sequencer = self.sequencer.clone();
        other.creat_tsc = self.creat_tsc;
        other.metadata = self.metadata.clone();
        &other == self
    }
}

/// Encryption parameters.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum Encryption {
    /// Name of the secret or file to parse the encryption parameters.
    Secret(EncryptionSecret),
}

/// Encryption secret.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct EncryptionSecret {
    /// Name of the secret.
    pub name: String,
}

/// User specification of a pool.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PoolSpec {
    #[serde(flatten)]
    pub spec: PoolUSpec,
    /// Pool metadata information.
    #[serde(default, skip_serializing_if = "super::is_default")]
    pub metadata: PoolMetadata,
}

impl Deref for PoolSpec {
    type Target = PoolUSpec;
    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}
impl DerefMut for PoolSpec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
}

/// User specification of a pool.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PoolUSpec {
    /// id of the io-engine instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
    /// status of the pool
    pub status: PoolSpecStatus,
    /// labels to be set on the pool
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<PoolLabel>,
    /// Update in progress
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Record of the operation in progress
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<PoolOperationState>,
    /// Last modification timestamp.
    #[serde(skip)]
    pub creat_tsc: Option<std::time::SystemTime>,
    /// Use to create/import encrypted pool
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<Encryption>,
    /// Cordon/drain state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cordon_drain: Option<CordonDrainState>,
    /// Blobstore cluster size used for this pool.
    #[serde(default = "default_pool_cluster_size")]
    pub cluster_size: u32,
    /// Maximum expansion size for this pool.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_expansion: Option<String>,
}

/// Pool meta information.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PoolMetadata {
    /// Persisted metadata information.
    #[serde(default, skip_serializing_if = "super::is_default")]
    pub persisted: PoolPersistedMetadata,
    /// Runtime information, useful to quick checks without having to read out from PSTOR
    /// or any other control-plane related registry.
    #[serde(skip)]
    pub runtime: PoolRuntimeMetadata,
}

/// Pool meta information.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PoolPersistedMetadata {
    /// Populated when drain request is submitted.
    /// Stores states driving the drain procedure.
    #[serde(default, skip_serializing_if = "super::is_default")]
    pub drain_record: Option<PoolDrainRecord>,
}

/// Record of an in-progress pool drain, driving the drain procedure and tracking its progress.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolDrainRecord {
    /// Current phase of the drain state machine.
    pub phase: DrainPhase,
    /// Why pool is in the aforementioned phase.
    pub phase_reason: Option<PhaseReason>,
    /// The initial usage stats of the pool when the pool transitions into Draining state.
    pub initial_stats: Option<PoolUsage>,
    /// In-flight replica moves for this drain.
    pub replica_moves: Vec<DrainConfig>,
}

impl Default for PoolDrainRecord {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolDrainRecord {
    fn new() -> Self {
        Self {
            phase: DrainPhase::Queued,
            phase_reason: Some(PhaseReason::WaitingForSlot),
            initial_stats: None,
            replica_moves: vec![],
        }
    }

    /// Returns the current phase of the drain state machine.
    pub fn phase(&self) -> &DrainPhase {
        &self.phase
    }

    /// Returns the reason for the current phase of the drain state machine.
    pub fn phase_reason(&self) -> &Option<PhaseReason> {
        &self.phase_reason
    }

    /// Returns the initial usage stats of the pool when the pool transitions into Draining state.
    pub fn initial_stats(&self) -> &Option<PoolUsage> {
        &self.initial_stats
    }

    /// Returns the in-flight replica moves for this drain.
    pub fn replica_moves(&self) -> &Vec<DrainConfig> {
        &self.replica_moves
    }
}

/// Why the pool is in its current drain phase.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PhaseReason {
    /// The pool is waiting for a slot to start draining, as the number of concurrent drains is
    /// capped cluster-wide.
    WaitingForSlot,
    /// The pool is degraded and cannot safely drain without risking data loss.
    OfflinePool,
    /// The pool has a single replica that cannot be safely evicted without risking data loss.
    SingleReplicaUnsafeEviction,
    /// The node containing pool is back but pool is cordoned for import, so it cannot be drained.
    ImportCordoned,
    /// Snapshots are left behind due to user chosen drain policy.
    SnapshotsRetained,
}

/// The phase of the pool drain state machine.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DrainPhase {
    /// Could not determine the actual drain state.
    Unknown,
    /// The drain has been admitted and the pool is self-cordoned, but no replica has been moved
    /// yet, the pool is waiting for a concurrency slot.
    Queued,
    /// Replicas are actively being evacuated.
    Draining,
    /// All replicas are evacuated from this pool from the volume's perspective, but replicas are
    /// pending cleanup on the pool itself.
    AwaitingCleanup,
    /// Transitioned from Draining when we can't make any progress on drain operation. Awaiting on
    /// state change or user intervention to continue.
    PartiallyDrained,
    /// The pool has reached zero allocation/commitment: all replicas were evacuated and no
    /// snapshots are left behind.
    Drained,
    /// The user has cancelled the drain and the control plane is undoing the changes the drain
    /// made, unwinding in-flight moves and their spare replicas.
    Aborted,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PoolUsage {
    /// Number of replicas present on pool.
    pub repl_count: u64,
    /// Number of snapshots present on pool.
    pub snap_count: u64,
    /// Used capacity, in bytes.
    pub used: u64,
    /// Committed capacity, in bytes.
    /// `None` when the pool state does not report a commitment.
    pub committed: Option<u64>,
}

/// Configuration of a single replica move carried out as part of a pool drain.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DrainConfig {
    /// When this move first attempted to place its spare.
    pub placement_started_at: Option<SystemTime>,
    /// Id of the volume draining_replica belongs to.
    pub volume: VolumeId,
    /// Id of the pool draining_replica belongs to.
    pub pool: PoolId,
    /// Id of the draining replica. It's None when it's evacuated successfully.
    pub draining_replica: Option<ReplicaId>,
    /// Id of the spare replica created as part of drain procedure.
    pub spare_replica: Option<SpareReplica>,
    /// Why a move's over-replicated spare is being removed.
    #[serde(skip)]
    pub unwind_spare: Option<UnwindSpare>,
}

/// Spare replica reference for this drain.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct SpareReplica {
    /// Replica Id of the spare, if placed.
    pub replica_id: Option<ReplicaId>,
}

/// Contains the reason for unwinding a spare replica, which is used to determine how to proceed with
/// the drain procedure.
#[derive(Debug, Clone, PartialEq)]
pub enum UnwindSpare {
    /// The drain was aborted (`DrainPhase → Aborted`): remove the spare and end the
    /// move, keeping `draining_replica` where it is.
    Abort,
    /// The pool hosting the still-rebuilding spare has itself entered a drain: remove
    /// the spare and let this move place a fresh one on another eligible pool.
    Respare,
}
/// Runtime pool information.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PoolRuntimeMetadata {
    /// Diagnostic info for the pool.
    pub diag: Option<PoolDiag>,
    /// How many replica volumes owned by the pool.
    /// This value is re-generated from the replicas, and then keep track via create/destroy.
    /// Note that this count may differ from expected, as it tracks resources in
    /// created and deleting states
    pub replica_count: Option<u64>,
    /// How many replica snapshots owned by the pool.
    /// This value is re-generated from the volume-snapshots, and then keep track via create/destroy.
    /// Note that this count may differ from expected, as it tracks resources in
    /// created and deleting states.
    pub snapshot_count: Option<u64>,
}

impl PoolSpec {
    /// Pool identification.
    pub fn id(&self) -> &PoolId {
        &self.id
    }

    /// Label pool by applying the labels.
    pub fn label(&mut self, labels: HashMap<String, String>) {
        match &mut self.labels {
            Some(existing_labels) => {
                existing_labels.extend(labels);
            }
            None => {
                self.labels = Some(labels);
            }
        }
    }

    /// Check if the pool has the given topology label key.
    pub fn has_labels_key(&self, key: &str) -> bool {
        if let Some(labels) = &self.labels {
            return labels.contains_key(key);
        }
        false
    }

    /// Remove label from pool.
    pub fn unlabel(&mut self, label_key: &str) {
        if let Some(labels) = &mut self.labels {
            labels.remove(label_key);
        }
    }

    /// Check if there are key collisions between current topology labels and the given labels.
    pub fn label_collisions<'a>(
        &'a self,
        labels: &'a HashMap<String, String>,
    ) -> (HashMap<&'a String, &'a String>, bool) {
        let mut conflict = false;
        let mut existing_conflicts = HashMap::new();

        if let Some(existing_labels) = &self.labels {
            for (key, value) in labels {
                if let Some(existing_value) = existing_labels.get(key) {
                    if existing_value != value {
                        conflict = true;
                        existing_conflicts.insert(key, existing_value);
                    }
                }
            }
        }

        (existing_conflicts, conflict)
    }

    /// Ensure the state is consistent.
    pub fn resolve(&mut self) {
        if let Some(ds) = &mut self.cordon_drain {
            match ds {
                CordonDrainState::Cordoned(state) => {
                    if !state.cordoned() {
                        self.cordon_drain = None;
                    }
                }
                CordonDrainState::Drain(spec) => {
                    if let Some(uc) = &spec.user_cordon {
                        if !uc.cordoned() {
                            spec.user_cordon = None;
                        }
                    }
                }
            }
        }
    }

    /// Cordon the pool.
    pub fn cordon(&mut self, op: PoolCordonOp) {
        match &mut self.cordon_drain {
            Some(ds) => {
                ds.add_cordon(op);
            }
            None => {
                self.cordon_drain = Some(CordonDrainState::Cordoned(CordonedState::from(op)));
            }
        }
        self.resolve();
    }

    /// Uncordon the pool.
    pub fn uncordon(&mut self, op: PoolCordonOp) {
        if let Some(ds) = &mut self.cordon_drain {
            ds.rm_cordon(op);
        }
        self.resolve();
    }

    /// Returns whether the pool is cordoned and its state.
    pub fn cordoned(&self) -> Option<CordonedState> {
        self.effective_cordon()
    }

    /// Check if the pool is cordoned for imports, as per the effective cordon policy.
    pub fn cordoned_imports(&self) -> bool {
        self.cordoned().map(|s| s.import).unwrap_or_default()
    }

    /// Returns true if all labels are already present.
    pub fn cordon_would_modify(&self, op: &PoolCordonOp) -> bool {
        match &self.cordon_drain {
            Some(ds) => ds.would_modify(op, true),
            None => true,
        }
    }

    /// Returns true if all labels are already present.
    pub fn uncordon_would_modify(&self, op: &PoolCordonOp) -> bool {
        match &self.cordon_drain {
            Some(ds) => ds.would_modify(op, false),
            None => false,
        }
    }

    /// Retuns true if drain is allowed.
    pub fn drain_allowed(&self, op: &PoolDrainOp) -> bool {
        match &self.cordon_drain {
            Some(ds) => {
                if let Some(record) = self.drain_record() {
                    // No point on updating drain config on an already Drained pool.
                    if record.phase == DrainPhase::Drained {
                        false
                    } else {
                        ds.drain_allowed(op)
                    }
                } else {
                    true
                }
            }
            None => true,
        }
    }

    /// May retry the pool import.
    /// Otherwise, try again next time.
    pub fn can_retry_import(&self) -> bool {
        let Some(diag) = &self.metadata.runtime.diag else {
            return true;
        };
        diag.import.retriable()
    }

    /// Sets drain configuration on the pool, carrying over the user's own cordon, if any.
    /// Existing drain record is preserved, if any, to keep track of the drain progress.
    /// If no drain record exists, a new one is created.
    pub fn set_drain(&mut self, op: PoolDrainOp) {
        let (user_cordon, request_ts) = match self.cordon_drain.as_ref() {
            Some(CordonDrainState::Drain(drain)) => {
                (drain.user_cordon.clone(), Some(drain.request_timestamp))
            }
            Some(CordonDrainState::Cordoned(cordoned)) => (Some(cordoned.clone()), None),
            None => (None, None),
        };
        let drain_spec = DrainSpec::new(op.policy, user_cordon, request_ts);
        self.cordon_drain = Some(CordonDrainState::Drain(drain_spec));
        if self.metadata.persisted.drain_record.is_none() {
            let drain_record = PoolDrainRecord::default();
            self.metadata.persisted.drain_record = Some(drain_record);
        }
    }

    /// Removes drain configuration from the pool, restoring the user's own cordon, if they had
    /// one set before the drain.
    pub fn abort_drain(&mut self) {
        if let Some(CordonDrainState::Drain(drain)) = &self.cordon_drain {
            self.cordon_drain = drain
                .user_cordon
                .as_ref()
                .map(|uc| CordonDrainState::Cordoned(uc.clone()));
        }
        self.metadata.persisted.drain_record = None
    }

    /// Returns the applied drain configuration on the pool.
    pub fn drain_policy(&self) -> Option<&DrainPolicy> {
        match self.cordon_drain.as_ref()? {
            CordonDrainState::Drain(drain) => Some(&drain.policy),
            CordonDrainState::Cordoned(_) => None,
        }
    }

    /// Returns the cordon policy in effect: consider self-cordon while a drain is applied, otherwise
    /// the cordon the user applied.
    pub fn effective_cordon(&self) -> Option<CordonedState> {
        self.cordon_drain.as_ref().map(|s| s.effective_cordon())
    }

    /// Returns the drain record of the pool, if a drain has been admitted.
    pub fn drain_record(&self) -> Option<&PoolDrainRecord> {
        self.metadata.persisted.drain_record.as_ref()
    }
}

impl From<&PoolSpec> for ImportPool {
    fn from(value: &PoolSpec) -> Self {
        Self {
            node: value.node.clone(),
            id: value.id.clone(),
            disks: value.disks.clone(),
            uuid: None,
            encryption: value.encryption.clone(),
        }
    }
}

impl AsOperationSequencer for PoolSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl From<PoolSpec> for models::PoolSpec {
    fn from(src: PoolSpec) -> Self {
        let spec = src.spec;
        Self::from(spec)
    }
}

impl From<PoolUSpec> for models::PoolSpec {
    fn from(src: PoolUSpec) -> Self {
        let encryption = match src.encryption {
            None => None,
            Some(encr) => match encr {
                Encryption::Secret(details) => Some(PoolSpecEncryption::secret(
                    openapi::models::EncryptionSecret { name: details.name },
                )),
            },
        };
        Self::new_all(
            src.disks,
            src.id,
            src.labels,
            src.node,
            src.status,
            encryption,
            src.cordon_drain.into_opt(),
            Some(src.cluster_size as i64),
            src.max_expansion,
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolOperationState {
    /// Record of the operation
    pub operation: PoolOperation,
    /// Result of the operation
    pub result: Option<bool>,
}

impl SpecTransaction<PoolOperation> for PoolSpec {
    fn has_pending_op(&self) -> bool {
        self.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.operation.clone() {
            match op.operation {
                PoolOperation::Destroy => {
                    self.status = SpecStatus::Deleted;
                }
                PoolOperation::Create => {
                    self.status = SpecStatus::Created(transport::PoolStatus::Online);
                    self.metadata.runtime.diag = None;
                }
                PoolOperation::Label(PoolLabelOp { labels, .. }) => {
                    self.label(labels);
                }
                PoolOperation::Unlabel(PoolUnLabelOp { label_key }) => {
                    self.unlabel(&label_key);
                }
                PoolOperation::Cordon(op) => {
                    self.cordon(op);
                }
                PoolOperation::Uncordon(op) => {
                    self.uncordon(op);
                }
                PoolOperation::Import(_) => {
                    self.metadata.runtime.diag = None;
                }
                PoolOperation::Drain(op) => {
                    self.set_drain(op);
                }
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        let Some(op) = self.operation.take() else {
            return;
        };
        if let PoolOperation::Import(op) = &op.operation {
            if let Some(h) = op.report.lock().expect("not poisoned").take() {
                self.metadata.runtime.diag = Some(h);
            }
        }
    }

    fn start_op(&mut self, operation: PoolOperation) {
        if matches!(operation, PoolOperation::Create) && self.creat_tsc.is_none() {
            self.creat_tsc = Some(std::time::SystemTime::now());
        }
        self.operation = Some(PoolOperationState {
            operation,
            result: None,
        })
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.operation {
            op.result = Some(result);
        }
    }

    fn pending_op(&self) -> Option<&PoolOperation> {
        self.operation.as_ref().map(|o| &o.operation)
    }

    fn log_op(&self, operation: &PoolOperation) -> (bool, bool) {
        match operation {
            PoolOperation::Create => (true, true),
            PoolOperation::Destroy => (true, true),
            PoolOperation::Label(_) => (false, true),
            PoolOperation::Unlabel(_) => (false, true),
            PoolOperation::Cordon(_) => (false, true),
            PoolOperation::Uncordon(_) => (false, true),
            PoolOperation::Import(_) => (false, false),
            PoolOperation::Drain(_) => (false, true),
        }
    }
}

/// Available Pool Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PoolOperation {
    Create,
    Destroy,
    Label(PoolLabelOp),
    Unlabel(PoolUnLabelOp),
    Cordon(PoolCordonOp),
    Uncordon(PoolCordonOp),
    Import(PoolImportOp),
    Drain(PoolDrainOp),
}

/// Pool importing info.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PoolImportOp {
    /// The report which is used to retrieve the pool diagnostic info.
    #[serde(skip)]
    pub report: std::sync::Arc<std::sync::Mutex<Option<PoolDiag>>>,
}
impl PartialEq for PoolImportOp {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

/// Parameter for adding/removing pool cordons.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolCordonOp {
    /// No new replicas can be created on this pool
    pub replicas: bool,
    /// No new snapshots can be created on this pool
    pub snapshots: bool,
    /// No new restores can be created on this pool
    pub restores: bool,
    /// Pool cannot be imported after node/engine restart.
    pub import: bool,
}
impl PoolCordonOp {
    fn resource(yes: bool, name: &str) -> &str {
        if yes {
            name
        } else {
            ""
        }
    }
    /// Convert cordon resources to a comma separated string.
    pub fn resources(&self) -> String {
        [
            Self::resource(self.replicas, "replicas"),
            Self::resource(self.snapshots, "snapshots"),
            Self::resource(self.restores, "restores"),
            Self::resource(self.import, "import"),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(",")
    }
}

/// Parameter for draining a pool.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolDrainOp {
    /// Drain policy to be applied to the pool.
    pub policy: DrainPolicy,
}

/// Parameter for adding pool labels.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolLabelOp {
    pub labels: HashMap<String, String>,
    pub overwrite: bool,
}
impl From<(HashMap<String, String>, bool)> for PoolLabelOp {
    fn from((labels, overwrite): (HashMap<String, String>, bool)) -> Self {
        Self { labels, overwrite }
    }
}
/// Parameter for removing pool labels.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolUnLabelOp {
    pub label_key: String,
}
impl From<String> for PoolUnLabelOp {
    fn from(label_key: String) -> Self {
        Self { label_key }
    }
}

impl PartialEq<transport::PoolState> for PoolSpec {
    fn eq(&self, other: &transport::PoolState) -> bool {
        self.node == other.node
    }
}

/// Key used by the store to uniquely identify a PoolSpec structure.
pub struct PoolSpecKey(PoolId);

impl From<&PoolId> for PoolSpecKey {
    fn from(id: &PoolId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for PoolSpecKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::PoolSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for PoolSpec {
    type Key = PoolSpecKey;

    fn key(&self) -> Self::Key {
        PoolSpecKey(self.id.clone())
    }
}

impl From<&PoolSpec> for transport::PoolState {
    fn from(pool: &PoolSpec) -> Self {
        Self {
            node: pool.node.clone(),
            id: pool.id.clone(),
            uuid: None,
            disks: pool.disks.clone(),
            status: transport::PoolStatus::Unknown,
            capacity: 0,
            used: 0,
            committed: None,
            encrypted: pool.encryption.is_some(),
            cluster_size: pool.cluster_size,
            disk_capacity: None,
            max_expandable_size: None,
            disk_info: vec![],
            errors: None,
            repl_count: pool.metadata.runtime.replica_count,
            snap_count: pool.metadata.runtime.snapshot_count,
        }
    }
}

impl From<EncryptionSecret> for models::EncryptionSecret {
    fn from(value: EncryptionSecret) -> Self {
        Self { name: value.name }
    }
}

impl From<models::Encryption> for Encryption {
    fn from(value: models::Encryption) -> Self {
        match value {
            models::Encryption::secret(secret_name) => Self::Secret(secret_name.into()),
        }
    }
}

/// Data relating to the cordoning of a pool.
#[derive(Clone, Default, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CordonedState {
    // todo: or should these be negated, ie: by default all blocked?
    /// No new replicas can be created on this pool.
    pub replicas: bool,
    /// No new snapshots can be created on this pool.
    pub snapshots: bool,
    /// No new restores can be created on this pool.
    pub restores: bool,
    /// Pool cannot be imported after node/engine restart.
    pub import: bool,
}
impl CordonedState {
    fn cordoned(&self) -> bool {
        self.replicas || self.snapshots || self.restores || self.import
    }

    /// A cordoned state that is self-cordoned for all operations except import.
    pub const SELF_CORDON: Self = Self {
        replicas: true,
        snapshots: true,
        restores: true,
        import: false,
    };
}

impl From<PoolCordonOp> for CordonedState {
    fn from(value: PoolCordonOp) -> Self {
        Self {
            replicas: value.replicas,
            snapshots: value.snapshots,
            restores: value.restores,
            import: value.import,
        }
    }
}

impl CordonedState {
    fn set_if(ifset: bool, set: &mut bool, val: bool) {
        if ifset {
            *set = val;
        }
    }
    /// Add cordon resources.
    pub fn add_cordon(&mut self, op: PoolCordonOp) {
        self.op_cordon(op, true);
    }
    /// Remove cordon resources.
    pub fn rm_cordon(&mut self, op: PoolCordonOp) {
        self.op_cordon(op, false);
    }
    fn op_cordon(&mut self, op: PoolCordonOp, cordon: bool) {
        Self::set_if(op.replicas, &mut self.replicas, cordon);
        Self::set_if(op.snapshots, &mut self.snapshots, cordon);
        Self::set_if(op.restores, &mut self.restores, cordon);
        Self::set_if(op.import, &mut self.import, cordon);
    }
    fn if_modify(current: bool, op: bool, cordon: bool) -> bool {
        if cordon {
            !current && op
        } else {
            current && op
        }
    }
    /// Returns whether the operation would yield changes.
    pub fn would_modify(&self, op: &PoolCordonOp, cordon: bool) -> bool {
        Self::if_modify(self.replicas, op.replicas, cordon)
            || Self::if_modify(self.snapshots, op.snapshots, cordon)
            || Self::if_modify(self.restores, op.restores, cordon)
            || Self::if_modify(self.import, op.import, cordon)
    }
}

/// Enum variant encompassing data related to a cordoned or draining pool.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum CordonDrainState {
    /// The pool is being cordoned.
    Cordoned(CordonedState),
    /// The pool is being drained.
    Drain(DrainSpec),
}

impl CordonDrainState {
    /// Update cordon with the given options.
    pub fn add_cordon(&mut self, cordon: PoolCordonOp) {
        match self {
            CordonDrainState::Cordoned(state) => {
                state.add_cordon(cordon);
            }
            CordonDrainState::Drain(state) => {
                if let Some(uc) = state.user_cordon.as_mut() {
                    uc.add_cordon(cordon);
                }
            }
        }
    }

    /// Update cordon with the given options.
    pub fn rm_cordon(&mut self, cordon: PoolCordonOp) {
        match self {
            CordonDrainState::Cordoned(state) => {
                state.rm_cordon(cordon);
            }
            CordonDrainState::Drain(spec) => {
                if let Some(uc) = spec.user_cordon.as_mut() {
                    uc.rm_cordon(cordon);
                }
            }
        }
    }
    /// Returns the cordon policy in effect.
    /// A draining pool is always self-cordoned for replicas, snapshots and restores, so only the
    /// import cordon is taken from the user's own cordon, if they had one set before the drain.
    pub fn effective_cordon(&self) -> CordonedState {
        match self {
            CordonDrainState::Cordoned(cordoned) => cordoned.clone(),
            CordonDrainState::Drain(spec) => CordonedState {
                import: spec.user_cordon.as_ref().is_some_and(|uc| uc.import),
                ..CordonedState::SELF_CORDON
            },
        }
    }
    /// Returns whether the state has all the specified cordon labels.
    pub fn would_modify(&self, op: &PoolCordonOp, cordon: bool) -> bool {
        match self {
            CordonDrainState::Cordoned(state) => state.would_modify(op, cordon),
            CordonDrainState::Drain(spec) => {
                if let Some(uc) = &spec.user_cordon {
                    uc.would_modify(op, cordon)
                } else {
                    cordon
                }
            }
        }
    }

    /// Update of already existing is only allowed to change snapshot policy. unsafe_evict or
    /// unsafe_rebuild_otherwise_evict should not be updated.
    pub fn drain_allowed(&self, op: &PoolDrainOp) -> bool {
        match self {
            CordonDrainState::Cordoned(_) => true,
            CordonDrainState::Drain(spec) => {
                let applied_policy = &spec.policy;
                let request_policy = &op.policy;
                applied_policy.unsafe_evict == request_policy.unsafe_evict
                    && applied_policy.unsafe_rebuild_otherwise_evict
                        == request_policy.unsafe_rebuild_otherwise_evict
            }
        }
    }
}

impl From<CordonDrainState> for models::PoolCordonDrain {
    fn from(pool_ds: CordonDrainState) -> Self {
        let state = pool_ds.effective_cordon();
        // TODO: We don't currently report the drain policy, will be added in a future PR.
        // This mapping will change when Drain is added to openapi.
        Self::cordoned(models::PoolCordon {
            replicas: state.replicas,
            snapshots: state.snapshots,
            restores: state.restores,
            import: state.import,
        })
    }
}

/// Drain spec applied on the pool.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DrainSpec {
    /// Timestamp when the drain was requested.
    pub request_timestamp: SystemTime,
    /// Drain policy applied on the pool.
    pub policy: DrainPolicy,
    /// Holds user applied cordon configs if present before starting drain.
    pub user_cordon: Option<CordonedState>,
}

impl DrainSpec {
    /// Create a new drain spec with the given policy.
    pub fn new(
        policy: DrainPolicy,
        user_cordon: Option<CordonedState>,
        req_tsc: Option<SystemTime>,
    ) -> Self {
        Self {
            request_timestamp: req_tsc.unwrap_or(SystemTime::now()),
            policy,
            user_cordon,
        }
    }
}

/// The user's drain policy, each of which alters what the drain is permitted to do.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct DrainPolicy {
    /// What to do with the snapshots left on the pool once all replicas are evacuated.
    /// Defaults to `Ignore`, which leaves them in place.
    pub snapshot_policy: SnapshotPolicy,
    /// Grace period after which an unplaceable replica is force-evicted.
    /// `None` disables forced eviction entirely.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsafe_rebuild_otherwise_evict: Option<Duration>,
    /// Skip the safe over-replicate flow and evict replicas directly.
    pub unsafe_evict: bool,
}

impl DrainPolicy {
    /// Create a new drain policy with the given parameters.
    pub fn new(
        snapshot_policy: SnapshotPolicy,
        unsafe_rebuild_otherwise_evict: Option<Duration>,
        unsafe_evict: bool,
    ) -> Self {
        Self {
            snapshot_policy,
            unsafe_rebuild_otherwise_evict,
            unsafe_evict,
        }
    }
}

/// What a drain does with the snapshots left on the pool once all replicas are evacuated.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub enum SnapshotPolicy {
    /// Move the replicas only, leaving the snapshots in place.
    #[default]
    Ignore,
    /// Destroy the snapshots left on the pool once all replicas are evacuated, letting the
    /// pool reach `Drained`.
    AcceptLoss,
}
