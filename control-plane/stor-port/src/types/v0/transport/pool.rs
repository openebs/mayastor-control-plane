use super::*;

use crate::{
    types::v0::store::pool::{
        Encryption, EncryptionSecret, PoolLabel, PoolPersistedMetadata, PoolRuntimeMetadata,
        PoolSpec, PoolUSpec, PoolUsage,
    },
    IntoOption,
};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap, fmt::Debug, ops::Deref};
use strum_macros::{Display, EnumString};

/// Pool Service
/// Get all the pools from specific node or None for all nodes.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetPools {
    /// Filter request
    pub filter: Filter,
}

/// Status of the Pool.
#[derive(Serialize, Deserialize, Debug, Default, Clone, EnumString, Display, Eq, PartialEq)]
pub enum PoolStatus {
    /// Unknown state.
    #[default]
    Unknown,
    /// The pool is in normal working order.
    Online,
    /// The pool has experienced a failure but can still function.
    Degraded,
    /// The pool is completely inaccessible.
    Faulted,
    /// Pool has at least an alert level of warning.
    Suspected,
    /// The pool is offline.
    Offline,
}

// todo: this conversion is bypassing the io-engine proto-api translation.
//  this may cause issues if the numbers here get desynced with the io-engine api.
impl From<i32> for PoolStatus {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            4 => Self::Suspected,
            5 => Self::Offline,
            _ => Self::Unknown,
        }
    }
}
impl From<PoolStatus> for models::PoolStatus {
    fn from(src: PoolStatus) -> Self {
        match src {
            PoolStatus::Unknown => Self::Unknown,
            PoolStatus::Offline => Self::Offline,
            PoolStatus::Online => Self::Online,
            PoolStatus::Degraded => Self::Degraded,
            PoolStatus::Faulted => Self::Faulted,
            PoolStatus::Suspected => Self::Suspected,
        }
    }
}

/// Control-Plane Pool state information.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CtrlPoolState {
    /// The state, mostly as returned by the data-plane.
    pub state: PoolState,
}
impl CtrlPoolState {
    /// Construct a new pool with spec and state.
    pub fn new(state: PoolState) -> Self {
        Self { state }
    }
    /// Get the pool state.
    pub fn state(&self) -> &PoolState {
        &self.state
    }
}
impl Deref for CtrlPoolState {
    type Target = PoolState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

/// Different pool errors
#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy, PartialEq)]
pub enum PoolErrorCode {
    /// Unknown error.
    #[default]
    Unknown,
    /// Disk not found in the system.
    DiskNotFound,
    /// Disk read IO errors.
    DiskReadIoError,
    /// Pool on-disk name doesn't match the expected.
    ForeignPoolName,
    /// Pool on-disk uuid doesn't match the expected.
    ForeignPoolUid,
    /// Failed to check super block error.
    SuperBlockIoError,
    /// Invalid super block (eg: CRC error).
    InvalidSuperBlock,
    /// Disk is a directory (can happen when setting up incorrect volume mounts).
    DiskIsADirectory,
    /// Node is in unknown state.
    NodeIsUnknown,
    /// Node is offline.
    NodeIsOffline,
    /// Import is disable due to cordoning.
    ImportDisabled,
    /// gRPC to the pool timed out.
    TimeOut,
    /// gRPC aborted because it took too long.
    Aborted,
    // If the Disk is already claimed by something.
    // This may happen if the disk is used by another pool for example.
    DiskClaimed,
    // PCI driver not supported.
    PCIDriverUnsupported,
    // PCI still bound to kernel nvme driver.
    PCIKernelBound,
    // PCI BDF exists but it's not an NVMe device.
    PCINotNvme,
    // The disk URI is invalid or not supported.
    InvalidDiskUri,
    // Disk is not importable (ie malloc).
    DiskNotImportable,
    // Probing is not implemented for this URI.
    UriNotHandled,
}

/// Pool error code and human-readable message.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct PoolError {
    /// Code of the encountered error.
    pub code: PoolErrorCode,
    /// Human-readable message.
    pub msg: Option<String>,
}

/// Pool Disk Errors.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct PoolDiskError {
    /// Affected Disk.
    pub disk: String,
    /// Error encountered.
    pub error: PoolError,
}

/// Pool diagnostic information.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PoolDiag {
    /// Errors encountered when trying to import the pool.
    pub import_errors: Vec<PoolDiskError>,
    /// Information about import timings.
    #[serde(skip)]
    pub import: ImportBackoff,
    /// Inferred error of the pool, if any.
    #[serde(skip)]
    pub error: Option<PoolError>,
    /// Inferred status of the pool.
    #[serde(skip)]
    pub status: PoolStatus,
}
impl PoolDiag {
    /// Add the status to the diagnostic information.
    pub fn with_state(self, state: &PoolState) -> PoolDiag {
        Self {
            status: state.status.clone(),
            ..self
        }
    }
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.msg {
            None => write!(f, "{:?}", self.code),
            Some(msg) => write!(f, "{msg}: {:?}", self.code),
        }
    }
}
impl std::fmt::Display for PoolDiag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.error {
            None => Ok(()),
            Some(error) => write!(f, "{error}"),
        }
    }
}

/// Pool import backoff.
/// Used to avoid thrashing the logs.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ImportBackoff {
    pub retries: u64,
    pub next_retry: Option<std::time::SystemTime>,
}
impl ImportBackoff {
    /// We may retry the import at this time.
    pub fn retriable(&self) -> bool {
        let Some(next_retry) = &self.next_retry else {
            return true;
        };
        next_retry <= &std::time::SystemTime::now()
    }
    /// Create a new ImportBackoff.
    pub fn new(old: &PoolRuntimeMetadata, gc_period: std::time::Duration) -> Self {
        const MAX_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(60 * 60);

        let retries = match &old.diag {
            Some(diag) => diag.import.retries + 1,
            None => 0,
        };

        let cooldown = if retries < 5 {
            gc_period
        } else if retries < 10 {
            gc_period * retries as u32
        } else {
            gc_period * 10 * (retries - 9) as u32
        }
        .min(MAX_COOLDOWN);

        Self {
            retries,
            next_retry: std::time::SystemTime::now().checked_add(cooldown),
        }
    }
}

/// Pool state information - as reported by the io-engine.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PoolState {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub id: PoolId,
    /// Uuid of the pool.
    pub uuid: Option<PoolUuid>,
    /// Absolute disk paths claimed by the pool.
    pub disks: Vec<PoolDeviceUri>,
    /// Current state of the pool.
    pub status: PoolStatus,
    /// Size of the pool in bytes.
    pub capacity: u64,
    /// Used bytes from the pool.
    pub used: u64,
    /// Total pool commitment (in bytes) which is basically the accrued size of pool replicas.
    pub committed: Option<u64>,
    /// Is the pool encrypted.
    #[serde(default)]
    pub encrypted: bool,
    /// Blobstore cluster size used for this pool.
    #[serde(default = "crate::types::v0::store::pool::default_pool_cluster_size")]
    pub cluster_size: u32,
    /// Size of the underlying disk, in bytes.
    pub disk_capacity: Option<u64>,
    /// Maximum disk_capacity this pool can be expanded to, in bytes.
    pub max_expandable_size: Option<u64>,
    /// Information for each pool disk.
    pub disk_info: Vec<DiskInfo>,
    /// Error information at the pool top-level.
    pub errors: Option<PoolErrorInfo>,
    /// How many replicas exist in the pool.
    pub repl_count: Option<u64>,
    /// How many replica-snapshots exist in the pool.
    pub snap_count: Option<u64>,
}

/// Pool information related to a specific disk.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct DiskInfo {
    /// The disk uri used to open the disk
    pub uri: String,
    /// Errors seen for this disk
    pub errors: PoolErrorInfo,
}

/// Alerts and error information for a pool.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct PoolErrorInfo {
    /// These are generated from the below metrics
    pub alerts: PoolAlerts,
    /// Count of all disk errors since the pool has been opened.
    pub io_error_count: u64,
    /// After this many errors a pool alert is raised as Warning.
    pub io_error_threshold: u64,
    /// The I/O is stalled.
    pub io_stalled: bool,
    /// Number of stall transitions.
    pub io_stall_transition_count: u64,
    /// After this many transitions within the window, a pool alert is raised as Warning.
    pub io_stall_transition_threshold: u64,
}

/// Pool alerts and inferred status.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct PoolAlerts {
    /// The [`PoolAlertStatus`] of the pool.
    pub status: PoolAlertStatus,
    /// No status alerts are raised.
    pub notice: Vec<PoolAlert>,
    /// Raises an Attention level, but pool state is left as is.
    pub attention: Vec<PoolAlert>,
    /// Warnings downgrades the pool state to Suspected.
    pub warning: Vec<PoolAlert>,
    /// Pools with critical alerts should not be used.
    pub critical: Vec<PoolAlert>,
}

/// Alert-based status for the DiskPool.
#[derive(Serialize, Deserialize, Debug, Default, Clone, EnumString, Display, Eq, PartialEq)]
pub enum PoolAlertStatus {
    /// No issues detected; pool is operating normally.
    Healthy,
    /// Non‑critical issues present; user attention is recommended.
    Attention,
    /// Conditions have exceeded warning thresholds (fixed or based on trends over a defined time window).
    Warning,
    /// Severe issues detected; pool usage should be avoided.
    Critical,
    /// Unknown Alert Status
    #[default]
    Unknown,
}

/// The various alerts that can be raised by the pool.
#[derive(Serialize, Deserialize, Debug, Default, Clone, EnumString, Display, Eq, PartialEq)]
pub enum PoolAlert {
    /// Unrecognized or unsupported alert type.
    #[default]
    Unknown,
    /// Pool I/O is currently stalled.
    IoStalled,
    /// I/O is not stalled as of now, but stalls have occurred over a defined time window.
    IoStallIntermittent,
    /// I/O is not stalled as of now, but stalls have occurred too frequently over a defined time window.
    IoStallIntermittentExc,
    /// I/O errors have been detected for the pool.
    IoError,
    /// I/O errors exceed defined thresholds.
    IoErrorExc,
}

impl From<CtrlPoolState> for models::PoolState {
    fn from(src: CtrlPoolState) -> Self {
        let src = src.state;
        Self::new_all(
            src.capacity,
            src.disks,
            src.id,
            src.node,
            src.status,
            src.used,
            src.committed,
            src.encrypted,
            Some(src.cluster_size as u64),
            src.disk_capacity,
            src.max_expandable_size,
            src.errors.into_opt(),
            src.repl_count,
            src.snap_count,
        )
    }
}

impl From<PoolErrorInfo> for models::PoolErrorInfo {
    fn from(value: PoolErrorInfo) -> Self {
        Self {
            alerts: value.alerts.into(),
            io_error_count: value.io_error_count,
            io_error_threshold: value.io_error_threshold,
            io_stalled: value.io_stalled,
            io_stall_transition_count: value.io_stall_transition_count,
            io_stall_transition_threshold: value.io_stall_transition_threshold,
        }
    }
}

impl From<PoolAlerts> for models::PoolAlerts {
    fn from(value: PoolAlerts) -> Self {
        use crate::IntoVec;
        Self {
            status: value.status.into(),
            notice: value.notice.into_vec(),
            attention: value.attention.into_vec(),
            warning: value.warning.into_vec(),
            critical: value.critical.into_vec(),
        }
    }
}

impl From<PoolAlert> for models::PoolAlert {
    fn from(value: PoolAlert) -> Self {
        match value {
            PoolAlert::Unknown => Self::Unknown,
            PoolAlert::IoStalled => Self::IoStalled,
            PoolAlert::IoStallIntermittent => Self::IoStallIntermittent,
            PoolAlert::IoStallIntermittentExc => Self::IoStallIntermittentExc,
            PoolAlert::IoError => Self::IoError,
            PoolAlert::IoErrorExc => Self::IoErrorExc,
        }
    }
}

impl From<PoolAlertStatus> for models::PoolAlertStatus {
    fn from(value: PoolAlertStatus) -> Self {
        match value {
            PoolAlertStatus::Healthy => Self::Healthy,
            PoolAlertStatus::Attention => Self::Attention,
            PoolAlertStatus::Warning => Self::Warning,
            PoolAlertStatus::Critical => Self::Critical,
            PoolAlertStatus::Unknown => Self::Unknown,
        }
    }
}

impl From<PoolDiag> for models::PoolDiag {
    fn from(src: PoolDiag) -> Self {
        Self::new_all(src.import_errors, src.status, src.error.into_opt())
    }
}

impl From<PoolDiskError> for models::PoolDiskError {
    fn from(value: PoolDiskError) -> Self {
        Self::new_all(value.disk, value.error)
    }
}
impl From<PoolError> for models::PoolProbeError {
    fn from(value: PoolError) -> Self {
        Self::new_all(value.msg, value.code)
    }
}
impl From<PoolErrorCode> for models::PoolProbeErrorCode {
    fn from(value: PoolErrorCode) -> Self {
        match value {
            PoolErrorCode::Unknown => Self::Unknown,
            PoolErrorCode::DiskNotFound => Self::DiskNotFound,
            PoolErrorCode::DiskReadIoError => Self::DiskReadIoError,
            PoolErrorCode::ForeignPoolName => Self::ForeignPoolName,
            PoolErrorCode::ForeignPoolUid => Self::ForeignPoolUid,
            PoolErrorCode::SuperBlockIoError => Self::SuperBlockIoError,
            PoolErrorCode::InvalidSuperBlock => Self::InvalidSuperBlock,
            PoolErrorCode::DiskIsADirectory => Self::DiskIsADirectory,
            PoolErrorCode::NodeIsUnknown => Self::NodeIsUnknown,
            PoolErrorCode::NodeIsOffline => Self::NodeIsOffline,
            PoolErrorCode::ImportDisabled => Self::ImportDisabled,
            PoolErrorCode::TimeOut => Self::TimeOut,
            PoolErrorCode::Aborted => Self::Aborted,
            PoolErrorCode::DiskClaimed => Self::DiskClaimed,
            PoolErrorCode::PCIDriverUnsupported => Self::PciDriverUnsupported,
            PoolErrorCode::PCIKernelBound => Self::PciKernelBound,
            PoolErrorCode::PCINotNvme => Self::PciNotNvme,
            PoolErrorCode::InvalidDiskUri => Self::InvalidDiskUri,
            PoolErrorCode::DiskNotImportable => Self::DiskNotImportable,
            PoolErrorCode::UriNotHandled => Self::UriNotHandled,
        }
    }
}

rpc_impl_string_id!(PoolId, "ID of a pool");
rpc_impl_string_uuid!(PoolUuid, "UUID of a pool");

// online > degraded > unknown/faulted
impl PartialOrd for PoolStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            PoolStatus::Unknown => match other {
                PoolStatus::Unknown => None,
                PoolStatus::Offline => None,
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Degraded => Some(Ordering::Less),
                PoolStatus::Suspected => Some(Ordering::Less),
                PoolStatus::Faulted => None,
            },
            PoolStatus::Online => match other {
                PoolStatus::Unknown => Some(Ordering::Greater),
                PoolStatus::Offline => Some(Ordering::Greater),
                PoolStatus::Online => Some(Ordering::Equal),
                PoolStatus::Degraded => Some(Ordering::Greater),
                PoolStatus::Suspected => Some(Ordering::Greater),
                PoolStatus::Faulted => Some(Ordering::Greater),
            },
            PoolStatus::Degraded | PoolStatus::Suspected => match other {
                PoolStatus::Unknown => Some(Ordering::Greater),
                PoolStatus::Offline => Some(Ordering::Greater),
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Suspected => Some(Ordering::Equal),
                PoolStatus::Degraded => Some(Ordering::Equal),
                PoolStatus::Faulted => Some(Ordering::Greater),
            },
            PoolStatus::Offline => match other {
                PoolStatus::Unknown => None,
                PoolStatus::Offline => Some(Ordering::Equal),
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Suspected => Some(Ordering::Less),
                PoolStatus::Degraded => Some(Ordering::Less),
                PoolStatus::Faulted => None,
            },
            PoolStatus::Faulted => match other {
                PoolStatus::Unknown => None,
                PoolStatus::Offline => None,
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Degraded => Some(Ordering::Less),
                PoolStatus::Suspected => Some(Ordering::Less),
                PoolStatus::Faulted => Some(Ordering::Equal),
            },
        }
    }
}

/// User configuration with user specification and metadata information.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PoolDef {
    /// User specification for the pool.
    pub spec: PoolUSpec,
    /// How many replicas are owned by the pool.
    pub replica_count: Option<u64>,
    /// How many snapshots are owned by the pool.
    pub snapshot_count: Option<u64>,
    pub persisted_metadata: PoolPersistedMetadata,
}

/// User configuration with user specification and metadata information.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PoolConfig {
    /// Pool definition.
    pub definition: PoolDef,
    /// Health of the pool.
    pub diag: Option<PoolDiag>,
}
impl PoolConfig {
    fn with_state_diag(mut self, state: &CtrlPoolState) -> Self {
        if let Some(ref mut diag) = self.diag {
            diag.status = state.state.status.clone();
        }
        self
    }
}

impl From<PoolSpec> for PoolConfig {
    fn from(value: PoolSpec) -> Self {
        Self {
            definition: PoolDef {
                spec: value.spec,
                replica_count: value.metadata.runtime.replica_count,
                snapshot_count: value.metadata.runtime.snapshot_count,
                persisted_metadata: value.metadata.persisted,
            },
            diag: value.metadata.runtime.diag,
        }
    }
}

/// A Storage Pool.
/// It may have a spec which is the specification provided by the creator.
/// It may have a state if such state is retrieved from a storage node.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// Pool identification.
    id: PoolId,
    /// [`PoolConfig`].
    pub config: Option<PoolConfig>,
    /// Runtime state of the pool.
    pub state: Option<CtrlPoolState>,
}

impl Pool {
    /// Construct a new pool with spec and state.
    pub fn new(spec: PoolSpec, state: Option<CtrlPoolState>) -> Self {
        Self {
            id: spec.id.clone(),
            config: Some(PoolConfig::from(spec)),
            state,
        }
    }
    /// Construct a new pool with spec but no state.
    pub fn from_spec(spec: PoolSpec) -> Self {
        Self {
            id: spec.id.clone(),
            config: Some(PoolConfig::from(spec)),
            state: None,
        }
    }
    /// Construct a new pool with optional spec and state.
    pub fn from_state(state: CtrlPoolState, spec: Option<PoolSpec>) -> Self {
        Self {
            id: state.id.clone(),
            config: spec.map(|s| PoolConfig::from(s).with_state_diag(&state)),
            state: Some(state),
        }
    }
    /// Try to construct a new pool from spec and state.
    pub fn try_new(spec: Option<PoolSpec>, state: Option<CtrlPoolState>) -> Option<Self> {
        match (spec, state) {
            (Some(spec), Some(state)) => Some(Self::new(spec, Some(state))),
            (Some(spec), None) => Some(Self::from_spec(spec)),
            (None, Some(state)) => Some(Self::from_state(state, None)),
            _ => None,
        }
    }
    /// Get the pool spec.
    pub fn spec(&self) -> Option<PoolUSpec> {
        Some(self.config.as_ref()?.definition.spec.clone())
    }
    /// Get the pool diagnostics.
    pub fn diag(&self) -> Option<&PoolDiag> {
        self.config.as_ref()?.diag.as_ref()
    }
    /// Get a live pool usage statistics, if available.
    pub fn current_usage(&self) -> Option<PoolUsage> {
        let state = self.state.as_ref()?;
        let def = self.config.as_ref().map(|config| &config.definition);
        Some(PoolUsage {
            repl_count: def.and_then(|def| def.replica_count)?,
            snap_count: def.and_then(|def| def.snapshot_count)?,
            used: state.used,
            committed: state.committed,
        })
    }
    /// Get the pool identification.
    pub fn id(&self) -> &PoolId {
        &self.id
    }
    /// Get the pool state.
    pub fn state(&self) -> Option<&PoolState> {
        self.state.as_ref().map(|p| p.state())
    }
    /// Get the controller's pool state.
    pub fn ctrl_state(&self) -> Option<&CtrlPoolState> {
        self.state.as_ref()
    }
    /// Get the node identification.
    pub fn node(&self) -> NodeId {
        match &self.config {
            // guaranteed that at either spec or state are defined
            // todo: use enum derivation
            None => self.state.as_ref().unwrap().node.clone(),
            Some(config) => config.definition.spec.node.clone(),
        }
    }
}

impl From<Pool> for models::Pool {
    fn from(src: Pool) -> Self {
        let (def, diag) = match src.config {
            None => (None, None),
            Some(config) => (Some(config.definition), config.diag),
        };
        let (spec, meta) = match def {
            None => (None, None),
            Some(def) => {
                let meta = models::PoolMeta::new_all(def.replica_count, def.snapshot_count);
                (Some(def.spec), Some(meta))
            }
        };

        models::Pool::new_all(
            src.id,
            spec.into_opt(),
            src.state.into_opt(),
            diag.into_opt(),
            meta,
        )
    }
}

/// Pool device URI.
/// Can be specified in the form of a file path or a URI.
/// eg: /dev/sda, aio:///dev/sda, malloc:///disk?size_mb=100.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PoolDeviceUri(String);
impl Deref for PoolDeviceUri {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Default for PoolDeviceUri {
    fn default() -> Self {
        Self("malloc:///disk?size_mb=100".into())
    }
}
impl From<&str> for PoolDeviceUri {
    fn from(device: &str) -> Self {
        Self(device.to_string())
    }
}
impl From<&String> for PoolDeviceUri {
    fn from(device: &String) -> Self {
        Self(device.clone())
    }
}
impl From<String> for PoolDeviceUri {
    fn from(device: String) -> Self {
        Self(device)
    }
}
impl std::fmt::Display for PoolDeviceUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl From<PoolDeviceUri> for String {
    fn from(device: PoolDeviceUri) -> Self {
        device.to_string()
    }
}

/// Create Pool Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreatePool {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub id: PoolId,
    /// Disk device paths or URIs to be claimed by the pool.
    pub disks: Vec<PoolDeviceUri>,
    /// Labels to be set on the pool.
    pub labels: Option<PoolLabel>,
    /// Encryption parameters for this pool.
    pub encryption: Option<Encryption>,
    /// Blobstore cluster size in bytes.
    pub cluster_size: Option<u32>,
    /// Maximum expansion size for this pool.
    pub max_expansion: Option<String>,
}

impl CreatePool {
    /// Create new `Self` from the given parameters.
    pub fn new(
        node: &NodeId,
        id: &PoolId,
        disks: &[PoolDeviceUri],
        labels: &Option<PoolLabel>,
        encryption: &Option<Encryption>,
        cluster_size: &Option<u32>,
        max_expansion: &Option<String>,
    ) -> Self {
        Self {
            node: node.clone(),
            id: id.clone(),
            disks: disks.to_vec(),
            labels: labels.clone(),
            encryption: encryption.clone(),
            cluster_size: *cluster_size,
            max_expansion: max_expansion.clone(),
        }
    }
}

/// Create Pool Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ImportPool {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub id: PoolId,
    /// Disk device paths or URIs to be claimed by the pool.
    pub disks: Vec<PoolDeviceUri>,
    /// The pool uuid if specified.
    pub uuid: Option<PoolUuid>,
    /// Encryption parameters for this pool.
    pub encryption: Option<Encryption>,
}

impl ImportPool {
    /// Create new `Self` from the given parameters.
    pub fn new(
        node: &NodeId,
        id: &PoolId,
        disks: &[PoolDeviceUri],
        encryption: &Option<Encryption>,
    ) -> Self {
        Self {
            node: node.clone(),
            id: id.clone(),
            disks: disks.to_vec(),
            uuid: None,
            encryption: encryption.clone(),
        }
    }
}

/// Destroy Pool Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyPool {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub id: PoolId,
    /// Purge pool without contacting io-engine (for offline/unknown pools).
    #[serde(default)]
    pub purge: bool,
    /// Accept deletion when pool has replicas.
    #[serde(default)]
    pub accept: bool,
    /// Accept volume loss (last healthy replica for volumes).
    #[serde(default)]
    pub accept_volume_loss: bool,
    /// Accept snapshot loss (last replica snapshot for snapshots).
    #[serde(default)]
    pub accept_snapshot_loss: bool,
}
impl DestroyPool {
    /// Create a new DestroyPool request (normal delete, not purge).
    pub fn new(node: NodeId, id: PoolId) -> Self {
        Self {
            node,
            id,
            purge: false,
            accept: false,
            accept_volume_loss: false,
            accept_snapshot_loss: false,
        }
    }

    /// Create a purge request.
    pub fn purge(node: NodeId, id: PoolId) -> Self {
        Self {
            node,
            id,
            purge: true,
            accept: false,
            accept_volume_loss: false,
            accept_snapshot_loss: false,
        }
    }

    /// Set purge option.
    pub fn with_purge(mut self, purge: bool) -> Self {
        self.purge = purge;
        self
    }

    /// Set accept option.
    pub fn with_accept(mut self, accept: bool) -> Self {
        self.accept = accept;
        self
    }

    /// Set accept_volume_loss option.
    pub fn with_accept_volume_loss(mut self, accept_volume_loss: bool) -> Self {
        self.accept_volume_loss = accept_volume_loss;
        self
    }

    /// Set accept_snapshot_loss option.
    pub fn with_accept_snapshot_loss(mut self, accept_snapshot_loss: bool) -> Self {
        self.accept_snapshot_loss = accept_snapshot_loss;
        self
    }
}
impl From<CreatePool> for DestroyPool {
    fn from(value: CreatePool) -> Self {
        Self::new(value.node, value.id)
    }
}

/// Result of a pool purge operation.
///
/// Only returned by purge operations (not normal deletes). The `volume_loss` and `snapshot_loss`
/// fields are always present — empty lists indicate no loss occurred.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct PoolDeleteResult {
    /// The deleted pool ID.
    pub pool_id: PoolId,
    /// Information about volumes that lost healthy replicas.
    /// An empty `volumes` list means no data loss occurred.
    pub volume_loss: VolumeLossInfo,
    /// Information about snapshots that lost replica snapshots.
    /// An empty `snapshots` list means no snapshot loss occurred.
    pub snapshot_loss: SnapshotLossInfo,
}

impl PoolDeleteResult {
    /// Create a new result for a purge with no data or snapshot loss.
    pub fn new(pool_id: PoolId) -> Self {
        Self {
            pool_id,
            volume_loss: VolumeLossInfo::default(),
            snapshot_loss: SnapshotLossInfo::default(),
        }
    }

    /// Check if any data loss occurred.
    pub fn has_volume_loss(&self) -> bool {
        !self.volume_loss.volumes.is_empty()
    }

    /// Check if any snapshot loss occurred.
    pub fn has_snapshot_loss(&self) -> bool {
        !self.snapshot_loss.snapshots.is_empty()
    }
}

impl From<PoolDeleteResult> for models::PoolDeleteResult {
    fn from(src: PoolDeleteResult) -> Self {
        models::PoolDeleteResult {
            pool_id: src.pool_id.to_string(),
            volume_loss: models::VolumeLossInfo {
                volumes: src
                    .volume_loss
                    .volumes
                    .into_iter()
                    .map(|v| models::VolumeLossDetail {
                        volume_id: v.volume_id.to_string(),
                        replicas_before: v.replicas_before,
                        healthy_before: v.healthy_before,
                        lost_on_pool: v.lost_on_pool,
                        healthy_after: v.healthy_after,
                    })
                    .collect(),
            },
            snapshot_loss: models::SnapshotLossInfo {
                snapshots: src
                    .snapshot_loss
                    .snapshots
                    .into_iter()
                    .map(|s| models::SnapshotLossDetail {
                        snapshot_id: s.snapshot_id.to_string(),
                        replica_snapshots_before: s.replica_snapshots_before,
                        healthy_before: s.healthy_before,
                        lost_on_pool: s.lost_on_pool,
                        healthy_after: s.healthy_after,
                    })
                    .collect(),
            },
        }
    }
}

/// Information about volume data loss caused by a pool deletion.
///
/// Contains a list of volumes that lost their last healthy replica as a result of
/// the pool being purged. An empty `volumes` list means no data loss occurred.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct VolumeLossInfo {
    /// List of volumes that lost their last healthy replica.
    pub volumes: Vec<VolumeLossDetail>,
}

/// Details about a volume affected by pool deletion.
///
/// When a pool is purged, replicas on that pool are destroyed. This struct records the
/// impact on a specific volume: how many replicas it had, how many were healthy, how many
/// were lost, and how many healthy ones remain. If `healthy_after` is zero, the volume
/// has suffered data loss — no healthy replicas remain to serve I/O.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct VolumeLossDetail {
    /// The affected volume's unique identifier.
    pub volume_id: VolumeId,
    /// Total number of replicas this volume had across all pools before the deletion.
    pub replicas_before: u32,
    /// Number of those replicas that were in a healthy state before the deletion.
    /// A healthy replica is one that was fully synced and serving I/O.
    pub healthy_before: u32,
    /// Number of this volume's replicas that resided on the pool being deleted.
    /// These replicas are destroyed as part of the purge.
    pub lost_on_pool: u32,
    /// Number of healthy replicas remaining after the pool deletion.
    /// Zero means no healthy replicas survive — the volume has suffered data loss.
    pub healthy_after: u32,
}

/// Information about snapshot loss caused by a pool deletion.
///
/// Contains a list of snapshots that lost their last healthy replica snapshot as a result
/// of the pool being purged. An empty `snapshots` list means no snapshot loss occurred.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct SnapshotLossInfo {
    /// List of snapshots that lost their last replica snapshot.
    pub snapshots: Vec<SnapshotLossDetail>,
}

/// Details about a snapshot affected by pool deletion.
///
/// When a pool is purged, replica snapshots on that pool are destroyed. This struct records
/// the impact on a specific volume snapshot: how many replica snapshots it had, how many
/// were healthy, how many were lost, and how many healthy ones remain. If `healthy_after`
/// is zero, the snapshot has been lost — it can no longer be used for restores.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct SnapshotLossDetail {
    /// The affected volume snapshot's unique identifier.
    pub snapshot_id: SnapshotId,
    /// Total number of replica snapshots this snapshot had across all pools before the deletion.
    pub replica_snapshots_before: u32,
    /// Number of those replica snapshots that were in a healthy state before the deletion.
    /// A healthy replica snapshot is one that is available for restore operations.
    pub healthy_before: u32,
    /// Number of this snapshot's replica snapshots that resided on the pool being deleted.
    /// These replica snapshots are destroyed as part of the purge.
    pub lost_on_pool: u32,
    /// Number of healthy replica snapshots remaining after the pool deletion.
    /// Zero means no healthy replica snapshots survive — the snapshot is lost.
    pub healthy_after: u32,
}

/// Expand Pool Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExpandPool {
    /// Id of the pool.
    pub id: PoolId,
}

impl ExpandPool {
    /// Create a new `Self` from the given parameters.
    pub fn new(id: PoolId) -> Self {
        Self { id }
    }
}

/// Label Pool Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelPool {
    /// Id of the pool.
    pub pool_id: PoolId,
    /// Labels to be set on the pool
    pub labels: HashMap<String, String>,
    /// Overwrite the existing labels
    pub overwrite: bool,
}

impl LabelPool {
    /// Create new `Self` from the given parameters.
    pub fn new(pool_id: PoolId, labels: HashMap<String, String>, overwrite: bool) -> Self {
        Self {
            pool_id,
            labels,
            overwrite,
        }
    }
}

/// Un-Label Pool Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnlabelPool {
    /// Id of the pool.
    pub pool_id: PoolId,
    /// Label key to be removed from the pool.
    pub label_key: String,
}

impl UnlabelPool {
    /// Create new `Self` from the given parameters.
    pub fn new(pool_id: PoolId, label_key: String) -> Self {
        Self { pool_id, label_key }
    }
}

impl TryFrom<Encryption> for models::Encryption {
    type Error = String;

    fn try_from(value: Encryption) -> Result<Self, Self::Error> {
        match value {
            Encryption::Secret(secret_name) => Ok(Self::secret(secret_name.into())),
        }
    }
}

impl From<models::EncryptionSecret> for EncryptionSecret {
    fn from(value: models::EncryptionSecret) -> Self {
        Self { name: value.name }
    }
}

/// Response from a GetPoolHealth query.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetPoolHealthResponse {
    pub disks: Vec<DiskHealth>,
}

/// Health for a single disk backing the pool.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct DiskHealth {
    pub disk_uri: String,
    pub supported: bool,
    pub health: Option<DeviceHealth>,
    pub error: Option<String>,
}

/// SMART / health information for a backing device.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct DeviceHealth {
    pub critical_warning: u32,
    pub healthy: bool,
    pub temperature_celsius: Option<i32>,
    pub available_spare_percent: Option<u32>,
    pub available_spare_threshold_percent: Option<u32>,
    pub percentage_used: Option<u32>,
    pub data_units_read: Option<u64>,
    pub data_units_written: Option<u64>,
    pub host_reads: Option<u64>,
    pub host_writes: Option<u64>,
    pub controller_busy_minutes: Option<u64>,
    pub power_cycles: Option<u64>,
    pub power_on_hours: Option<u64>,
    pub unsafe_shutdowns: Option<u64>,
    pub media_errors: Option<u64>,
    pub num_error_log_entries: Option<u64>,
    pub identity: Option<DeviceIdentity>,
    pub smart_attributes: Vec<SmartAttribute>,
    pub error_log_entries: Vec<NvmeErrorLogEntry>,
}

/// Device identity/inventory data.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct DeviceIdentity {
    pub model: Option<String>,
    pub model_family: Option<String>,
    pub serial_number: Option<String>,
    pub firmware_revision: Option<String>,
    pub wwn: Option<String>,
    pub capacity_bytes: Option<u64>,
    pub logical_sector_size: Option<u32>,
    pub physical_sector_size: Option<u32>,
    pub rotation_rate: Option<u32>,
    pub form_factor: Option<String>,
    pub transport: Option<String>,
    pub link_speed: Option<String>,
}

/// A single SMART attribute table entry (ATA devices only).
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct SmartAttribute {
    pub id: u32,
    pub name: String,
    pub value: u32,
    pub worst: u32,
    pub threshold: u32,
    pub raw_value: u64,
}

/// A single NVMe Error Information Log entry.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct NvmeErrorLogEntry {
    pub error_count: u64,
    pub submission_queue_id: u32,
    pub command_id: Option<u32>,
    pub status_field: u32,
    pub lba: Option<u64>,
    pub namespace_id: Option<u32>,
}
