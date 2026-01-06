use super::*;

use crate::{
    types::v0::store::pool::{Encryption, EncryptionSecret, PoolLabel, PoolSpec, PoolUSpec},
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
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, Display, Eq, PartialEq)]
pub enum PoolStatus {
    /// Unknown state.
    Unknown = 0,
    /// The pool is in normal working order.
    Online = 1,
    /// The pool has experienced a failure but can still function.
    Degraded = 2,
    /// The pool is completely inaccessible.
    Faulted = 3,
}

impl Default for PoolStatus {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for PoolStatus {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}
impl From<PoolStatus> for models::PoolStatus {
    fn from(src: PoolStatus) -> Self {
        match src {
            PoolStatus::Unknown => Self::Unknown,
            PoolStatus::Online => Self::Online,
            PoolStatus::Degraded => Self::Degraded,
            PoolStatus::Faulted => Self::Faulted,
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
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub enum PoolErrorCode {
    /// Unknown error
    #[default]
    Unknown,
    /// Disk not found in the system
    DiskNotFound,
    /// Disk read IO errors
    DiskReadIoError,
    /// Pool on-disk name doesn't match the expected
    ForeignPoolName,
    /// Pool on-disk uuid doesn't match the expected
    ForeignPoolUid,
    /// Failed to check super block error
    SuperBlock,
    /// Invalid super block (eg: CRC error)
    InvalidSuperBlock,
}

/// Pool error code and human-readable message.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct PoolError {
    /// Code of the encountered error.
    pub code: PoolErrorCode,
    /// Human-readable message.
    pub msg: String,
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
}

/// Pool state information - as reported by the io-engine.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PoolState {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub id: PoolId,
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
        )
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
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Degraded => Some(Ordering::Less),
                PoolStatus::Faulted => None,
            },
            PoolStatus::Online => match other {
                PoolStatus::Unknown => Some(Ordering::Greater),
                PoolStatus::Online => Some(Ordering::Equal),
                PoolStatus::Degraded => Some(Ordering::Greater),
                PoolStatus::Faulted => Some(Ordering::Greater),
            },
            PoolStatus::Degraded => match other {
                PoolStatus::Unknown => Some(Ordering::Greater),
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Degraded => Some(Ordering::Equal),
                PoolStatus::Faulted => Some(Ordering::Greater),
            },
            PoolStatus::Faulted => match other {
                PoolStatus::Unknown => None,
                PoolStatus::Online => Some(Ordering::Less),
                PoolStatus::Degraded => Some(Ordering::Less),
                PoolStatus::Faulted => Some(Ordering::Equal),
            },
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
    /// Desired specification of the pool.
    pub spec: Option<PoolUSpec>,
    /// Runtime state of the pool.
    pub state: Option<CtrlPoolState>,
    /// Health of the pool.
    /// todo: is this needed since we have it on the "spec" misnomer.
    pub diag: Option<PoolDiag>,
}

impl Pool {
    /// Construct a new pool with spec and state.
    pub fn new(spec: PoolSpec, state: Option<CtrlPoolState>) -> Self {
        Self {
            id: spec.id.clone(),
            diag: spec.metadata.runtime.diag.clone(),
            spec: Some(spec.spec),
            state,
        }
    }
    /// Construct a new pool with spec but no state.
    pub fn from_spec(spec: PoolSpec) -> Self {
        Self {
            id: spec.id.clone(),
            diag: spec.metadata.runtime.diag.clone(),
            spec: Some(spec.spec),
            state: None,
        }
    }
    /// Construct a new pool with optional spec and state.
    pub fn from_state(state: CtrlPoolState, spec: Option<PoolSpec>) -> Self {
        Self {
            id: state.id.clone(),
            diag: spec
                .as_ref()
                .and_then(|spec| spec.metadata.runtime.diag.clone()),
            spec: spec.map(|s| s.spec),
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
        self.spec.clone()
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
        match &self.spec {
            // guaranteed that at either spec or state are defined
            // todo: use enum derivation
            None => self.state.as_ref().unwrap().node.clone(),
            Some(spec) => spec.node.clone(),
        }
    }
}

impl From<Pool> for models::Pool {
    fn from(src: Pool) -> Self {
        models::Pool::new_all(src.id, src.spec.into_opt(), src.state.into_opt())
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
}
impl DestroyPool {
    /// Create a new `Self` from the given parameters.
    pub fn new(node: NodeId, id: PoolId) -> Self {
        Self { node, id }
    }
}
impl From<CreatePool> for DestroyPool {
    fn from(value: CreatePool) -> Self {
        Self::new(value.node, value.id)
    }
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
