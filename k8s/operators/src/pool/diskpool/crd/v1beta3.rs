use k8s_openapi::apimachinery::pkg::apis::meta::v1 as meta_v1;
use kube::CustomResource;
#[cfg(feature = "openapi")]
use openapi::models::{pool_status::PoolStatus as RestPoolStatus, Pool};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::AsRefStr;

use super::quantity::Quantity;

#[derive(CustomResource, Serialize, Deserialize, Default, Debug, Clone, JsonSchema)]
#[kube(
    group = "openebs.io",
    version = "v1beta3",
    kind = "DiskPool",
    plural = "diskpools",
    // The name of the struct that gets created that represents a resource
    namespaced,
    status = "DiskPoolStatus",
    derive = "Default",
    shortname = "dsp",
    printcolumn = r#"{ "name":"node", "type":"string", "description":"node the pool is on", "jsonPath":".spec.node"}"#,
    printcolumn = r#"{ "name":"state", "type":"string", "description":"dsp cr state", "jsonPath":".status.cr_state"}"#,
    printcolumn = r#"{ "name":"status", "type":"string", "description":"Control plane pool status", "jsonPath":".status.status"}"#,
    printcolumn = r#"{ "name":"error", "type":"string", "description":"Control plane pool status", "jsonPath":".status.error.code"}"#,
    printcolumn = r#"{ "name":"alerts", "type":"string", "description":"Control plane pool status", "jsonPath":".status.alertError"}"#,
    printcolumn = r#"{ "name":"encrypted", "type":"boolean", "description":"encryption enabled", "jsonPath":".status.encrypted"}"#,
    printcolumn = r#"{ "name":"capacity", "type":"string", "nullable": "true", "description":"total bytes", "jsonPath":".status.capacity_q"}"#,
    printcolumn = r#"{ "name":"used", "type":"string", "nullable": "true", "description":"used bytes", "jsonPath":".status.used_q"}"#,
    printcolumn = r#"{ "name":"available", "type":"string", "nullable": "true", "description":"available bytes", "jsonPath":".status.available_q"}"#,
    printcolumn = r#"{ "name":"disk-capacity", "type":"string", "nullable": "true", "description":"underlying disk capacity", "jsonPath":".status.diskCapacity"}"#,
    printcolumn = r#"{ "name":"max-expandable-size", "type":"string", "nullable": "true", "description":"max expandable size", "jsonPath":".status.maxExpandableSize"}"#
)]
/// The pool spec which contains the parameters we use when creating the pool
pub struct DiskPoolSpec {
    /// The node the pool is placed on
    pub node: String,
    /// The disk device the pool is located on
    pub disks: Vec<String>,
    /// The topology for data placement.
    pub topology: Option<Topology>,
    /// Use to create encrypted pool.
    #[serde(rename = "encryptionConfig")]
    pub encryption_config: Option<EncryptionConfig>,
    /// Blobstore cluster size required for this pool. This is an advanced option,
    /// please refer documentation to understand this configuration and its implications.
    #[serde(rename = "clusterSize")]
    pub cluster_size: Option<String>,
    /// Maximum expected expansion for the pool. It can be a factor or an absolute size,
    /// Example: 5x, 10x, 6x, 200GiB, 2TiB or 536870912000B.
    #[serde(rename = "maxExpansion")]
    pub max_expansion: Option<String>,
}

/// Pool diagnostic information.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolDiag {
    /// Errors encountered when trying to import the pool.
    pub import_errors: Vec<DiskInfo>,
    /// Inferred error of the pool, if any.
    pub error: Option<PoolError>,
    /// Inferred status of the pool.
    pub status: PoolStatus,
}

/// Different pool errors.
#[derive(
    Debug, Default, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, JsonSchema, AsRefStr,
)]
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
    SuperBlock,
    /// Invalid super block (eg: CRC error).
    InvalidSuperBlock,
    /// Disk is a directory (can happen when setting up incorrect volume mounts).
    DiskIsADirectory,
    /// Node is in unknown state.
    NodeIsUnknown,
    /// Node is offline.
    NodeIsOffline,
    /// Imports are disabled due to cordon restrictions.
    ImportDisabled,
    /// gRPC to the pool timed out.
    TimeOut,
    // If the Disk is already claimed by something.
    // This may happen if the disk is used by another pool for example.
    DiskClaimed,
    // PCI driver not supported.
    PciDriverUnsupported,
    // PCI still bound to kernel nvme driver.
    PciKernelBound,
    // PCI BDF exists but it's not an NVMe device.
    PciNotNvme,
    // The disk URI is invalid or not supported.
    InvalidDiskUri,
    /// Pool deleted under the Custom Resource.
    PoolDeleted,
    /// Control-Plane service is unreachable.
    Unreachable,
    /// The encryption secret was not found.
    EncryptionSecretError,
}

/// Pool error code and human-readable message.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
pub struct PoolError {
    /// Code of the encountered error.
    pub code: PoolErrorCode,
    /// Human-readable message.
    pub message: Option<String>,
}

/// Pool information related to a specific disk.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiskInfo {
    /// The disk uri used to open the disk.
    pub uri: String,
    /// Errors seen for this disk.
    pub errors: PoolError,
}

/// Alerts and error information for a pool.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolErrorInfo {
    /// These are generated from the below metrics.
    pub alerts: PoolAlerts,
    /// Count of all disk errors since the pool has been opened.
    pub io_error_count: u64,
    /// The I/O is stalled.
    pub io_stalled: bool,
    /// Number of stall transitions.
    pub io_stall_transition_count: u64,
}

/// Pool alerts and inferred status.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
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
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
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
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
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

/// Placement pool topology used by volume operations.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
pub struct Topology {
    /// Label for topology.
    #[serde(default)]
    pub labelled: HashMap<String, String>,
}

/// Encryption configuration from a specified source.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
pub struct EncryptionConfig {
    pub source: EncryptionSource,
}

/// Encryption configuration Sources.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
pub enum EncryptionSource {
    #[serde(rename = "secret")]
    Secret(EncryptionSecretConfig),
}

/// Encryption Secret source details.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
pub struct EncryptionSecretConfig {
    pub name: String,
}

impl DiskPoolSpec {
    /// Create a new DiskPoolSpec from the node and the disks.
    pub fn new(
        node: String,
        disks: Vec<String>,
        topology: Option<Topology>,
        encryption_config: Option<EncryptionConfig>,
        cluster_size: Option<String>,
        max_expansion: Option<String>,
    ) -> Self {
        Self {
            node,
            disks,
            topology,
            encryption_config,
            cluster_size,
            max_expansion,
        }
    }
    /// The node the pool is placed on.
    pub fn node(&self) -> String {
        self.node.clone()
    }
    /// The disk devices that compose the pool.
    pub fn disks(&self) -> Vec<String> {
        self.disks.clone()
    }

    /// The topology that decides replica placement.
    pub fn topology(&self) -> Option<Topology> {
        self.topology.clone()
    }
    /// The encryption configuration.
    pub fn encryption_config(&self) -> Option<EncryptionConfig> {
        self.encryption_config.clone()
    }
    /// Blobstore cluster size for this pool.
    pub fn cluster_size(&self) -> Option<String> {
        self.cluster_size.clone()
    }

    /// Maximum expected expansion for the pool.
    pub fn max_expansion(&self) -> Option<String> {
        self.max_expansion.clone()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, JsonSchema, Default)]
/// PoolState represents operator specific states for DSP CR.
pub enum CrPoolState {
    /// The pool is a new OR missing resource, and it has not been created or
    /// imported yet by the operator. The pool spec MAY be but DOES
    /// NOT have a status field.
    #[default]
    Creating,
    /// The resource spec has been created, and the pool is getting created by
    /// the control plane.
    Created,
    /// This state is set when we receive delete event on the dsp cr.
    Terminating,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
/// PoolStatus is Control plane status of a given DSP CR.
pub enum PoolStatus {
    /// State is Unknown.
    #[default]
    Unknown,
    /// State is Offline.
    Offline,
    /// The pool is in normal working order.
    Online,
    /// The pool has experienced a failure but can still function.
    Degraded,
    /// The pool is completely inaccessible.
    Faulted,
    /// The pool is in a suspected state, with notice or more severed alerts.
    Suspected,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
/// Status of the pool which is driven and changed by the controller loop.
pub struct DiskPoolStatus {
    #[serde(default)]
    pub cr_state: CrPoolState,
    /// Pool status from respective control plane object.
    pub pool_status: Option<PoolStatus>,
    /// Capacity as number of bytes.
    pub capacity: u64,
    /// Used number of bytes.
    pub used: u64,
    /// Available number of bytes.
    pub available: u64,
    /// Total capacity.
    pub capacity_q: Option<Quantity>,
    /// Used capacity.
    pub used_q: Option<Quantity>,
    /// Available capacity.
    pub available_q: Option<Quantity>,
    /// Encryption enabled.
    pub encrypted: Option<bool>,
    /// Blobstore cluster size of this pool.
    #[serde(rename = "clusterSize")]
    pub cluster_size: Option<Quantity>,
    /// Current size of the underlying disk. in quantity.
    #[serde(rename = "diskCapacity")]
    pub disk_capacity: Option<Quantity>,
    /// Maximum capacity the disk can be expanded to.
    /// This is an absolute max. No expansion is allowed beyond this size.
    #[serde(rename = "maxExpandableSize")]
    pub max_expandable_size: Option<Quantity>,
    /// Information for each pool disk.
    #[serde(rename = "diskInfo")]
    pub disk_info: Option<Vec<DiskInfo>>,
    /// Error information at the pool top-level.
    #[serde(rename = "errorInfo")]
    pub error_info: Option<PoolErrorInfo>,
    /// The pool diagnostic information.
    pub diag: Option<PoolDiag>,
    /// The inferred status obtained from the data-plane and the diagnostics.
    pub status: Option<PoolStatus>,
    /// The inferred error obtained from the openapi diagnostics or the creation/import result.
    pub error: Option<PoolError>,
    /// Combined alert and errors for the pretty columns.
    #[serde(rename = "alertError")]
    pub alert_error: Option<String>,
    /// Information about the state of a DiskPool using standard K8s conditions.
    /// PoolReady: Indicates whether this dsp is ready and usable for volumes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) conditions: Vec<meta_v1::Condition>,
}

/// The various DiskPool conditions.
#[derive(AsRefStr, strum_macros::Display)]
pub(crate) enum DspCondition {
    /// Indicates whether this dsp is online and ready to be used.
    PoolReady,
}

impl DiskPoolStatus {
    fn inferred_status(pool: &Pool) -> PoolStatus {
        if let Some(ref diag) = pool.diag {
            return diag.status.into();
        }
        let Some(ref state) = pool.state else {
            return PoolStatus::Unknown;
        };

        state.status.into()
    }
    fn combine_alert_error(mut self) -> Self {
        let Some(error_info) = self.error_info.as_ref() else {
            return self;
        };

        let alerts = { error_info.alerts.attention.iter() }
            .chain(&error_info.alerts.warning)
            .chain(&error_info.alerts.critical)
            .map(|x| format!("{x:?}"))
            .collect::<Vec<_>>();

        let status = &error_info.alerts.status;
        if alerts.is_empty() {
            self.alert_error = Some(format!("{status:?}"));
        } else {
            self.alert_error = Some(format!("{status:?} ({})", alerts.join(",")));
        }

        self
    }

    /// Set when Pool is not found for some reason.
    pub fn not_found(dsp: &DiskPool, error: Option<PoolError>) -> Self {
        let status = dsp.status.as_ref();
        Self {
            cr_state: status.map(|s| s.cr_state).unwrap_or_default(),
            status: Some(PoolStatus::Unknown),
            error,
            ..Default::default()
        }
        .with_conditions(dsp)
    }

    /// Set when Pool is not found for some reason.
    pub fn disk_not_found(dsp: &DiskPool) -> Self {
        let status = dsp.status.as_ref();
        Self {
            cr_state: status.map(|s| s.cr_state).unwrap_or_default(),
            status: Some(PoolStatus::Offline),
            error: Some(PoolError {
                code: PoolErrorCode::DiskNotFound,
                message: None,
            }),
            ..Default::default()
        }
        .with_conditions(dsp)
    }

    /// Set when operator is attempting to delete on pool.
    #[cfg(feature = "openapi")]
    pub fn terminating(dsp: &DiskPool, p: Pool) -> Self {
        let status = Self::inferred_status(&p);
        let state = p.state.unwrap_or_default();
        let free = state.capacity.saturating_sub(state.used);
        Self {
            cr_state: CrPoolState::Terminating,
            pool_status: Some(state.status.into()),
            capacity: state.capacity,
            used: state.used,
            available: free,
            encrypted: Some(state.encrypted),
            capacity_q: Some(Quantity::from_bytes(state.capacity)),
            used_q: Some(Quantity::from_bytes(state.used)),
            available_q: Some(Quantity::from_bytes(free)),
            cluster_size: state.cluster_size.map(Quantity::from_bytes),
            disk_capacity: state.disk_capacity.map(Quantity::from_bytes),
            max_expandable_size: state.max_expandable_size.map(Quantity::from_bytes),
            error_info: state.error_info.map(Into::into),
            status: Some(status),
            ..Default::default()
        }
        .combine_alert_error()
        .with_conditions(dsp)
    }

    /// Set when deleting a Pool which is not accessible.
    pub fn terminating_when_unknown() -> Self {
        Self {
            cr_state: CrPoolState::Terminating,
            pool_status: Some(PoolStatus::Unknown),
            status: Some(PoolStatus::Unknown),
            error: Some(PoolError {
                code: PoolErrorCode::Unknown,
                message: Some("pool is terminating".to_string()),
            }),
            ..Default::default()
        }
    }

    fn ready_condition(&self, dsp: &DiskPool) -> meta_v1::Condition {
        // todo: what about I/O stall?
        let ready = matches!(
            self.status.unwrap_or_default(),
            PoolStatus::Online | PoolStatus::Degraded
        );
        let error_code = self.error.as_ref().map(|e| e.code).unwrap_or_default();
        let reason = if ready {
            "".to_string()
        } else {
            format!("{error_code:?}")
        };
        meta_v1::Condition {
            last_transition_time: meta_v1::Time(chrono::Utc::now()),
            // todo: build nice messages
            message: String::new(),
            observed_generation: dsp.metadata.generation,
            reason,
            status: if ready { "True" } else { "False" }.to_string(),
            type_: DspCondition::PoolReady.to_string(),
        }
    }

    /// Fill the DiskPool conditions.
    pub(crate) fn with_conditions(mut self, dsp: &DiskPool) -> Self {
        // todo: we should always have a state, we should change the upper-level api to
        //  ensure this is always the case from the signatures.
        let Some(status) = dsp.status.as_ref() else {
            return self;
        };

        let ready_cond = self.ready_condition(dsp);
        let mut conditions = status.conditions.iter();
        let Some(existing) = conditions.find(|c| c.type_ == DspCondition::PoolReady.as_ref())
        else {
            self.conditions = vec![ready_cond];
            return self;
        };

        if existing.status != ready_cond.status || existing.reason != ready_cond.reason {
            self.conditions = vec![ready_cond];
        }
        self
    }
}

#[cfg(feature = "openapi")]
impl From<RestPoolStatus> for PoolStatus {
    fn from(p: RestPoolStatus) -> Self {
        match p {
            RestPoolStatus::Unknown => Self::Unknown,
            RestPoolStatus::Offline => Self::Offline,
            RestPoolStatus::Online => Self::Online,
            RestPoolStatus::Degraded => Self::Degraded,
            RestPoolStatus::Suspected => Self::Suspected,
            RestPoolStatus::Faulted => Self::Faulted,
        }
    }
}

/// Returns DiskPoolStatus from Control plane pool object.
#[cfg(feature = "openapi")]
impl From<Pool> for DiskPoolStatus {
    fn from(p: Pool) -> Self {
        let status = Self::inferred_status(&p);
        let diag: Option<PoolDiag> = p.diag.map(Into::into);
        if let Some(state) = p.state {
            let free = state.capacity.saturating_sub(state.used);
            Self {
                cr_state: CrPoolState::Created,
                pool_status: Some(state.status.into()),
                capacity: state.capacity,
                used: state.used,
                available: free,
                encrypted: Some(state.encrypted),
                capacity_q: Some(Quantity::from_bytes(state.capacity)),
                used_q: Some(Quantity::from_bytes(state.used)),
                available_q: Some(Quantity::from_bytes(free)),
                cluster_size: Some(Quantity::from_bytes(state.cluster_size.unwrap_or(0))),
                disk_capacity: state.disk_capacity.map(Quantity::from_bytes),
                max_expandable_size: state.max_expandable_size.map(Quantity::from_bytes),
                error: diag.as_ref().and_then(|d| d.error.clone()),
                error_info: state.error_info.map(Into::into),
                diag,
                status: Some(status),
                ..Default::default()
            }
            .combine_alert_error()
        } else {
            Self {
                cr_state: CrPoolState::Created,
                status: Some(status),
                error: diag.as_ref().and_then(|d| d.error.clone()),
                diag,
                ..Default::default()
            }
        }
    }
}

#[cfg(feature = "openapi")]
impl From<openapi::models::PoolAlert> for PoolAlert {
    fn from(value: openapi::models::PoolAlert) -> Self {
        match value {
            openapi::models::PoolAlert::Unknown => Self::Unknown,
            openapi::models::PoolAlert::IoStalled => Self::IoStalled,
            openapi::models::PoolAlert::IoError => Self::IoError,
            openapi::models::PoolAlert::IoErrorExc => Self::IoErrorExc,
            openapi::models::PoolAlert::IoStallIntermittent => Self::IoStallIntermittent,
            openapi::models::PoolAlert::IoStallIntermittentExc => Self::IoStallIntermittentExc,
        }
    }
}

#[cfg(feature = "openapi")]
impl From<openapi::models::PoolErrorInfo> for PoolErrorInfo {
    fn from(value: openapi::models::PoolErrorInfo) -> Self {
        use openapi::apis::IntoVec;
        PoolErrorInfo {
            alerts: PoolAlerts {
                status: match value.alerts.status {
                    openapi::models::PoolAlertStatus::Unknown => PoolAlertStatus::Unknown,
                    openapi::models::PoolAlertStatus::Healthy => PoolAlertStatus::Healthy,
                    openapi::models::PoolAlertStatus::Attention => PoolAlertStatus::Attention,
                    openapi::models::PoolAlertStatus::Warning => PoolAlertStatus::Warning,
                    openapi::models::PoolAlertStatus::Critical => PoolAlertStatus::Critical,
                },
                notice: value.alerts.notice.into_vec(),
                attention: value.alerts.attention.into_vec(),
                warning: value.alerts.warning.into_vec(),
                critical: value.alerts.critical.into_vec(),
            },
            io_error_count: value.io_error_count,
            io_stalled: value.io_stalled,
            io_stall_transition_count: value.io_stall_transition_count,
        }
    }
}

#[cfg(feature = "openapi")]
impl From<openapi::models::PoolProbeError> for PoolError {
    fn from(value: openapi::models::PoolProbeError) -> Self {
        Self {
            code: value.code.into(),
            message: Some(value.message),
        }
    }
}

#[cfg(feature = "openapi")]
impl From<openapi::models::PoolProbeErrorCode> for PoolErrorCode {
    fn from(value: openapi::models::PoolProbeErrorCode) -> Self {
        match value {
            openapi::models::PoolProbeErrorCode::Unknown => PoolErrorCode::Unknown,
            openapi::models::PoolProbeErrorCode::DiskNotFound => PoolErrorCode::DiskNotFound,
            openapi::models::PoolProbeErrorCode::DiskReadIoError => PoolErrorCode::DiskReadIoError,
            openapi::models::PoolProbeErrorCode::ForeignPoolName => PoolErrorCode::ForeignPoolName,
            openapi::models::PoolProbeErrorCode::ForeignPoolUid => PoolErrorCode::ForeignPoolUid,
            openapi::models::PoolProbeErrorCode::SuperBlock => PoolErrorCode::SuperBlock,
            openapi::models::PoolProbeErrorCode::InvalidSuperBlock => {
                PoolErrorCode::InvalidSuperBlock
            }
            openapi::models::PoolProbeErrorCode::DiskIsADirectory => {
                PoolErrorCode::DiskIsADirectory
            }
            openapi::models::PoolProbeErrorCode::NodeIsUnknown => PoolErrorCode::NodeIsUnknown,
            openapi::models::PoolProbeErrorCode::NodeIsOffline => PoolErrorCode::NodeIsOffline,
            openapi::models::PoolProbeErrorCode::ImportDisabled => PoolErrorCode::ImportDisabled,
            openapi::models::PoolProbeErrorCode::TimeOut => PoolErrorCode::TimeOut,
            openapi::models::PoolProbeErrorCode::DiskClaimed => PoolErrorCode::DiskClaimed,
            openapi::models::PoolProbeErrorCode::PciDriverUnsupported => {
                PoolErrorCode::PciDriverUnsupported
            }
            openapi::models::PoolProbeErrorCode::PciKernelBound => PoolErrorCode::PciKernelBound,
            openapi::models::PoolProbeErrorCode::PciNotNvme => PoolErrorCode::PciNotNvme,
            openapi::models::PoolProbeErrorCode::InvalidDiskUri => PoolErrorCode::InvalidDiskUri,
        }
    }
}

#[cfg(feature = "openapi")]
impl From<openapi::models::PoolDiag> for PoolDiag {
    fn from(value: openapi::models::PoolDiag) -> Self {
        use openapi::apis::IntoVec;
        Self {
            import_errors: value.import_errors.into_vec(),
            error: value.error.map(Into::into),
            status: value.status.into(),
        }
    }
}

#[cfg(feature = "openapi")]
impl From<openapi::models::PoolDiskError> for DiskInfo {
    fn from(value: openapi::models::PoolDiskError) -> Self {
        Self {
            uri: value.disk,
            errors: value.error.into(),
        }
    }
}
