use crate::{
    common,
    context::Context,
    misc::traits::{StringValue, ValidateRequestTypes},
    pool::{
        self, get_pools_request, CordonPoolRequest, CreatePoolRequest, DestroyPoolRequest,
        ExpandPoolRequest, LabelPoolRequest, UnlabelPoolRequest,
    },
};
use prost::UnknownEnumValue;
use std::{collections::HashMap, convert::TryFrom, ops::Deref};
use stor_port::{
    transport_api::{v0::Pools, ReplyError, ResourceKind},
    types::v0::{
        store::pool::{
            CordonDrainState, CordonedState, Encryption, EncryptionSecret, PoolLabel, PoolMetadata,
            PoolRuntimeMetadata, PoolSpec, PoolSpecStatus, PoolUSpec, POOL_BS_CLUSTER_SIZE_DEFAULT,
        },
        transport::{
            CreatePool, CtrlPoolState, DestroyPool, DiskInfo, ExpandPool, Filter, LabelPool,
            NodeId, Pool, PoolAlert, PoolAlertStatus, PoolAlerts, PoolDef, PoolDeleteResult,
            PoolDeviceUri, PoolDiag, PoolDiskError, PoolError, PoolErrorCode, PoolErrorInfo,
            PoolId, PoolState, PoolStatus, SnapshotLossDetail, SnapshotLossInfo, UnlabelPool,
            VolumeId, VolumeLossDetail, VolumeLossInfo,
        },
    },
    IntoOption, IntoVec,
};

struct ExternalType<T>(T);
type ProstER<T> = ExternalType<Result<T, UnknownEnumValue>>;

impl<T: TryFrom<i32, Error = UnknownEnumValue>> ExternalType<T> {
    /// Convert a vector of enums (as i32) into a vector of `R`.
    fn from_i32vec<R: From<ProstER<T>>>(vec: Vec<i32>) -> Vec<R> {
        vec.into_iter()
            .map(|i| ExternalType::<Result<T, UnknownEnumValue>>(T::try_from(i)))
            .map(Into::into)
            .collect::<Vec<R>>()
    }
    /// Convert the `T` enum as i32 into `R`.
    fn from_i32<R: From<ProstER<T>>>(value: i32) -> R {
        ExternalType::<Result<T, UnknownEnumValue>>(T::try_from(value)).into()
    }
}

/// Error type which is returned over the transport for any operation.
#[derive(Clone, Debug)]
pub struct PoolCreateError {
    /// The generic ReplyError.
    pub error: ReplyError,
    /// Pool diagnostic information used to identify the errors, similar to the runtime information.
    pub diag: Option<PoolDiag>,
}

impl Deref for PoolCreateError {
    type Target = ReplyError;
    fn deref(&self) -> &Self::Target {
        &self.error
    }
}

impl From<tokio::task::JoinError> for PoolCreateError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self {
            error: ReplyError::aborted_error(error),
            diag: None,
        }
    }
}

impl std::fmt::Display for PoolCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.diag {
            None => write!(f, "{}", self.error),
            Some(diag) => write!(f, "{diag}"),
        }
    }
}

impl From<tonic::Status> for PoolCreateError {
    fn from(status: tonic::Status) -> Self {
        Self {
            error: status.into(),
            diag: None,
        }
    }
}

impl From<ReplyError> for PoolCreateError {
    fn from(error: ReplyError) -> Self {
        Self { error, diag: None }
    }
}

impl From<crate::common::ReplyError> for PoolCreateError {
    fn from(error: crate::common::ReplyError) -> Self {
        Self {
            error: error.into(),
            diag: None,
        }
    }
}

/// Trait implemented by services which support pool operations.
#[tonic::async_trait]
pub trait PoolOperations: Send + Sync {
    /// Create a pool
    async fn create(
        &self,
        pool: &dyn CreatePoolInfo,
        ctx: Option<Context>,
    ) -> Result<Pool, PoolCreateError>;
    /// Destroy a pool.
    /// Returns `Some(PoolDeleteResult)` for purge operations,
    /// `None` for normal deletes.
    async fn destroy(
        &self,
        pool: &dyn DestroyPoolInfo,
        ctx: Option<Context>,
    ) -> Result<Option<PoolDeleteResult>, ReplyError>;
    /// Get pools based on the filters
    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Pools, ReplyError>;
    /// Associate the labels with the given pool.
    async fn label(
        &self,
        pool: &dyn LabelPoolInfo,
        ctx: Option<Context>,
    ) -> Result<Pool, ReplyError>;
    /// Remove label from the a given pool.
    async fn unlabel(
        &self,
        pool: &dyn UnlabelPoolInfo,
        ctx: Option<Context>,
    ) -> Result<Pool, ReplyError>;
    /// Cordon the pool with the given info and associate the label with the cordoned pool.
    async fn cordon(&self, info: PoolCordonRequest) -> Result<Pool, ReplyError>;
    /// Uncordon the pool with the given info by removing the associated label.
    /// All cordon labels must be removed in order to uncordon the node.
    async fn uncordon(&self, info: PoolCordonRequest) -> Result<Pool, ReplyError>;
    /// Expands the pool to span the entire capacity of backing disk.
    async fn expand(&self, info: &dyn ExpandPoolInfo) -> Result<Pool, ReplyError>;
    /// Clears runtime errors from the specified pool.
    async fn clear_errors(&self, request: &ClearErrorsRequest) -> Result<Pool, ReplyError>;
}

impl TryFrom<pool::PoolDefinition> for PoolSpec {
    type Error = ReplyError;

    fn try_from(pool_definition: pool::PoolDefinition) -> Result<Self, Self::Error> {
        let pool_spec = match pool_definition.spec {
            Some(spec) => spec,
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Pool,
                    "pool.definition.spec",
                ))
            }
        };
        let pool_meta = match pool_definition.metadata {
            Some(meta) => meta,
            None => {
                return Err(ReplyError::missing_argument(
                    ResourceKind::Pool,
                    "pool.definition.metadata",
                ))
            }
        };
        let pool_spec_status = match common::SpecStatus::try_from(pool_meta.spec_status).ok() {
            Some(status) => status.into(),
            None => {
                return Err(ReplyError::invalid_argument(
                    ResourceKind::Pool,
                    "pool.metadata.spec_status",
                    "".to_string(),
                ))
            }
        };
        Ok(PoolSpec {
            spec: PoolUSpec {
                node: pool_spec.node_id.into(),
                id: pool_spec.pool_id.into(),
                disks: pool_spec.disks.iter().map(|i| i.into()).collect(),
                status: pool_spec_status,
                labels: match pool_spec.labels {
                    Some(labels) => Some(labels.value),
                    None => None,
                },
                sequencer: Default::default(),
                operation: None,
                creat_tsc: None,
                encryption: pool_spec
                    .secret
                    .map(|details| Encryption::Secret(EncryptionSecret { name: details.name })),
                cordon_drain: match pool_spec.cordon_drain {
                    Some(state) => match state {
                        pool::pool_spec::CordonDrain::Cordoned(state) => {
                            Some(CordonDrainState::Cordoned(CordonedState {
                                replicas: state.replicas,
                                snapshots: state.snapshots,
                                restores: state.restores,
                                import: state.import,
                            }))
                        }
                    },
                    None => None,
                },
                cluster_size: pool_spec
                    .cluster_size
                    .unwrap_or(POOL_BS_CLUSTER_SIZE_DEFAULT),
                max_expansion: None,
            },
            metadata: PoolMetadata {
                persisted: Default::default(),
                runtime: PoolRuntimeMetadata {
                    diag: None,
                    replica_count: pool_meta.repl_count,
                    snapshot_count: pool_meta.snap_count,
                },
            },
        })
    }
}

impl TryFrom<pool::PoolState> for PoolState {
    type Error = ReplyError;

    fn try_from(pool_state: pool::PoolState) -> Result<Self, Self::Error> {
        let status = pool_state.status();
        Ok(PoolState {
            node: pool_state.node_id.into(),
            id: pool_state.pool_id.into(),
            uuid: None,
            disks: pool_state.disks_uri.iter().map(|i| i.into()).collect(),
            status: status.into(),
            capacity: pool_state.capacity,
            used: pool_state.used,
            committed: pool_state.committed,
            encrypted: pool_state.encrypted.unwrap_or_default(),
            cluster_size: pool_state
                .cluster_size
                .unwrap_or(POOL_BS_CLUSTER_SIZE_DEFAULT),
            disk_capacity: pool_state.disk_capacity,
            max_expandable_size: pool_state.max_expandable_size,
            disk_info: pool_state.disk_info.into_vec(),
            errors: pool_state.errors.into_opt(),
            repl_count: pool_state.repl_count,
            snap_count: pool_state.snap_count,
        })
    }
}

impl From<pool::DiskInfo> for DiskInfo {
    fn from(value: pool::DiskInfo) -> Self {
        Self {
            uri: value.uri,
            errors: value.errors.map(Into::into).unwrap_or_default(),
        }
    }
}
impl From<DiskInfo> for pool::DiskInfo {
    fn from(value: DiskInfo) -> Self {
        Self {
            uri: value.uri,
            errors: Some(value.errors.into()),
        }
    }
}

impl From<pool::PoolErrors> for PoolErrorInfo {
    fn from(value: pool::PoolErrors) -> Self {
        Self {
            alerts: value.alerts.map(Into::into).unwrap_or_default(),
            io_error_count: value.io_error_count,
            io_error_threshold: value.io_error_threshold,
            io_stalled: value.io_stalled,
            io_stall_transition_count: value.io_stall_transition_count,
            io_stall_transition_threshold: value.io_stall_transition_threshold,
        }
    }
}
impl From<PoolErrorInfo> for pool::PoolErrors {
    fn from(value: PoolErrorInfo) -> Self {
        Self {
            alerts: Some(value.alerts.into()),
            io_error_count: value.io_error_count,
            io_error_threshold: value.io_error_threshold,
            io_stalled: value.io_stalled,
            io_stall_transition_count: value.io_stall_transition_count,
            io_stall_transition_threshold: value.io_stall_transition_threshold,
        }
    }
}

impl From<pool::PoolAlerts> for PoolAlerts {
    fn from(value: pool::PoolAlerts) -> Self {
        Self {
            status: ExternalType::from_i32(value.status),
            notice: ExternalType::from_i32vec(value.notice),
            attention: ExternalType::from_i32vec(value.attention),
            warning: ExternalType::from_i32vec(value.warning),
            critical: ExternalType::from_i32vec(value.critical),
        }
    }
}
impl From<PoolAlerts> for pool::PoolAlerts {
    fn from(value: PoolAlerts) -> Self {
        let mm = |a: Vec<PoolAlert>| -> Vec<i32> {
            a.into_iter()
                .map(pool::PoolAlert::from)
                .map(Into::into)
                .collect::<Vec<i32>>()
        };
        Self {
            status: pool::PoolAlertStatus::from(value.status) as i32,
            notice: mm(value.notice),
            attention: mm(value.attention),
            warning: mm(value.warning),
            critical: mm(value.critical),
        }
    }
}

impl From<ProstER<pool::PoolAlert>> for PoolAlert {
    fn from(value: ProstER<pool::PoolAlert>) -> Self {
        match value.0 {
            Ok(pool::PoolAlert::AlertUnknown) | Err(_) => Self::Unknown,
            Ok(pool::PoolAlert::IoStalled) => Self::IoStalled,
            Ok(pool::PoolAlert::IoStallIntermittent) => Self::IoStallIntermittent,
            Ok(pool::PoolAlert::IoStallIntermittentExc) => Self::IoStallIntermittentExc,
            Ok(pool::PoolAlert::IoError) => Self::IoError,
            Ok(pool::PoolAlert::IoErrorExc) => Self::IoErrorExc,
        }
    }
}
impl From<PoolAlert> for pool::PoolAlert {
    fn from(value: PoolAlert) -> Self {
        match value {
            PoolAlert::Unknown => Self::AlertUnknown,
            PoolAlert::IoStalled => Self::IoStalled,
            PoolAlert::IoStallIntermittent => Self::IoStallIntermittent,
            PoolAlert::IoStallIntermittentExc => Self::IoStallIntermittentExc,
            PoolAlert::IoError => Self::IoError,
            PoolAlert::IoErrorExc => Self::IoErrorExc,
        }
    }
}

impl From<ProstER<pool::PoolAlertStatus>> for PoolAlertStatus {
    fn from(value: ProstER<pool::PoolAlertStatus>) -> Self {
        match value.0 {
            Ok(pool::PoolAlertStatus::Healthy) => Self::Healthy,
            Ok(pool::PoolAlertStatus::Attention) => Self::Attention,
            Ok(pool::PoolAlertStatus::Warning) => Self::Warning,
            Ok(pool::PoolAlertStatus::Critical) => Self::Critical,
            Ok(pool::PoolAlertStatus::StatusUnknown) | Err(_) => Self::Unknown,
        }
    }
}
impl From<PoolAlertStatus> for pool::PoolAlertStatus {
    fn from(value: PoolAlertStatus) -> Self {
        match value {
            PoolAlertStatus::Healthy => Self::Healthy,
            PoolAlertStatus::Attention => Self::Attention,
            PoolAlertStatus::Warning => Self::Warning,
            PoolAlertStatus::Critical => Self::Critical,
            PoolAlertStatus::Unknown => Self::StatusUnknown,
        }
    }
}

fn pool_with_diag(mut pool_spec: PoolSpec, diag: Option<pool::PoolDiag>) -> PoolSpec {
    if let Some(diag) = diag {
        pool_spec.metadata.runtime.diag = Some(diag.into());
    }
    pool_spec
}

impl TryFrom<pool::Pool> for Pool {
    type Error = ReplyError;
    fn try_from(pool: pool::Pool) -> Result<Self, Self::Error> {
        let state = match pool.state {
            None => None,
            Some(state) => {
                let state = PoolState::try_from(state)?;
                Some(CtrlPoolState::new(state))
            }
        };

        let pool_spec = match pool.definition {
            None => None,
            Some(pool_definition) => Some(pool_with_diag(
                PoolSpec::try_from(pool_definition)?,
                pool.diag,
            )),
        };

        match Pool::try_new(pool_spec, state) {
            Some(pool) => Ok(pool),
            None => Err(ReplyError::missing_argument(
                ResourceKind::Pool,
                "pool.spec and pool.state",
            )),
        }
    }
}

impl From<PoolUSpec> for pool::PoolDefinition {
    fn from(spec: PoolUSpec) -> Self {
        PoolDef {
            spec,
            ..Default::default()
        }
        .into()
    }
}

impl From<PoolDef> for pool::PoolDefinition {
    fn from(pool_def: PoolDef) -> Self {
        let pool_spec = pool_def.spec;
        let spec_status: common::SpecStatus = pool_spec.status.into();
        pool::PoolDefinition {
            spec: Some(pool::PoolSpec {
                node_id: pool_spec.node.to_string(),
                pool_id: pool_spec.id.to_string(),
                disks: pool_spec.disks.iter().map(|i| i.to_string()).collect(),
                labels: pool_spec
                    .labels
                    .map(|labels| crate::common::StringMapValue { value: labels }),
                secret: match pool_spec.encryption {
                    None => None,
                    Some(config) => match config {
                        Encryption::Secret(details) => {
                            Some(common::EncryptionSecret { name: details.name })
                        }
                    },
                },
                cordon_drain: match pool_spec.cordon_drain {
                    Some(cordon_drain) => {
                        let co = match cordon_drain {
                            CordonDrainState::Cordoned(state) => pool::CordonedState {
                                replicas: state.replicas,
                                snapshots: state.snapshots,
                                restores: state.restores,
                                import: state.import,
                            },
                        };
                        Some(pool::pool_spec::CordonDrain::Cordoned(co))
                    }
                    None => None,
                },
                cluster_size: Some(pool_spec.cluster_size),
            }),
            metadata: Some(pool::Metadata {
                uuid: None,
                spec_status: spec_status as i32,
                repl_count: pool_def.replica_count,
                snap_count: pool_def.snapshot_count,
            }),
        }
    }
}

impl From<PoolState> for pool::PoolState {
    fn from(pool_state: PoolState) -> Self {
        pool::PoolState {
            node_id: pool_state.node.to_string(),
            pool_id: pool_state.id.to_string(),
            disks_uri: pool_state.disks.iter().map(|i| i.to_string()).collect(),
            status: pool_state.status as i32,
            capacity: pool_state.capacity,
            used: pool_state.used,
            committed: pool_state.committed,
            encrypted: Some(pool_state.encrypted),
            cluster_size: Some(pool_state.cluster_size),
            disk_capacity: pool_state.disk_capacity,
            max_expandable_size: pool_state.max_expandable_size,
            disk_info: pool_state.disk_info.into_vec(),
            errors: pool_state.errors.into_opt(),
            repl_count: pool_state.repl_count,
            snap_count: pool_state.snap_count,
        }
    }
}

impl From<PoolErrorCode> for pool::ProbeErrorCode {
    fn from(value: PoolErrorCode) -> Self {
        match value {
            PoolErrorCode::Unknown => Self::ProbeUnknown,
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
impl From<PoolDiag> for pool::PoolDiag {
    fn from(diag: PoolDiag) -> Self {
        let import_error = |value: PoolDiskError| pool::DiskError {
            error: Some(pool::ProbeError {
                code: pool::ProbeErrorCode::from(value.error.code) as i32,
                msg: value.error.msg,
            }),
            disk: value.disk,
        };
        Self {
            import_errors: diag.import_errors.into_iter().map(import_error).collect(),
            status: pool::PoolStatus::from(diag.status) as i32,
            error: diag.error.into_opt(),
        }
    }
}
impl From<pool::ProbeErrorCode> for PoolErrorCode {
    fn from(value: pool::ProbeErrorCode) -> Self {
        match value {
            pool::ProbeErrorCode::ProbeUnknown => Self::Unknown,
            pool::ProbeErrorCode::DiskNotFound => Self::DiskNotFound,
            pool::ProbeErrorCode::DiskReadIoError => Self::DiskReadIoError,
            pool::ProbeErrorCode::ForeignPoolName => Self::ForeignPoolName,
            pool::ProbeErrorCode::ForeignPoolUid => Self::ForeignPoolUid,
            pool::ProbeErrorCode::SuperBlockIoError => Self::SuperBlockIoError,
            pool::ProbeErrorCode::InvalidSuperBlock => Self::InvalidSuperBlock,
            pool::ProbeErrorCode::DiskIsADirectory => Self::DiskIsADirectory,
            pool::ProbeErrorCode::NodeIsUnknown => Self::NodeIsUnknown,
            pool::ProbeErrorCode::NodeIsOffline => Self::NodeIsOffline,
            pool::ProbeErrorCode::ImportDisabled => Self::ImportDisabled,
            pool::ProbeErrorCode::TimeOut => Self::TimeOut,
            pool::ProbeErrorCode::Aborted => Self::Aborted,
            pool::ProbeErrorCode::DiskClaimed => Self::DiskClaimed,
            pool::ProbeErrorCode::PciDriverUnsupported => Self::PCIDriverUnsupported,
            pool::ProbeErrorCode::PciKernelBound => Self::PCIKernelBound,
            pool::ProbeErrorCode::PciNotNvme => Self::PCINotNvme,
            pool::ProbeErrorCode::InvalidDiskUri => Self::InvalidDiskUri,
            pool::ProbeErrorCode::DiskNotImportable => Self::DiskNotImportable,
            pool::ProbeErrorCode::UriNotHandled => Self::UriNotHandled,
        }
    }
}
impl From<pool::PoolDiag> for PoolDiag {
    fn from(diag: pool::PoolDiag) -> Self {
        let import_error = |value: pool::DiskError| PoolDiskError {
            error: value.error.unwrap_or_default().into(),
            disk: value.disk,
        };
        Self {
            status: diag.status().into(),
            error: diag.error.into_opt(),
            import: Default::default(),
            import_errors: diag.import_errors.into_iter().map(import_error).collect(),
        }
    }
}

impl From<pool::ProbeError> for PoolError {
    fn from(value: pool::ProbeError) -> Self {
        PoolError {
            code: PoolErrorCode::from(value.code()),
            msg: value.msg,
        }
    }
}
impl From<PoolError> for pool::ProbeError {
    fn from(value: PoolError) -> Self {
        Self {
            code: pool::ProbeErrorCode::from(value.code) as i32,
            msg: value.msg,
        }
    }
}

impl From<Pool> for pool::Pool {
    fn from(pool: Pool) -> Self {
        let state = pool.state;
        let (def, diag) = match pool.config {
            None => (None, None),
            Some(cfg) => (Some(cfg.definition), cfg.diag),
        };
        pool::Pool {
            definition: def.into_opt(),
            state: state.map(|p| p.state).into_opt(),
            diag: diag.into_opt(),
        }
    }
}

impl TryFrom<pool::Pools> for Pools {
    type Error = ReplyError;
    fn try_from(grpc_pool_type: pool::Pools) -> Result<Self, Self::Error> {
        let mut pools: Vec<Pool> = vec![];
        for pool in grpc_pool_type.pools {
            pools.push(Pool::try_from(pool.clone())?)
        }
        Ok(Pools(pools))
    }
}

impl From<Pools> for pool::Pools {
    fn from(pools: Pools) -> Self {
        pool::Pools {
            pools: pools
                .into_inner()
                .iter()
                .map(|pool| pool.clone().into())
                .collect(),
        }
    }
}

impl TryFrom<get_pools_request::Filter> for Filter {
    type Error = ReplyError;
    fn try_from(filter: get_pools_request::Filter) -> Result<Self, Self::Error> {
        Ok(match filter {
            get_pools_request::Filter::Common(common_filter) => Filter::Volume(VolumeId::try_from(
                StringValue(Some(common_filter.volume_id)),
            )?),
            get_pools_request::Filter::Node(node_filter) => {
                Filter::Node(node_filter.node_id.into())
            }
            get_pools_request::Filter::NodePool(node_pool_filter) => Filter::NodePool(
                node_pool_filter.node_id.into(),
                node_pool_filter.pool_id.into(),
            ),
            get_pools_request::Filter::Pool(pool_filter) => {
                Filter::Pool(pool_filter.pool_id.into())
            }
        })
    }
}

/// CreatePoolInfo trait for the pool creation to be implemented by entities which want to avail
/// this operation
pub trait CreatePoolInfo: Send + Sync + std::fmt::Debug {
    /// Id of the pool.
    fn pool_id(&self) -> PoolId;
    /// Id of the IoEngine instance.
    fn node_id(&self) -> NodeId;
    /// Disk device paths or URIs to be claimed by the pool.
    fn disks(&self) -> Vec<PoolDeviceUri>;
    /// Labels to be set on the pool.
    fn labels(&self) -> Option<PoolLabel>;
    /// Encryption parameters for the pool.
    fn encryption(&self) -> Option<Encryption>;
    /// Requested cluster size for blobstore.
    fn cluster_size(&self) -> Option<u32>;
    /// Maximum expansion size for this pool.
    fn max_expansion(&self) -> Option<String>;
}

/// DestroyPoolInfo trait for the pool deletion to be implemented by entities which want to avail
/// this operation
pub trait DestroyPoolInfo: Sync + Send + std::fmt::Debug {
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
    /// Id of the IoEngine instance
    fn node_id(&self) -> NodeId;
    /// Whether to purge (delete specs without contacting io-engine).
    fn purge(&self) -> bool;
    /// Accept deletion when pool has replicas.
    fn accept(&self) -> bool;
    /// Accept volume loss (volumes losing last healthy replica).
    fn accept_volume_loss(&self) -> bool;
    /// Accept snapshot loss (snapshots losing last replica snapshot).
    fn accept_snapshot_loss(&self) -> bool;
}

impl DestroyPoolInfo for Pool {
    fn pool_id(&self) -> PoolId {
        self.id().clone()
    }
    fn node_id(&self) -> NodeId {
        self.node()
    }
    fn purge(&self) -> bool {
        false
    }
    fn accept(&self) -> bool {
        false
    }
    fn accept_volume_loss(&self) -> bool {
        false
    }
    fn accept_snapshot_loss(&self) -> bool {
        false
    }
}

impl CreatePoolInfo for CreatePool {
    fn pool_id(&self) -> PoolId {
        self.id.clone()
    }

    fn node_id(&self) -> NodeId {
        self.node.clone()
    }

    fn disks(&self) -> Vec<PoolDeviceUri> {
        self.disks.clone()
    }

    fn labels(&self) -> Option<PoolLabel> {
        self.labels.clone()
    }

    fn encryption(&self) -> Option<Encryption> {
        self.encryption.clone()
    }

    fn cluster_size(&self) -> Option<u32> {
        self.cluster_size
    }

    fn max_expansion(&self) -> Option<String> {
        self.max_expansion.clone()
    }
}

/// Intermediate structure that validates the conversion to CreatePoolRequest type.
#[derive(Debug)]
pub struct ValidatedCreatePoolRequest {
    inner: CreatePoolRequest,
    encryption: Option<Encryption>,
}

impl CreatePoolInfo for ValidatedCreatePoolRequest {
    fn pool_id(&self) -> PoolId {
        self.inner.pool_id.clone().into()
    }

    fn node_id(&self) -> NodeId {
        self.inner.node_id.clone().into()
    }

    fn disks(&self) -> Vec<PoolDeviceUri> {
        self.inner.disks.iter().map(|disk| disk.into()).collect()
    }

    fn labels(&self) -> Option<PoolLabel> {
        match self.inner.labels.clone() {
            None => None,
            Some(labels) => Some(labels.value),
        }
    }

    fn encryption(&self) -> Option<Encryption> {
        self.encryption.clone()
    }

    fn cluster_size(&self) -> Option<u32> {
        self.inner.cluster_size
    }

    fn max_expansion(&self) -> Option<String> {
        self.inner.max_expansion.clone()
    }
}

impl ValidateRequestTypes for CreatePoolRequest {
    type Validated = ValidatedCreatePoolRequest;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(ValidatedCreatePoolRequest {
            encryption: match self.encryption.clone() {
                None => None,
                Some(encryption) => match encryption {
                    pool::create_pool_request::Encryption::Secret(secret) => {
                        Some(Encryption::Secret(secret.into()))
                    }
                },
            },
            inner: self,
        })
    }
}

impl From<&dyn CreatePoolInfo> for CreatePoolRequest {
    fn from(data: &dyn CreatePoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().to_string(),
            node_id: data.node_id().to_string(),
            disks: data.disks().iter().map(|disk| disk.to_string()).collect(),
            labels: data
                .labels()
                .map(|labels| crate::common::StringMapValue { value: labels }),
            encryption: data.encryption().into_opt(),
            cluster_size: data.cluster_size(),
            max_expansion: data.max_expansion(),
        }
    }
}

impl From<&dyn CreatePoolInfo> for CreatePool {
    fn from(data: &dyn CreatePoolInfo) -> Self {
        Self {
            node: data.node_id(),
            id: data.pool_id(),
            disks: data.disks(),
            labels: data.labels(),
            encryption: data.encryption(),
            cluster_size: data.cluster_size(),
            max_expansion: data.max_expansion(),
        }
    }
}

impl DestroyPoolInfo for DestroyPool {
    fn pool_id(&self) -> PoolId {
        self.id.clone()
    }
    fn node_id(&self) -> NodeId {
        self.node.clone()
    }
    fn purge(&self) -> bool {
        self.purge
    }
    fn accept(&self) -> bool {
        self.accept
    }
    fn accept_volume_loss(&self) -> bool {
        self.accept_volume_loss
    }
    fn accept_snapshot_loss(&self) -> bool {
        self.accept_snapshot_loss
    }
}

impl DestroyPoolInfo for DestroyPoolRequest {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone().into()
    }
    fn node_id(&self) -> NodeId {
        self.node_id.clone().into()
    }
    fn purge(&self) -> bool {
        self.purge.unwrap_or(false)
    }
    fn accept(&self) -> bool {
        self.accept.unwrap_or(false)
    }
    fn accept_volume_loss(&self) -> bool {
        self.accept_volume_loss.unwrap_or(false)
    }
    fn accept_snapshot_loss(&self) -> bool {
        self.accept_snapshot_loss.unwrap_or(false)
    }
}

impl From<&dyn DestroyPoolInfo> for DestroyPoolRequest {
    fn from(data: &dyn DestroyPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().to_string(),
            node_id: data.node_id().to_string(),
            purge: Some(data.purge()),
            accept: Some(data.accept()),
            accept_volume_loss: Some(data.accept_volume_loss()),
            accept_snapshot_loss: Some(data.accept_snapshot_loss()),
        }
    }
}

impl From<&dyn DestroyPoolInfo> for DestroyPool {
    fn from(data: &dyn DestroyPoolInfo) -> Self {
        Self {
            node: data.node_id(),
            id: data.pool_id(),
            purge: data.purge(),
            accept: data.accept(),
            accept_volume_loss: data.accept_volume_loss(),
            accept_snapshot_loss: data.accept_snapshot_loss(),
        }
    }
}

/// ExpandPoolInfo trait for the pool expansion to be implemented by entities which want to avail
/// this operation.
pub trait ExpandPoolInfo: Sync + Send + std::fmt::Debug {
    /// Id of the pool
    fn pool_id(&self) -> PoolId;
}

impl ExpandPoolInfo for ExpandPool {
    fn pool_id(&self) -> PoolId {
        self.id.clone()
    }
}

impl ExpandPoolInfo for ExpandPoolRequest {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone().into()
    }
}

impl From<&dyn ExpandPoolInfo> for ExpandPoolRequest {
    fn from(data: &dyn ExpandPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().clone().into(),
        }
    }
}

impl From<&dyn ExpandPoolInfo> for ExpandPool {
    fn from(data: &dyn ExpandPoolInfo) -> Self {
        Self { id: data.pool_id() }
    }
}

impl From<pool::PoolStatus> for PoolStatus {
    fn from(src: pool::PoolStatus) -> Self {
        match src {
            pool::PoolStatus::Online => Self::Online,
            pool::PoolStatus::Degraded => Self::Degraded,
            pool::PoolStatus::Suspected => Self::Suspected,
            pool::PoolStatus::Faulted => Self::Faulted,
            pool::PoolStatus::Unknown => Self::Unknown,
            pool::PoolStatus::Offline => Self::Offline,
        }
    }
}

impl From<PoolStatus> for pool::PoolStatus {
    fn from(pool_status: PoolStatus) -> Self {
        match pool_status {
            PoolStatus::Unknown => Self::Unknown,
            PoolStatus::Offline => Self::Offline,
            PoolStatus::Online => Self::Online,
            PoolStatus::Degraded => Self::Degraded,
            PoolStatus::Suspected => Self::Suspected,
            PoolStatus::Faulted => Self::Faulted,
        }
    }
}

impl From<common::SpecStatus> for PoolSpecStatus {
    fn from(src: common::SpecStatus) -> Self {
        match src {
            common::SpecStatus::Created => Self::Created(Default::default()),
            common::SpecStatus::Creating => Self::Creating,
            common::SpecStatus::Deleted => Self::Deleted,
            common::SpecStatus::Deleting => Self::Deleting,
            common::SpecStatus::Purging => Self::Purging,
        }
    }
}

impl From<PoolSpecStatus> for common::SpecStatus {
    fn from(src: PoolSpecStatus) -> Self {
        match src {
            PoolSpecStatus::Creating => Self::Creating,
            PoolSpecStatus::Created(_) => Self::Created,
            PoolSpecStatus::Deleting => Self::Deleting,
            PoolSpecStatus::Purging => Self::Purging,
            PoolSpecStatus::Deleted => Self::Deleted,
        }
    }
}

/// LabelPoolInfo trait for the pool labeling to be implemented by entities which want
/// to avail this operation
pub trait LabelPoolInfo: Send + Sync + std::fmt::Debug {
    /// Id of the pool.
    fn pool_id(&self) -> PoolId;
    /// Labels to be set on the pool.
    fn labels(&self) -> HashMap<String, String>;
    /// Overwrite the existing labels.
    fn overwrite(&self) -> bool;
}

impl LabelPoolInfo for LabelPool {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }

    fn labels(&self) -> HashMap<String, String> {
        self.labels.clone()
    }

    fn overwrite(&self) -> bool {
        self.overwrite
    }
}

impl LabelPoolInfo for LabelPoolRequest {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone().into()
    }

    fn labels(&self) -> HashMap<String, String> {
        self.labels.clone()
    }

    fn overwrite(&self) -> bool {
        self.overwrite
    }
}

impl From<&dyn LabelPoolInfo> for LabelPoolRequest {
    fn from(data: &dyn LabelPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().to_string(),
            labels: data.labels().clone(),
            overwrite: data.overwrite(),
        }
    }
}

impl From<&dyn LabelPoolInfo> for LabelPool {
    fn from(data: &dyn LabelPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id(),
            labels: data.labels(),
            overwrite: data.overwrite(),
        }
    }
}

/// UnlabelPoolInfo trait for the pool unlabeling to be implemented by entities which want to avail
/// this operation
pub trait UnlabelPoolInfo: Send + Sync + std::fmt::Debug {
    /// Id of the pool.
    fn pool_id(&self) -> PoolId;
    /// Key of the label to be removed.
    fn label_key(&self) -> String;
}

impl UnlabelPoolInfo for UnlabelPool {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone()
    }
    fn label_key(&self) -> String {
        self.label_key.clone()
    }
}

impl UnlabelPoolInfo for UnlabelPoolRequest {
    fn pool_id(&self) -> PoolId {
        self.pool_id.clone().into()
    }
    fn label_key(&self) -> String {
        self.label_key.clone()
    }
}

impl From<&dyn UnlabelPoolInfo> for UnlabelPoolRequest {
    fn from(data: &dyn UnlabelPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id().to_string(),
            label_key: data.label_key().clone(),
        }
    }
}

impl From<&dyn UnlabelPoolInfo> for UnlabelPool {
    fn from(data: &dyn UnlabelPoolInfo) -> Self {
        Self {
            pool_id: data.pool_id(),
            label_key: data.label_key(),
        }
    }
}

impl From<Encryption> for pool::create_pool_request::Encryption {
    fn from(value: Encryption) -> Self {
        match value {
            Encryption::Secret(secret) => {
                pool::create_pool_request::Encryption::Secret(secret.into())
            }
        }
    }
}

impl From<EncryptionSecret> for common::EncryptionSecret {
    fn from(value: EncryptionSecret) -> Self {
        Self { name: value.name }
    }
}

impl From<common::EncryptionSecret> for EncryptionSecret {
    fn from(value: common::EncryptionSecret) -> Self {
        Self { name: value.name }
    }
}

/// Pool cordon and uncordon information.
#[derive(Debug)]
pub struct PoolCordonRequest {
    /// Node ID of where the pool resides on.
    /// This is optional and may be used for stricter checks.
    pub node_id: Option<NodeId>,
    /// The ID of the pool to cordon/uncordon.
    pub pool_id: PoolId,
    /// Cordon or uncordon replicas.
    pub replicas: bool,
    /// Cordon or uncordon snapshots.
    pub snapshots: bool,
    /// Cordon or uncordon restores.
    pub restores: bool,
    /// Importing the pool after node/engine restart.
    pub import: bool,
}

impl From<CordonPoolRequest> for PoolCordonRequest {
    fn from(value: CordonPoolRequest) -> Self {
        Self {
            node_id: value.node_id.map(Into::into),
            pool_id: value.pool_id.into(),
            replicas: value.replicas,
            snapshots: value.snapshots,
            restores: value.restores,
            import: value.import,
        }
    }
}
impl From<PoolCordonRequest> for CordonPoolRequest {
    fn from(value: PoolCordonRequest) -> Self {
        Self {
            pool_id: value.pool_id.into(),
            node_id: value.node_id.map(Into::into),
            replicas: value.replicas,
            snapshots: value.snapshots,
            restores: value.restores,
            import: value.import,
        }
    }
}

/// Pool clear errors request.
#[derive(Debug, Clone, Default)]
pub struct ClearErrorsRequest {
    /// Node ID of where the pool resides on.
    /// This is optional and may be used for stricter checks.
    pub node_id: Option<NodeId>,
    /// The ID of the pool to clear errors from.
    pub pool_id: PoolId,
    /// If one or more disks is specified, only those errors associated with the
    /// specified disk or disks are cleared.
    pub disks: Vec<String>,
    /// Error clearing request
    pub clear: ClearErrors,
}

impl ClearErrorsRequest {
    /// Create a new `Self`.
    pub fn new(pool_id: PoolId) -> Self {
        Self {
            pool_id,
            ..Default::default()
        }
    }
    /// Create a new `Self`.
    pub fn new_ext(pool_id: PoolId, disks: Vec<String>, clear: ClearErrors) -> Self {
        Self {
            pool_id,
            disks,
            clear,
            ..Default::default()
        }
    }
}

/// Clear errors variants.
#[derive(Debug, Clone, Copy, Default)]
pub enum ClearErrors {
    /// Clears all counted errors and stall transitions.
    /// Note: It doesn't clear an io stall state if the pool is currently in a stalled state.
    /// The stall state will be cleared when the pool is no longer stalled.
    #[default]
    All,
    /// Clears only the counted I/O errors.
    IoErrors,
    /// Clears only the I/O stall transition count.
    IoStallTransitions,
}

impl From<pool::ClearErrorsRequest> for ClearErrorsRequest {
    fn from(value: pool::ClearErrorsRequest) -> Self {
        Self {
            clear: value.clear().into(),
            node_id: value.node_id.into_opt(),
            pool_id: value.pool_id.into(),
            disks: value.disks,
        }
    }
}
impl From<&ClearErrorsRequest> for pool::ClearErrorsRequest {
    fn from(value: &ClearErrorsRequest) -> Self {
        Self {
            node_id: value.node_id.as_ref().map(|n| n.to_string()),
            pool_id: value.pool_id.to_string(),
            clear: pool::ClearErrors::from(value.clear) as i32,
            disks: value.disks.clone(),
        }
    }
}

impl From<VolumeLossInfo> for pool::VolumeLossInfo {
    fn from(info: VolumeLossInfo) -> Self {
        Self {
            volumes: info.volumes.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<pool::ClearErrors> for ClearErrors {
    fn from(value: pool::ClearErrors) -> Self {
        match value {
            pool::ClearErrors::ClearAll => Self::All,
            pool::ClearErrors::ClearIoErrors => Self::IoErrors,
            pool::ClearErrors::ClearIoStallTransitions => Self::IoStallTransitions,
        }
    }
}
impl From<ClearErrors> for pool::ClearErrors {
    fn from(value: ClearErrors) -> Self {
        match value {
            ClearErrors::All => Self::ClearAll,
            ClearErrors::IoErrors => Self::ClearIoErrors,
            ClearErrors::IoStallTransitions => Self::ClearIoStallTransitions,
        }
    }
}

impl From<PoolDeleteResult> for pool::PoolDeleteResult {
    fn from(result: PoolDeleteResult) -> Self {
        Self {
            pool_id: result.pool_id.to_string(),
            volume_loss: Some(result.volume_loss.into()),
            snapshot_loss: Some(result.snapshot_loss.into()),
        }
    }
}
impl From<VolumeLossDetail> for pool::VolumeLossDetail {
    fn from(detail: VolumeLossDetail) -> Self {
        Self {
            volume_id: detail.volume_id.to_string(),
            replicas_before: detail.replicas_before,
            healthy_before: detail.healthy_before,
            lost_on_pool: detail.lost_on_pool,
            healthy_after: detail.healthy_after,
        }
    }
}

impl From<SnapshotLossInfo> for pool::SnapshotLossInfo {
    fn from(info: SnapshotLossInfo) -> Self {
        Self {
            snapshots: info.snapshots.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<SnapshotLossDetail> for pool::SnapshotLossDetail {
    fn from(detail: SnapshotLossDetail) -> Self {
        Self {
            snapshot_id: detail.snapshot_id.to_string(),
            replica_snapshots_before: detail.replica_snapshots_before,
            healthy_before: detail.healthy_before,
            lost_on_pool: detail.lost_on_pool,
            healthy_after: detail.healthy_after,
        }
    }
}

impl TryFrom<pool::PoolDeleteResult> for PoolDeleteResult {
    type Error = ReplyError;

    fn try_from(result: pool::PoolDeleteResult) -> Result<Self, Self::Error> {
        let volume_loss = match result.volume_loss {
            Some(volume_loss) => {
                let mut volumes = Vec::new();
                for v in volume_loss.volumes {
                    let volume_id = uuid::Uuid::parse_str(&v.volume_id).map_err(|_| {
                        ReplyError::invalid_argument(
                            ResourceKind::Volume,
                            "volume_id",
                            v.volume_id.clone(),
                        )
                    })?;
                    volumes.push(VolumeLossDetail {
                        volume_id: volume_id.into(),
                        replicas_before: v.replicas_before,
                        healthy_before: v.healthy_before,
                        lost_on_pool: v.lost_on_pool,
                        healthy_after: v.healthy_after,
                    });
                }
                VolumeLossInfo { volumes }
            }
            None => VolumeLossInfo::default(),
        };

        let snapshot_loss = match result.snapshot_loss {
            Some(snapshot_loss) => {
                let mut snapshots = Vec::new();
                for s in snapshot_loss.snapshots {
                    let snapshot_id = uuid::Uuid::parse_str(&s.snapshot_id).map_err(|_| {
                        ReplyError::invalid_argument(
                            ResourceKind::VolumeSnapshot,
                            "snapshot_id",
                            s.snapshot_id.clone(),
                        )
                    })?;
                    snapshots.push(SnapshotLossDetail {
                        snapshot_id: snapshot_id.into(),
                        replica_snapshots_before: s.replica_snapshots_before,
                        healthy_before: s.healthy_before,
                        lost_on_pool: s.lost_on_pool,
                        healthy_after: s.healthy_after,
                    });
                }
                SnapshotLossInfo { snapshots }
            }
            None => SnapshotLossInfo::default(),
        };

        Ok(PoolDeleteResult {
            pool_id: result.pool_id.into(),
            volume_loss,
            snapshot_loss,
        })
    }
}
