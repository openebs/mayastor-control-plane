use super::*;

use serde::{Deserialize, Serialize};
use std::{fmt::Debug, str::FromStr};

use crate::{types::v0::store::node::NodeSpec, IntoOption};
use strum_macros::{Display, EnumString};

/// Registration
///
/// Register message payload
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Register {
    /// Node Id of the io-engine instance.
    pub id: NodeId,
    /// Grpc endpoint of the io-engine instance.
    pub grpc_endpoint: std::net::SocketAddr,
    /// Api versions registered by the dataplane.
    pub api_versions: Option<Vec<ApiVersion>>,
    /// Used to identify dataplane process restarts.
    pub instance_uuid: Option<uuid::Uuid>,
    /// Used to identify dataplane nvme hostnqn.
    pub node_nqn: Option<HostNqn>,
    /// Features exposed by the io-engine.
    pub features: Option<NodeFeatures>,
    /// BugFixes exposed by the io-engine.
    pub bugfixes: Option<NodeBugFixes>,
    /// Version of the io-engine.
    pub version: Option<String>,
}

/// Deregister message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Deregister {
    /// Node Id of the io-engine instance.
    pub id: NodeId,
}

/// Node Service
///
/// Get storage nodes by filter.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetNodes {
    filter: Filter,
    ignore_notfound: bool,
}

impl GetNodes {
    /// New get nodes request.
    pub fn new(filter: Filter, ignore_notfound: bool) -> Self {
        Self {
            filter,
            ignore_notfound,
        }
    }
    /// Return `Self` to request all nodes (`None`) or a specific node (`NodeId`).
    pub fn from(node_id: impl Into<Option<NodeId>>) -> Self {
        let node_id = node_id.into();
        Self {
            filter: node_id.map_or(Filter::None, Filter::Node),
            ignore_notfound: true,
        }
    }
    /// Get the inner `Filter`.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }
    /// Check to ignore error when not found.
    pub fn ignore_notfound(&self) -> bool {
        self.ignore_notfound
    }
}

/// User configuration with user specification and metadata information.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeConfig {
    /// Specification of the node.
    pub spec: NodeSpec,
    /// Node resource counts.
    pub resources: Option<NodeRscCounts>,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeRscCounts {
    /// How many pools are owned by the node.
    pub pool_count: u64,
    /// How many replicas are owned by pools on the node.
    pub replica_count: u64,
    /// How many snapshots are owned by pools on the node.
    pub snapshot_count: u64,
}
impl From<NodeSpec> for NodeConfig {
    fn from(spec: NodeSpec) -> Self {
        Self {
            resources: Some(NodeRscCounts {
                pool_count: spec.metadata.runtime.pool_count,
                replica_count: spec.metadata.runtime.replica_count,
                snapshot_count: spec.metadata.runtime.snapshot_count,
            }),
            spec,
        }
    }
}

/// Node information
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    /// Node identification
    id: NodeId,
    /// [`NodeConfig`].
    config: Option<NodeConfig>,
    /// Runtime state of the node.
    state: Option<NodeState>,
}

impl Node {
    /// Get new `Self` from the given parameters
    pub fn new(id: NodeId, spec: Option<NodeSpec>, state: Option<NodeState>) -> Self {
        Self {
            id,
            config: spec.map(NodeConfig::from),
            state,
        }
    }
    /// Add the given node resource tallies.
    pub fn with_rsc(mut self, rsc: Option<NodeRscCounts>) -> Self {
        if let Some(ref mut config) = self.config.as_mut() {
            config.resources = rsc;
        }
        self
    }
    /// Get the node id
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    /// Get perceived status of the node.
    /// Either the status as determined from the state or the perceived status via
    /// the node spec shutdown flag.
    pub fn status(&self) -> NodeStatus {
        match &self.state {
            Some(state) => state.status,
            None => match self.spec() {
                Some(spec) if spec.is_shutdown() => NodeStatus::Offline,
                _ => NodeStatus::Unknown,
            },
        }
    }
    pub fn take_config(&mut self) -> Option<NodeConfig> {
        self.config.take()
    }
    /// Get the node specification
    pub fn spec(&self) -> Option<&NodeSpec> {
        self.config.as_ref().map(|config| &config.spec)
    }
    /// Get the node specification
    pub fn spec_mut(&mut self) -> Option<&mut NodeSpec> {
        self.config.as_mut().map(|config| &mut config.spec)
    }
    /// Get the node runtime state
    pub fn state(&self) -> Option<&NodeState> {
        self.state.as_ref()
    }
    /// Set the shutdown flag.
    pub fn with_shutdown(mut self, shutdown: bool) -> Self {
        if let Some(ref mut spec) = self.spec_mut() {
            spec.set_shutdown(shutdown);
        }
        self
    }
    /// Get a reference to the node resource counts.
    pub fn tallies(&self) -> Option<&NodeRscCounts> {
        self.config.as_ref().and_then(|c| c.resources.as_ref())
    }
}

impl From<Node> for models::Node {
    fn from(mut src: Node) -> Self {
        let status = src.status();
        let (spec, meta) = match src.take_config() {
            None => (None, None),
            Some(config) => (Some(config.spec), config.resources),
        };
        Self::new_all(
            src.id,
            spec.map(Into::into),
            meta.map(Into::into),
            src.state.map(Into::into),
            Some(status.into()),
        )
    }
}

impl From<NodeRscCounts> for models::NodeMeta {
    fn from(value: NodeRscCounts) -> Self {
        Self {
            tallies: models::NodeRscTallies {
                pool_count: value.pool_count,
                replica_count: value.replica_count,
                snapshot_count: value.snapshot_count,
            },
        }
    }
}

/// Status of the Node
#[derive(
    Serialize, Deserialize, Debug, Default, Clone, Copy, EnumString, Display, Eq, PartialEq,
)]
pub enum NodeStatus {
    /// Node has unexpectedly disappeared
    #[default]
    Unknown,
    /// Node is deemed online if it has not missed the
    /// registration keep alive deadline
    Online,
    /// Node is deemed offline if has missed the
    /// registration keep alive deadline
    Offline,
}

/// Node features as exposed by the node io-engine.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeFeatures {
    /// NVMe ANA is enabled.
    pub asymmetric_namespace_access: Option<bool>,
    /// LVM backend is enabled.
    pub logical_volume_manager: Option<bool>,
    /// SnapshotRebuild is enabled.
    pub snapshot_rebuild: Option<bool>,
    /// Mayastor nvmf target supports RDMA.
    pub rdma_capable_io_engine: Option<bool>,
    /// Mayastor supports diskpool encryption.
    pub diskpool_encryption: Option<bool>,
    /// Max version of the nexus label supported by the node.
    #[serde(default)]
    pub nexus_label_version: NexusVersion,
    /// The io-engine gRPC server has TLS enabled and expects TLS connections.
    #[serde(default)]
    pub grpc_tls: Option<bool>,
}

/// Bug fixe in enum format
pub enum NodeBugFix {
    NexusRebuildReplicaAncestry,
}

/// Node bug-fixes as exposed by the node io-engine.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeBugFixes {
    /// Nexus rebuilds both the clusters allocated to the replica and its ancestors clusters.
    pub nexus_rebuild_replica_ancestry: bool,
}
impl NodeBugFixes {
    /// Check if the given fix is present.
    pub fn contains(&self, fix: &NodeBugFix) -> bool {
        match fix {
            NodeBugFix::NexusRebuildReplicaAncestry => self.nexus_rebuild_replica_ancestry,
        }
    }
}

/// Node State information.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeState {
    /// Node Id of the io-engine instance.
    pub id: NodeId,
    /// Grpc endpoint of the io-engine instance.
    pub grpc_endpoint: std::net::SocketAddr,
    /// Deemed status of the node.
    pub status: NodeStatus,
    /// Api versions supported by the dataplane.
    pub api_versions: Option<Vec<ApiVersion>>,
    /// Used to identify dataplane process restarts.
    instance_uuid: Option<uuid::Uuid>,
    /// Used to identify dataplane nvme hostnqn.
    pub node_nqn: Option<HostNqn>,
    /// Features exposed by the io-engine.
    pub features: Option<NodeFeatures>,
    /// BugFixes exposed by the io-engine.
    pub bugfixes: Option<NodeBugFixes>,
    /// Version of the io-engine.
    pub version: Option<String>,
}

impl NodeState {
    /// Return a new `Self`.
    pub fn new(
        id: NodeId,
        grpc_endpoint: std::net::SocketAddr,
        status: NodeStatus,
        api_versions: Option<Vec<ApiVersion>>,
        node_nqn: Option<HostNqn>,
        version: Option<String>,
    ) -> Self {
        Self {
            id,
            grpc_endpoint,
            status,
            api_versions,
            instance_uuid: None,
            node_nqn,
            features: None,
            bugfixes: None,
            version,
        }
    }
    /// Get the node identification.
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    /// Get the node status.
    pub fn status(&self) -> &NodeStatus {
        &self.status
    }
    /// Get the instance uuid.
    pub fn instance_uuid(&self) -> &Option<uuid::Uuid> {
        &self.instance_uuid
    }
    /// Check if the nexus rebuild replica ancestry is fixed.
    pub fn has_rebuild_ancestry_fix(&self) -> bool {
        match &self.bugfixes {
            None => false,
            Some(fixes) => fixes.nexus_rebuild_replica_ancestry,
        }
    }
}
impl From<&Register> for NodeState {
    fn from(src: &Register) -> Self {
        Self::from(src.clone())
    }
}
impl From<Register> for NodeState {
    fn from(src: Register) -> Self {
        Self {
            id: src.id,
            grpc_endpoint: src.grpc_endpoint,
            status: NodeStatus::Online,
            api_versions: src.api_versions,
            instance_uuid: src.instance_uuid,
            node_nqn: src.node_nqn,
            features: src.features,
            bugfixes: src.bugfixes,
            version: src.version,
        }
    }
}

/// Destroy Node Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyNode {
    /// Id of the node to delete.
    pub id: NodeId,
    /// Purge node and all its resources without contacting io-engine.
    #[serde(default)]
    pub purge: bool,
    /// Accept deletion when node has resources (pools, replicas, nexuses).
    #[serde(default)]
    pub accept: bool,
    /// Accept volume loss (last healthy replica for volumes).
    #[serde(default)]
    pub accept_volume_loss: bool,
    /// Accept snapshot loss (last replica snapshot for snapshots).
    #[serde(default)]
    pub accept_snapshot_loss: bool,
}

impl DestroyNode {
    /// Create a new DestroyNode request.
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            purge: false,
            accept: false,
            accept_volume_loss: false,
            accept_snapshot_loss: false,
        }
    }

    /// Create a purge request for the given node.
    pub fn purge(id: NodeId) -> Self {
        Self {
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

/// Result of a node purge operation.
///
/// Always returned by node delete (purge) operations. The `volume_loss` and `snapshot_loss`
/// fields are always present — empty lists indicate no loss occurred.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct NodeDeleteResult {
    /// The deleted node ID.
    pub node_id: NodeId,
    /// Information about volumes that lost healthy replicas.
    pub volume_loss: VolumeLossInfo,
    /// Information about snapshots that lost replica snapshots.
    pub snapshot_loss: SnapshotLossInfo,
}

impl NodeDeleteResult {
    /// Create a new result for a node purge with no data or snapshot loss.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
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

    /// Merge volume and snapshot loss from a pool delete result.
    pub fn merge_pool_result(&mut self, pool_result: &super::PoolDeleteResult) {
        self.volume_loss
            .volumes
            .extend(pool_result.volume_loss.volumes.iter().cloned());
        self.snapshot_loss
            .snapshots
            .extend(pool_result.snapshot_loss.snapshots.iter().cloned());
    }
}

impl From<NodeDeleteResult> for models::NodeDeleteResult {
    fn from(src: NodeDeleteResult) -> Self {
        models::NodeDeleteResult {
            node_id: src.node_id.to_string(),
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

rpc_impl_string_id!(NodeId, "ID of a node");

impl From<NodeState> for models::NodeState {
    fn from(src: NodeState) -> Self {
        Self::new_all(
            src.grpc_endpoint.to_string(),
            src.id,
            src.status,
            src.node_nqn.into_opt(),
            src.version,
        )
    }
}
impl From<&NodeState> for models::NodeState {
    fn from(src: &NodeState) -> Self {
        Self::from(src.clone())
    }
}

impl From<NodeStatus> for models::NodeStatus {
    fn from(src: NodeStatus) -> Self {
        match src {
            NodeStatus::Unknown => Self::Unknown,
            NodeStatus::Online => Self::Online,
            NodeStatus::Offline => Self::Offline,
        }
    }
}

/// api versions known by control plane
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum ApiVersion {
    #[default]
    V0,
    V1,
}

impl FromStr for ApiVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v0" => Ok(Self::V0),
            "v1" => Ok(Self::V1),
            _ => Err(format!("The api version: {s} is not supported")),
        }
    }
}
