use crate::{
    resources::{
        error::Error,
        utils::{optional_cell, print_table, CreateRow, GetHeaderRow, OutputFormat},
        NodeId, PoolId, VolumeId,
    },
    rest_wrapper::RestClient,
};
use openapi::models::{self, PoolCordonDrain};
use prettytable::Row;
use serde::Serialize;

/// Cordon state for purge eligibility.
#[derive(Serialize, Debug, Clone)]
pub enum CordonState {
    /// Pool has no cordon in place.
    NotCordoned,
    /// Pool is cordoned but does not block both replicas and snapshots.
    Insufficient,
    /// Pool is cordoned and blocks both replica and snapshot scheduling.
    Ready,
}

impl Default for CordonState {
    fn default() -> Self {
        Self::NotCordoned
    }
}

impl std::fmt::Display for CordonState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCordoned => write!(f, "Not cordoned"),
            Self::Insufficient => write!(f, "Insufficient"),
            Self::Ready => write!(f, "Ready"),
        }
    }
}

/// Impact analysis for a pool purge operation.
///
/// Shown when using `--show-impact` to preview what would happen before
/// actually purging the pool.
#[derive(Serialize, Default, Debug, Clone)]
pub struct PoolPurgeImpact {
    /// The pool being analyzed.
    pub pool_id: PoolId,
    /// Current runtime status of the pool (e.g. Unknown, Online, Faulted).
    /// Purge is only allowed when status is Unknown.
    pub status: models::PoolStatus,
    /// Whether the pool is cordoned and blocks both replica and snapshot
    /// scheduling, as required for purge.
    pub cordon: CordonState,
    /// Number of replicas residing on this pool across all volumes.
    pub replica_count: usize,
    /// Volumes that have at least one replica on this pool and would be
    /// affected by the purge.
    pub affected_volumes: Vec<VolumeId>,
    /// Whether the pool meets all preconditions for purge:
    /// status is Unknown and cordon blocks both replicas and snapshots.
    pub ready_for_purge: bool,
}

impl GetHeaderRow for PoolPurgeImpact {
    fn get_header_row(&self) -> Row {
        row!["POOL", "STATUS", "CORDON", "REPLICAS", "VOLUMES", "READY",]
    }
}

impl CreateRow for PoolPurgeImpact {
    fn row(&self) -> Row {
        row![
            self.pool_id,
            self.status,
            self.cordon,
            self.replica_count,
            optional_cell((!self.affected_volumes.is_empty()).then(|| {
                self.affected_volumes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            })),
            self.ready_for_purge.to_string(),
        ]
    }
}

/// Per-pool impact detail within a node purge.
#[derive(Serialize, Debug, Clone)]
pub struct PoolImpact {
    /// The pool being analyzed.
    pub pool_id: String,
    /// Current runtime status of the pool.
    pub status: models::PoolStatus,
    /// Number of replicas residing on this pool.
    pub replica_count: usize,
    /// Volumes affected by purging this pool.
    pub affected_volumes: Vec<VolumeId>,
}

impl GetHeaderRow for PoolImpact {
    fn get_header_row(&self) -> Row {
        row!["POOL", "STATUS", "REPLICAS", "VOLUMES"]
    }
}

impl CreateRow for PoolImpact {
    fn row(&self) -> Row {
        row![
            self.pool_id,
            self.status,
            self.replica_count,
            optional_cell((!self.affected_volumes.is_empty()).then(|| {
                self.affected_volumes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            })),
        ]
    }
}

/// Impact analysis for a node purge operation.
///
/// Shown when using `--show-impact` to preview what would happen before
/// actually purging the node and all its pools.
#[derive(Serialize, Debug, Clone)]
pub struct NodePurgeImpact {
    /// The node being analyzed.
    pub node_id: NodeId,
    /// Deemed status of the node (Online, Offline, Unknown).
    pub status: models::NodeStatus,
    /// Whether the node is cordoned.
    pub cordoned: bool,
    /// Per-pool impact breakdown.
    pub pools: Vec<PoolImpact>,
    /// Total number of replicas across all pools on this node.
    pub total_replicas: usize,
    /// Total unique volumes affected across all pools.
    pub total_affected_volumes: usize,
    /// Whether the node meets all preconditions for purge:
    /// node is offline and cordoned.
    pub ready_for_purge: bool,
}

impl GetHeaderRow for NodePurgeImpact {
    fn get_header_row(&self) -> Row {
        row!["NODE", "STATUS", "CORDONED", "POOLS", "REPLICAS", "VOLUMES", "READY",]
    }
}

impl CreateRow for NodePurgeImpact {
    fn row(&self) -> Row {
        row![
            self.node_id,
            self.status,
            self.cordoned,
            self.pools.len(),
            self.total_replicas,
            self.total_affected_volumes,
            self.ready_for_purge,
        ]
    }
}

/// Compute replica count and affected volumes for a single pool by scanning
/// volume replica topologies.
pub fn compute_pool_replica_impact(
    pool_id: &str,
    volumes: &[models::Volume],
) -> (usize, Vec<VolumeId>) {
    let mut replica_count = 0usize;
    let mut affected_volumes = Vec::new();
    for volume in volumes {
        for topo in volume.state.replica_topology.values() {
            if topo.pool.as_deref() == Some(pool_id) {
                replica_count += 1;
                if !affected_volumes.contains(&volume.spec.uuid) {
                    affected_volumes.push(volume.spec.uuid);
                }
            }
        }
    }
    (replica_count, affected_volumes)
}

/// Determine the cordon state of a pool for purge eligibility.
pub fn pool_cordon_state(pool: &models::Pool) -> CordonState {
    match pool.spec.as_ref().and_then(|s| s.cordon_drain.as_ref()) {
        None => CordonState::NotCordoned,
        Some(PoolCordonDrain::cordoned(state)) => {
            if state.replicas && state.snapshots {
                CordonState::Ready
            } else {
                CordonState::Insufficient
            }
        }
    }
}

/// Fetch all volumes from the REST API with pagination.
async fn fetch_all_volumes() -> Vec<models::Volume> {
    let max_entries = 500;
    let mut starting_token = Some(0);
    let mut volumes = Vec::with_capacity(max_entries as usize);

    while starting_token.is_some() {
        match RestClient::client()
            .volumes_api()
            .get_volumes(max_entries, None, starting_token)
            .await
        {
            Ok(vols) => {
                let v = vols.into_body();
                volumes.extend(v.entries);
                starting_token = v.next_token;
            }
            Err(_) => break,
        }
    }
    volumes
}

/// Show what would happen if this pool is purged.
pub async fn show_pool_impact(pool_id: &PoolId, output: &OutputFormat) -> Result<(), Error> {
    let pool = RestClient::client()
        .pools_api()
        .get_pool(pool_id)
        .await
        .map_err(|e| Error::GetPoolError {
            id: pool_id.to_string(),
            source: e,
        })?
        .into_body();

    let status = pool
        .state
        .as_ref()
        .map(|s| s.status)
        .unwrap_or(models::PoolStatus::Unknown);

    let cordon = pool_cordon_state(&pool);
    let volumes = fetch_all_volumes().await;
    let (replica_count, affected_volumes) = compute_pool_replica_impact(pool_id, &volumes);
    let ready_for_purge =
        status == models::PoolStatus::Unknown && matches!(cordon, CordonState::Ready);

    let impact = PoolPurgeImpact {
        pool_id: pool_id.clone(),
        status,
        cordon,
        replica_count,
        affected_volumes,
        ready_for_purge,
    };

    print_table(output, impact);
    Ok(())
}

/// Show what would happen if this node is purged.
pub async fn show_node_impact(node_id: &NodeId, output: &OutputFormat) -> Result<(), Error> {
    let node = RestClient::client()
        .nodes_api()
        .get_node(node_id)
        .await
        .map_err(|e| Error::GetNodeError {
            id: node_id.to_string(),
            source: e,
        })?
        .into_body();

    let status = node.status.unwrap_or(models::NodeStatus::Unknown);
    let cordoned = node
        .spec
        .as_ref()
        .and_then(|s| s.cordondrainstate.as_ref())
        .is_some();

    let node_pools = RestClient::client()
        .pools_api()
        .get_node_pools(node_id)
        .await
        .map(|r| r.into_body())
        .unwrap_or_default();

    let volumes = fetch_all_volumes().await;

    let mut pool_impacts = Vec::new();
    let mut all_affected_volumes: std::collections::HashSet<VolumeId> =
        std::collections::HashSet::new();
    let mut total_replicas = 0usize;

    for pool in &node_pools {
        let pool_id = pool.spec.as_ref().map(|s| s.id.clone()).unwrap_or_default();
        let pool_status = pool
            .state
            .as_ref()
            .map(|s| s.status)
            .unwrap_or(models::PoolStatus::Unknown);

        let (replica_count, affected_volumes) = compute_pool_replica_impact(&pool_id, &volumes);
        all_affected_volumes.extend(&affected_volumes);
        total_replicas += replica_count;

        pool_impacts.push(PoolImpact {
            pool_id,
            status: pool_status,
            replica_count,
            affected_volumes,
        });
    }

    let ready_for_purge = status == models::NodeStatus::Offline && cordoned;

    let impact = NodePurgeImpact {
        node_id: node_id.clone(),
        status,
        cordoned,
        pools: pool_impacts.clone(),
        total_replicas,
        total_affected_volumes: all_affected_volumes.len(),
        ready_for_purge,
    };

    match output {
        OutputFormat::Yaml | OutputFormat::Json => {
            // pools are embedded in NodePurgeImpact, single serialised object.
            print_table(output, impact);
        }
        OutputFormat::None => {
            print_table(output, impact);
            if !pool_impacts.is_empty() {
                println!();
                print_table(output, pool_impacts);
            }
        }
    }

    Ok(())
}

impl GetHeaderRow for models::VolumeLossDetail {
    fn get_header_row(&self) -> Row {
        row![
            "VOLUME",
            "REPLICAS-BEFORE",
            "HEALTHY-BEFORE",
            "LOST-ON-POOL",
            "HEALTHY-AFTER"
        ]
    }
}

impl CreateRow for models::VolumeLossDetail {
    fn row(&self) -> Row {
        row![
            self.volume_id,
            self.replicas_before,
            self.healthy_before,
            self.lost_on_pool,
            self.healthy_after,
        ]
    }
}

impl GetHeaderRow for models::SnapshotLossDetail {
    fn get_header_row(&self) -> Row {
        row![
            "SNAPSHOT",
            "REPLICA-SNAPSHOTS-BEFORE",
            "HEALTHY-BEFORE",
            "LOST-ON-POOL",
            "HEALTHY-AFTER"
        ]
    }
}

impl CreateRow for models::SnapshotLossDetail {
    fn row(&self) -> Row {
        row![
            self.snapshot_id,
            self.replica_snapshots_before,
            self.healthy_before,
            self.lost_on_pool,
            self.healthy_after,
        ]
    }
}

/// Print the volume/snapshot loss details carried by a purge-rejected API error, if any.
///
/// The agent-core computes the loss impact as part of the purge pre-flight check, and
/// shares it back through the error response's `customInfo` when the purge is rejected
/// because `accept_volume_loss`/`accept_snapshot_loss` was required. This lets the plugin
/// show exactly what would be lost, without the user having to separately run `--show-impact`.
pub fn print_purge_loss_from_error(
    source: &openapi::tower::client::Error<models::RestJsonError>,
    output: &OutputFormat,
) {
    let Some(custom) = source.error_body().and_then(|body| body.custom_info.clone()) else {
        return;
    };

    if let Some(volume_loss) = custom.pool.volume_loss {
        if !volume_loss.volumes.is_empty() {
            println!("Volumes that would lose their last healthy replica:");
            print_table(output, volume_loss.volumes);
            println!();
        }
    }
    if let Some(snapshot_loss) = custom.pool.snapshot_loss {
        if !snapshot_loss.snapshots.is_empty() {
            println!("Snapshots that would lose their last replica snapshot:");
            print_table(output, snapshot_loss.snapshots);
            println!();
        }
    }
}
