use crate::{
    resources::{
        error::Error,
        utils::{optional_cell, print_table, CreateRow, GetHeaderRow, OutputFormat},
        NodeId, PoolId, SnapshotId, VolumeId,
    },
    rest_wrapper::RestClient,
};
use openapi::models::{self, PoolCordonDrain};
use prettytable::Row;
use serde::Serialize;
use std::collections::HashSet;

/// Cordon state for purge eligibility.
#[derive(Serialize, Default, Debug, Clone)]
pub enum CordonState {
    /// Pool has no cordon in place.
    #[default]
    NotCordoned,
    /// Pool is cordoned but does not block both replicas and snapshots.
    Insufficient,
    /// Pool is cordoned and blocks both replica and snapshot scheduling.
    Ready,
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
            optional_cell(format_volume_list(&self.affected_volumes)),
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
            optional_cell(format_volume_list(&self.affected_volumes)),
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

/// Format a list of affected volume IDs as a comma-separated string cell,
/// or `None` if the list is empty.
fn format_volume_list(volumes: &[VolumeId]) -> Option<String> {
    (!volumes.is_empty()).then(|| {
        volumes
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    })
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

/// Details about a volume that would lose its last healthy replica as a
/// result of a pool/node purge.
#[derive(Serialize, Debug, Clone)]
pub struct VolumeLossDetail {
    /// The affected volume's unique identifier.
    pub volume_id: VolumeId,
    /// Total number of replicas this volume had across all pools before the purge.
    pub replicas_before: u32,
    /// Number of those replicas that were in a healthy state before the purge.
    pub healthy_before: u32,
    /// Number of this volume's replicas that reside on the pool(s) being purged.
    pub lost_on_pool: u32,
    /// Number of healthy replicas remaining after the purge. Zero means data loss.
    pub healthy_after: u32,
}

impl GetHeaderRow for VolumeLossDetail {
    fn get_header_row(&self) -> Row {
        row![
            "VOLUME",
            "REPLICAS-BEFORE",
            "HEALTHY-BEFORE",
            "LOST-ON-POOL",
            "HEALTHY-AFTER",
        ]
    }
}

impl CreateRow for VolumeLossDetail {
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

/// Details about a snapshot that would lose its last replica snapshot as a
/// result of a pool/node purge.
#[derive(Serialize, Debug, Clone)]
pub struct SnapshotLossDetail {
    /// The affected volume snapshot's unique identifier.
    pub snapshot_id: SnapshotId,
    /// Total number of replica snapshots this snapshot had across all pools before the purge.
    pub replica_snapshots_before: u32,
    /// Number of this snapshot's replica snapshots that reside on the pool(s) being purged.
    pub lost_on_pool: u32,
}

impl GetHeaderRow for SnapshotLossDetail {
    fn get_header_row(&self) -> Row {
        row!["SNAPSHOT", "REPLICA-SNAPSHOTS-BEFORE", "LOST-ON-POOL"]
    }
}

impl CreateRow for SnapshotLossDetail {
    fn row(&self) -> Row {
        row![
            self.snapshot_id,
            self.replica_snapshots_before,
            self.lost_on_pool,
        ]
    }
}

/// Compute the volumes that would lose their last healthy replica if the
/// given pool(s) were purged, by scanning volume replica topologies.
///
/// Mirrors the semantics of the core agent's `analyze_volume_loss`.
pub fn compute_volume_loss(
    pool_ids: &HashSet<String>,
    volumes: &[models::Volume],
) -> Vec<VolumeLossDetail> {
    let mut volumes_with_loss = Vec::new();

    for volume in volumes {
        let topology = &volume.state.replica_topology;

        let replicas_before = topology.len() as u32;
        let is_healthy = |rt: &models::ReplicaTopology| {
            rt.state == models::ReplicaState::Online && rt.healthy == Some(true)
        };
        let on_pool =
            |rt: &models::ReplicaTopology| rt.pool.as_ref().is_some_and(|p| pool_ids.contains(p));

        let mut healthy_before = 0u32;
        let mut lost_on_pool = 0u32;
        let mut healthy_lost = 0u32;
        for rt in topology.values() {
            let healthy = is_healthy(rt);
            healthy_before += healthy as u32;
            if on_pool(rt) {
                lost_on_pool += 1;
                healthy_lost += healthy as u32;
            }
        }

        let healthy_after = healthy_before.saturating_sub(healthy_lost);

        if healthy_after == 0 && lost_on_pool > 0 {
            volumes_with_loss.push(VolumeLossDetail {
                volume_id: volume.spec.uuid,
                replicas_before,
                healthy_before,
                lost_on_pool,
                healthy_after,
            });
        }
    }

    volumes_with_loss
}

/// Compute the snapshots that would lose their last replica snapshot if the
/// given pool(s) were purged, by scanning volume snapshot replica states.
///
/// Mirrors the semantics of the core agent's `analyze_snapshot_loss`.
pub fn compute_snapshot_loss(
    pool_ids: &HashSet<String>,
    snapshots: &[models::VolumeSnapshot],
) -> Vec<SnapshotLossDetail> {
    let mut snapshots_with_loss = Vec::new();

    for vol_snapshot in snapshots {
        let state = &vol_snapshot.state;
        let total_replica_snaps = state.replica_snapshots.len() as u32;

        let on_these_pools = state
            .replica_snapshots
            .iter()
            .filter(|rs| {
                let pool_id = match rs {
                    models::ReplicaSnapshotState::online(s) => &s.pool_id,
                    models::ReplicaSnapshotState::offline(s) => &s.pool_id,
                };
                pool_ids.contains(pool_id)
            })
            .count() as u32;

        if on_these_pools > 0 {
            let surviving = total_replica_snaps - on_these_pools;
            if surviving == 0 {
                snapshots_with_loss.push(SnapshotLossDetail {
                    snapshot_id: state.uuid,
                    replica_snapshots_before: total_replica_snaps,
                    lost_on_pool: on_these_pools,
                });
            }
        }
    }

    snapshots_with_loss
}

/// Fetch all volume snapshots from the REST API with pagination.
async fn fetch_all_snapshots() -> Vec<models::VolumeSnapshot> {
    let max_entries = 500;
    let mut starting_token = Some(0);
    let mut snapshots = Vec::with_capacity(max_entries as usize);

    while starting_token.is_some() {
        match RestClient::client()
            .snapshots_api()
            .get_volumes_snapshots(max_entries, None, None, starting_token)
            .await
        {
            Ok(snaps) => {
                let s = snaps.into_body();
                snapshots.extend(s.entries);
                starting_token = s.next_token;
            }
            Err(_) => break,
        }
    }
    snapshots
}

/// Compute the volume and snapshot loss for the given set of pool IDs by
/// fetching current volumes and snapshots and analyzing their loss impact.
async fn compute_purge_loss(
    pool_ids: &HashSet<String>,
) -> (Vec<VolumeLossDetail>, Vec<SnapshotLossDetail>) {
    let volumes = fetch_all_volumes().await;
    let snapshots = fetch_all_snapshots().await;

    (
        compute_volume_loss(pool_ids, &volumes),
        compute_snapshot_loss(pool_ids, &snapshots),
    )
}

/// Compute the volume and snapshot loss that would result from purging a single pool.
pub async fn compute_pool_purge_loss(
    pool_id: &PoolId,
) -> (Vec<VolumeLossDetail>, Vec<SnapshotLossDetail>) {
    let pool_ids = HashSet::from([pool_id.to_string()]);
    compute_purge_loss(&pool_ids).await
}

/// Compute the volume and snapshot loss that would result from purging a node
/// and all of its pools.
pub async fn compute_node_purge_loss(
    node_id: &NodeId,
) -> (Vec<VolumeLossDetail>, Vec<SnapshotLossDetail>) {
    let node_pools = fetch_node_pools(node_id).await;

    let pool_ids: HashSet<String> = node_pools
        .iter()
        .filter_map(|pool| pool.spec.as_ref().map(|s| s.id.clone()))
        .collect();

    compute_purge_loss(&pool_ids).await
}

/// Print the volume and/or snapshot loss impact tables, if non-empty.
pub fn print_purge_loss(
    volume_loss: &[VolumeLossDetail],
    snapshot_loss: &[SnapshotLossDetail],
    output: &OutputFormat,
) {
    match output {
        OutputFormat::Yaml | OutputFormat::Json => {
            if !volume_loss.is_empty() {
                print_table(output, volume_loss.to_vec());
            }
            if !snapshot_loss.is_empty() {
                print_table(output, snapshot_loss.to_vec());
            }
        }
        OutputFormat::None => {
            if !volume_loss.is_empty() {
                println!("Volumes that would lose their last healthy replica:");
                print_table(output, volume_loss.to_vec());
            }
            if !snapshot_loss.is_empty() {
                if !volume_loss.is_empty() {
                    println!();
                }
                println!("Snapshots that would lose their last replica snapshot:");
                print_table(output, snapshot_loss.to_vec());
            }
        }
    }
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

/// Fetch all pools belonging to a node from the REST API.
async fn fetch_node_pools(node_id: &NodeId) -> Vec<models::Pool> {
    RestClient::client()
        .pools_api()
        .get_node_pools(node_id)
        .await
        .map(|r| r.into_body())
        .unwrap_or_default()
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

    let node_pools = fetch_node_pools(node_id).await;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn volume_with_topology(
        uuid: openapi::apis::Uuid,
        replicas: Vec<(&str, models::ReplicaState, Option<bool>)>,
    ) -> models::Volume {
        let replica_topology = replicas
            .into_iter()
            .enumerate()
            .map(|(idx, (pool, state, healthy))| {
                (
                    idx.to_string(),
                    models::ReplicaTopology {
                        pool: Some(pool.to_string()),
                        state,
                        healthy,
                        ..Default::default()
                    },
                )
            })
            .collect();

        models::Volume {
            spec: models::VolumeSpec {
                uuid,
                ..Default::default()
            },
            state: models::VolumeState {
                uuid,
                replica_topology,
                ..Default::default()
            },
        }
    }

    #[test]
    fn volume_loss_when_last_healthy_replica_on_pool() {
        let volume_id = openapi::apis::Uuid::new_v4();
        let volumes = vec![volume_with_topology(
            volume_id,
            vec![
                ("pool-1", models::ReplicaState::Online, Some(true)),
                ("pool-2", models::ReplicaState::Faulted, Some(false)),
            ],
        )];

        let mut pool_ids = HashSet::new();
        pool_ids.insert("pool-1".to_string());

        let loss = compute_volume_loss(&pool_ids, &volumes);
        assert_eq!(loss.len(), 1);
        assert_eq!(loss[0].volume_id, volume_id);
        assert_eq!(loss[0].replicas_before, 2);
        assert_eq!(loss[0].healthy_before, 1);
        assert_eq!(loss[0].lost_on_pool, 1);
        assert_eq!(loss[0].healthy_after, 0);
    }

    #[test]
    fn no_volume_loss_when_other_healthy_replica_survives() {
        let volume_id = openapi::apis::Uuid::new_v4();
        let volumes = vec![volume_with_topology(
            volume_id,
            vec![
                ("pool-1", models::ReplicaState::Online, Some(true)),
                ("pool-2", models::ReplicaState::Online, Some(true)),
            ],
        )];

        let mut pool_ids = HashSet::new();
        pool_ids.insert("pool-1".to_string());

        let loss = compute_volume_loss(&pool_ids, &volumes);
        assert!(loss.is_empty());
    }

    fn snapshot_with_replica_pools(
        uuid: openapi::apis::Uuid,
        pools: Vec<&str>,
    ) -> models::VolumeSnapshot {
        let replica_snapshots = pools
            .into_iter()
            .map(|pool| {
                models::ReplicaSnapshotState::offline(models::OfflineReplicaSnapshotState {
                    pool_id: pool.to_string(),
                    ..Default::default()
                })
            })
            .collect();

        models::VolumeSnapshot {
            state: models::VolumeSnapshotState {
                uuid,
                replica_snapshots,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_loss_when_all_replica_snapshots_on_pool() {
        let snap_id = openapi::apis::Uuid::new_v4();
        let snapshots = vec![snapshot_with_replica_pools(snap_id, vec!["pool-1"])];

        let mut pool_ids = HashSet::new();
        pool_ids.insert("pool-1".to_string());

        let loss = compute_snapshot_loss(&pool_ids, &snapshots);
        assert_eq!(loss.len(), 1);
        assert_eq!(loss[0].snapshot_id, snap_id);
        assert_eq!(loss[0].replica_snapshots_before, 1);
        assert_eq!(loss[0].lost_on_pool, 1);
    }

    #[test]
    fn no_snapshot_loss_when_replica_snapshot_survives_on_other_pool() {
        let snap_id = openapi::apis::Uuid::new_v4();
        let snapshots = vec![snapshot_with_replica_pools(
            snap_id,
            vec!["pool-1", "pool-2"],
        )];

        let mut pool_ids = HashSet::new();
        pool_ids.insert("pool-1".to_string());

        let loss = compute_snapshot_loss(&pool_ids, &snapshots);
        assert!(loss.is_empty());
    }
}
