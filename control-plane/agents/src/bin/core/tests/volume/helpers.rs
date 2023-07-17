use super::RECONCILE_TIMEOUT_SECS;
use deployer_cluster::Cluster;
use grpc::operations::{
    node::traits::NodeOperations, registry::traits::RegistryOperations,
    volume::traits::VolumeOperations,
};
use std::time::Duration;
use stor_port::types::v0::{
    openapi::models,
    transport::{
        Child, ChildUri, Filter, GetSpecs, GetVolumes, NodeId, NodeStatus, Volume, VolumeId,
    },
};
use uuid::Uuid;

/// Wait for the published volume to have the specified replicas and to not having the specified
/// child. Wait up to the specified timeout.
pub(crate) async fn wait_till_volume_nexus(
    volume: &VolumeId,
    replicas: usize,
    no_child: &str,
    volume_client: &dyn VolumeOperations,
    registry_client: &dyn RegistryOperations,
) -> Vec<Child> {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let volume = volume_client
            .get(GetVolumes::new(volume).filter, false, None, None)
            .await
            .unwrap();
        let volume_state = volume.entries.clone().first().unwrap().state();
        let nexus = volume_state.target.clone().unwrap();
        let specs = registry_client.get_specs(&GetSpecs {}, None).await.unwrap();
        let nexus_spec = specs.nexuses.first().unwrap().clone();

        if !nexus.contains_child(&ChildUri::from(no_child))
            && nexus.children.len() == replicas
            && nexus_spec.children.len() == replicas
        {
            return nexus.children;
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified state (replicas: '{replicas}', no_child: '{no_child}')! Current: {volume_state:#?}"
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Get the children of the specified volume (assumes non ANA)
pub(crate) async fn volume_children(
    volume: &VolumeId,
    client: &dyn VolumeOperations,
) -> Vec<Child> {
    let volume = client
        .get(GetVolumes::new(volume).filter, false, None, None)
        .await
        .unwrap();
    let volume_state = volume.entries.first().unwrap().state();
    volume_state.target.unwrap().children
}

/// Wait for the unpublished volume to have the specified replica count
pub(crate) async fn wait_till_volume(
    volume: &VolumeId,
    replicas: usize,
    volume_client: &dyn VolumeOperations,
    registry_client: &dyn RegistryOperations,
) {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let specs = registry_client.get_specs(&GetSpecs {}, None).await.unwrap();
        let replica_specs = specs
            .replicas
            .iter()
            .filter(|r| r.owners.owned_by(volume))
            .collect::<Vec<_>>();

        if replica_specs.len() == replicas
            && existing_replicas(volume, volume_client).await == replicas
        {
            return;
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified replica ('{}'), current: '{}'",
                replicas, replica_specs.len()
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Return the number of replicas that exist and have a state.
async fn existing_replicas(volume_id: &VolumeId, client: &dyn VolumeOperations) -> usize {
    let volumes = client
        .get(GetVolumes::new(volume_id).filter, false, None, None)
        .await
        .unwrap();

    // Get volumes with the given uuid.
    // There should only be one.
    let filtered_volumes: Vec<Volume> = volumes
        .entries
        .into_iter()
        .filter(|v| v.uuid() == volume_id)
        .collect();
    assert_eq!(filtered_volumes.len(), 1);

    let volume = filtered_volumes[0].clone();

    // A replica is deemed to exist if its topology indicates a valid node and pool.
    volume
        .state()
        .replica_topology
        .into_iter()
        .filter(|(_id, topology)| topology.node().is_some() && topology.pool().is_some())
        .count()
}

/// Wait for the published volume target to have the specified number of children.
#[allow(dead_code)]
pub(crate) async fn wait_till_volume_children(
    volume: &VolumeId,
    children: usize,
    volume_client: &dyn VolumeOperations,
) -> Vec<Child> {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let volume = volume_client
            .get(GetVolumes::new(volume).filter, false, None, None)
            .await
            .unwrap();
        let volume_state = volume.entries.clone().first().unwrap().state();
        let nexus = volume_state.target.clone().unwrap();

        if nexus.children.len() == children {
            return nexus.children;
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified state (children: '{children}')! Current: {volume_state:#?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Checks if node is online, returns true if yes.
pub(crate) async fn wait_node_online(
    node_client: &impl NodeOperations,
    node_id: NodeId,
) -> Result<(), ()> {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let node = node_client
            .get(Filter::Node(node_id.clone()), true, None)
            .await
            .expect("Cant get node object");
        if let Some(node) = node.0.get(0) {
            if node.state().map(|n| &n.status) == Some(&NodeStatus::Online) {
                return Ok(());
            }
        }
        if std::time::Instant::now() > (start + timeout) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(())
}

/// Wait for a volume to reach the provided status with timeout.
pub(crate) async fn wait_till_volume_status(
    cluster: &Cluster,
    volume: &Uuid,
    status: models::VolumeStatus,
    timeout: Duration,
) -> Result<(), String> {
    let start = std::time::Instant::now();
    loop {
        let volume = cluster.rest_v00().volumes_api().get_volume(volume).await;
        if volume.as_ref().unwrap().state.status == status {
            return Ok(());
        }

        if std::time::Instant::now() > (start + timeout) {
            let error = format!("Timeout waiting for the volume to reach the specified status ('{status:?}'), current: '{volume:?}'");
            return Err(error);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
