use super::RECONCILE_TIMEOUT_SECS;
use common_lib::types::v0::transport::{Child, ChildUri, GetSpecs, GetVolumes, Volume, VolumeId};
use grpc::operations::{registry::traits::RegistryOperations, volume::traits::VolumeOperations};
use std::time::Duration;

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
            .get(GetVolumes::new(volume).filter, None, None)
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
                "Timeout waiting for the volume to reach the specified state (replicas: '{}', no_child: '{}')! Current: {:#?}",
                replicas, no_child, volume_state
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
        .get(GetVolumes::new(volume).filter, None, None)
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
        .get(GetVolumes::new(volume_id).filter, None, None)
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
pub(crate) async fn wait_till_volume_children(
    volume: &VolumeId,
    children: usize,
    volume_client: &dyn VolumeOperations,
) -> Vec<Child> {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let volume = volume_client
            .get(GetVolumes::new(volume).filter, None, None)
            .await
            .unwrap();
        let volume_state = volume.entries.clone().first().unwrap().state();
        let nexus = volume_state.target.clone().unwrap();

        if nexus.children.len() == children {
            return nexus.children;
        }
        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified state (children: '{}')! Current: {:#?}",
                children, volume_state
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
