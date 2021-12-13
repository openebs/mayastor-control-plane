#![cfg(test)]

use common_lib::{
    mbus_api,
    mbus_api::{message_bus::v0::Replicas, Message, ReplyError, ReplyErrorKind, ResourceKind},
    store::etcd::Etcd,
    types::v0::{
        message_bus::{
            Child, ChildState, CreateReplica, CreateVolume, DestroyVolume, Filter, GetNexuses,
            GetNodes, GetReplicas, GetVolumes, Nexus, NodeId, PublishVolume, SetVolumeReplica,
            ShareVolume, Topology, UnpublishVolume, UnshareVolume, Volume, VolumeShareProtocol,
            VolumeState, VolumeStatus,
        },
        openapi::apis::{StatusCode, Uuid},
        store::{
            definitions::Store,
            nexus_persistence::{NexusInfo, NexusInfoKey},
        },
    },
};

use composer::rpc::mayastor::FaultNexusChildRequest;
use testlib::{Cluster, ClusterBuilder};

use common_lib::{
    mbus_api::TimeoutOptions,
    types::v0::{
        message_bus::{
            ChannelVs, ChildUri, CreateNexus, DestroyReplica, GetSpecs, Liveness, NexusId,
            ReplicaId, ReplicaOwners, VolumeId,
        },
        openapi::{models, models::NodeStatus, tower::client::Error},
        store::{definitions::StorableObject, volume::VolumeSpec},
    },
};
use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    time::Duration,
};

#[tokio::test]
async fn volume() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(3)
        .with_pools(1)
        .with_cache_period("1s")
        // don't let the reconcile interfere with the tests
        .with_reconcile_period(Duration::from_secs(1000), Duration::from_secs(1000))
        .build()
        .await
        .unwrap();

    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    test_volume(&cluster).await;
}

#[tracing::instrument(skip(cluster))]
async fn test_volume(cluster: &Cluster) {
    smoke_test().await;
    publishing_test(cluster).await;
    replica_count_test().await;
    nexus_persistence_test(cluster).await;
}

const RECONCILE_TIMEOUT_SECS: u64 = 7;

#[tokio::test]
async fn hotspare() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(3)
        .with_pools(2)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    hotspare_faulty_children(&cluster).await;
    hotspare_unknown_children(&cluster).await;
    hotspare_missing_children(&cluster).await;
    hotspare_replica_count(&cluster).await;
    hotspare_replica_count_spread(&cluster).await;
    hotspare_nexus_replica_count(&cluster).await;
}

const POOL_SIZE_BYTES: u64 = 128 * 1024 * 1024;
#[tokio::test]
async fn volume_nexus_reconcile() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(2)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_secs(1), Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    missing_nexus_reconcile(&cluster).await;
}

#[tokio::test]
async fn garbage_collection() {
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_agents(vec!["core"])
        .with_mayastors(3)
        .with_tmpfs_pool(POOL_SIZE_BYTES)
        .with_cache_period("1s")
        .with_reconcile_period(Duration::from_millis(500), Duration::from_millis(500))
        .build()
        .await
        .unwrap();
    let nodes = GetNodes::default().request().await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    unused_nexus_reconcile(&cluster).await;
    unused_reconcile(&cluster).await;
}

async fn unused_nexus_reconcile(cluster: &Cluster) {
    let rest_api = cluster.rest_v00();
    let volumes_api = rest_api.volumes_api();

    let volume = volumes_api
        .put_volume(
            &"1e3cf927-80c2-47a8-adf0-95c481bdd7b7".parse().unwrap(),
            models::CreateVolumeBody::new(models::VolumePolicy::default(), 2, 5242880u64),
        )
        .await
        .unwrap();

    let volume = volumes_api
        .put_volume_target(
            &volume.spec.uuid,
            cluster.node(0).as_str(),
            models::VolumeShareProtocol::Nvmf,
        )
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state;

    let volume_id = volume_state.uuid;
    let volume = volumes_api.get_volume(&volume_id).await.unwrap();
    assert_eq!(volume.state.status, models::VolumeStatus::Online);

    let mut create_nexus = CreateNexus {
        node: cluster.node(0),
        uuid: NexusId::new(),
        size: volume.spec.size,
        children: vec![
            "malloc:///test?size_mb=10&uuid=b9558b8c-cb22-47f3-b33b-583db25b5a8c".into(),
        ],
        managed: true,
        owner: None,
        config: None,
    };
    let nexus = create_nexus.request().await.unwrap();
    let nexus = wait_till_nexus_state(cluster, &nexus.uuid, None).await;
    assert_eq!(nexus, None, "nexus should be gone");

    create_nexus.owner = Some(VolumeId::new());
    let nexus = create_nexus.request().await.unwrap();
    let nexus = wait_till_nexus_state(cluster, &nexus.uuid, None).await;
    assert_eq!(nexus, None, "nexus should be gone");

    volumes_api.del_volume(&volume_id).await.unwrap();
}

async fn unused_reconcile(cluster: &Cluster) {
    let rest_api = cluster.rest_v00();
    let volumes_api = rest_api.volumes_api();

    let volume = volumes_api
        .put_volume(
            &"22054b1f-cf32-46dc-90ff-d6a5c61429c2".parse().unwrap(),
            models::CreateVolumeBody::new(models::VolumePolicy::new(true), 2, 5242880u64),
        )
        .await
        .unwrap();

    let data_replicas_nodes = volume
        .state
        .replica_topology
        .values()
        .map(|r| r.node.clone().unwrap())
        .collect::<Vec<_>>();
    let nodes = rest_api.nodes_api().get_nodes().await.unwrap();
    let unused_node = nodes
        .iter()
        .find(|r| !data_replicas_nodes.contains(&r.id))
        .cloned()
        .unwrap();
    let nexus_node = nodes
        .iter()
        .find(|n| n.id != unused_node.id)
        .cloned()
        .unwrap();
    let replica_nexus = volume
        .state
        .replica_topology
        .into_iter()
        .find_map(|(i, r)| {
            if r.node.as_ref().unwrap() == &nexus_node.id {
                Some(i)
            } else {
                None
            }
        })
        .unwrap();

    let volume = volumes_api
        .put_volume_target(
            &volume.spec.uuid,
            nexus_node.id.as_str(),
            models::VolumeShareProtocol::Nvmf,
        )
        .await
        .unwrap();
    tracing::info!("Volume: {:?}\nUnused Node: {}", volume, unused_node.id);

    // 1. first we kill the node where the nexus is running
    cluster.composer().kill(&nexus_node.id).await.unwrap();
    // 2. now we force unpublish the volume
    volumes_api
        .del_volume_target(&volume.spec.uuid, Some(true))
        .await
        .unwrap();
    // 3. publish on the previously unused node
    let volume = volumes_api
        .put_volume_target(
            &volume.spec.uuid,
            unused_node.id.as_str(),
            models::VolumeShareProtocol::Nvmf,
        )
        .await
        .unwrap();
    tracing::info!("Volume: {:?}", volume);

    // 4. now wait till the "broken" replica is disowned
    wait_till_replica_disowned(cluster, replica_nexus.parse().unwrap()).await;

    // 5. now wait till the volume becomes online again
    // (because we'll add a replica a rebuild)
    wait_till_volume_status(cluster, &volume.spec.uuid, models::VolumeStatus::Online).await;

    // 6. Bring back the mayastor and the original nexus and replica should be deleted
    cluster.composer().start(&nexus_node.id).await.unwrap();
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let specs = cluster.rest_v00().specs_api().get_specs().await.unwrap();
        if specs.nexuses.len() == 1 && specs.replicas.len() == 2 {
            break;
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!("Timeout waiting for the old nexus and replica to be removed");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    volumes_api.del_volume(&volume.spec.uuid).await.unwrap();
}

async fn wait_till_replica_disowned(cluster: &Cluster, replica_id: Uuid) {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let client = cluster.rest_v00();
    let specs_api = client.specs_api();
    let start = std::time::Instant::now();
    loop {
        let specs = specs_api.get_specs().await.unwrap();
        let replica_spec = specs
            .replicas
            .into_iter()
            .find(|r| r.uuid == replica_id)
            .unwrap();

        if replica_spec.owners.volume.is_none() {
            return;
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the replica to be disowned. Actual: '{:#?}'",
                replica_spec
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Creates a volume nexus on a mayastor instance, which will have both spec and state.
/// Stops/Kills the mayastor container. At some point we will have no nexus state, because the node
/// is gone. We then restart the node and the volume nexus reconciler will then recreate the nexus!
/// At this point, we'll have a state again and the volume will be Online!
async fn missing_nexus_reconcile(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 1,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let rest_api = cluster.rest_v00();
    let volumes_api = rest_api.volumes_api();

    let volume = volumes_api
        .put_volume_target(
            &volume.spec().uuid,
            cluster.node(0).as_str(),
            models::VolumeShareProtocol::Nvmf,
        )
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state;
    let nexus = volume_state.target.unwrap();

    cluster.composer().stop(nexus.node.as_str()).await.unwrap();
    let curr_nexus = wait_till_nexus_state(cluster, &nexus.uuid, None).await;
    assert_eq!(curr_nexus, None);

    cluster.composer().start(nexus.node.as_str()).await.unwrap();
    let curr_nexus = wait_till_nexus_state(cluster, &nexus.uuid, Some(&nexus)).await;
    assert_eq!(Some(nexus), curr_nexus);

    let volume_id = volume_state.uuid;
    let volume = volumes_api.get_volume(&volume_id).await.unwrap();
    assert_eq!(volume.state.status, models::VolumeStatus::Online);

    volumes_api.del_volume(&volume_id).await.unwrap();
}

/// Wait until the specified nexus state option matches the requested `state`
async fn wait_till_nexus_state(
    cluster: &Cluster,
    nexus_id: &Uuid,
    state: Option<&models::Nexus>,
) -> Option<models::Nexus> {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let client = cluster.rest_v00();
    let nexuses_api = client.nexuses_api();
    let start = std::time::Instant::now();
    loop {
        let nexus = nexuses_api.get_nexus(nexus_id).await;
        match &nexus {
            Ok(nexus) => {
                if let Some(state) = state {
                    if nexus.protocol != models::Protocol::None
                        && state.protocol != models::Protocol::None
                    {
                        return Some(nexus.clone());
                    }
                }
            }
            Err(Error::Response(response))
                if response.status() == StatusCode::NOT_FOUND && state.is_none() =>
            {
                return None;
            }
            _ => {}
        };

        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the nexus to have state: '{:#?}'. Actual: '{:#?}'",
                state, nexus
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Faults a volume nexus replica and waits for it to be replaced with a new one
async fn hotspare_faulty_children(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();

    let mut rpc_handle = cluster
        .composer()
        .grpc_handle(cluster.node(0).as_str())
        .await
        .unwrap();

    let children_before_fault = volume_children(volume.uuid()).await;
    tracing::info!("volume children: {:?}", children_before_fault);

    let fault_child = nexus.children.first().unwrap().uri.to_string();
    rpc_handle
        .mayastor
        .fault_nexus_child(FaultNexusChildRequest {
            uuid: nexus.uuid.to_string(),
            uri: fault_child.clone(),
        })
        .await
        .unwrap();

    tracing::debug!(
        "Nexus: {:?}",
        rpc_handle
            .mayastor
            .list_nexus(composer::rpc::mayastor::Null {})
            .await
            .unwrap()
    );

    let children = wait_till_volume_nexus(volume.uuid(), 2, &fault_child).await;
    tracing::info!("volume children: {:?}", children);

    assert_eq!(children.len(), 2);
    // the faulted child should have been replaced!
    assert!(!children.iter().any(|c| c.uri == fault_child));

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Wait for the published volume to have the specified replicas and to not having the specified
/// child. Wait up to the specified timeout.
async fn wait_till_volume_nexus(volume: &VolumeId, replicas: usize, no_child: &str) -> Vec<Child> {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let volume = GetVolumes::new(volume).request().await.unwrap();
        let volume_state = volume.0.clone().first().unwrap().state();
        let nexus = volume_state.target.clone().unwrap();
        let specs = GetSpecs::default().request().await.unwrap();
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
async fn volume_children(volume: &VolumeId) -> Vec<Child> {
    let volume = GetVolumes::new(volume).request().await.unwrap();
    let volume_state = volume.0.first().unwrap().state();
    volume_state.target.unwrap().children
}

/// Adds a child to the volume nexus (under the control plane) and waits till it gets removed
async fn hotspare_unknown_children(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();
    let size_mb = volume.spec().size / 1024 / 1024 + 5;

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();

    let mut rpc_handle = cluster
        .composer()
        .grpc_handle(cluster.node(0).as_str())
        .await
        .unwrap();

    let children_before_fault = volume_children(volume.uuid()).await;
    tracing::info!("volume children: {:?}", children_before_fault);

    let unknown_replica = format!(
        "malloc:///xxxxx?size_mb={}&uuid={}",
        size_mb,
        ReplicaId::new()
    );

    // todo: this sometimes fails??
    // is the reconciler interleaving with the add_child_nexus?
    rpc_handle
        .mayastor
        .add_child_nexus(composer::rpc::mayastor::AddChildNexusRequest {
            uuid: nexus.uuid.to_string(),
            uri: unknown_replica.clone(),
            norebuild: true,
        })
        .await
        .ok();

    tracing::debug!(
        "Nexus: {:?}",
        rpc_handle
            .mayastor
            .list_nexus(composer::rpc::mayastor::Null {})
            .await
            .unwrap()
    );
    let children = wait_till_volume_nexus(volume.uuid(), 2, &unknown_replica).await;
    tracing::info!("volume children: {:?}", children);

    assert_eq!(children.len(), 2);
    // the unknown child should have been replaced!
    assert!(!children.iter().any(|c| c.uri == unknown_replica));

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Remove a child from a volume nexus (under the control plane) and waits till it gets added back
async fn hotspare_missing_children(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);
    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();

    let mut rpc_handle = cluster
        .composer()
        .grpc_handle(cluster.node(0).as_str())
        .await
        .unwrap();

    let children_before_fault = volume_children(volume.uuid()).await;
    tracing::info!("volume children: {:?}", children_before_fault);

    let missing_child = nexus.children.first().unwrap().uri.to_string();
    rpc_handle
        .mayastor
        .remove_child_nexus(composer::rpc::mayastor::RemoveChildNexusRequest {
            uuid: nexus.uuid.to_string(),
            uri: missing_child.clone(),
        })
        .await
        .unwrap();

    tracing::debug!(
        "Nexus: {:?}",
        rpc_handle
            .mayastor
            .list_nexus(composer::rpc::mayastor::Null {})
            .await
            .unwrap()
    );
    let children = wait_till_volume_nexus(volume.uuid(), 2, &missing_child).await;
    tracing::info!("volume children: {:?}", children);

    assert_eq!(children.len(), 2);
    // the missing child should have been replaced!
    assert!(!children.iter().any(|c| c.uri == missing_child));

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// When more than 1 replicas are faulted at the same time, the new replicas should be spread
/// across the existing pools, and no pool nor any node should be reused
async fn hotspare_replica_count_spread(cluster: &Cluster) {
    let nodes = cluster.rest_v00().nodes_api().get_nodes().await.unwrap();
    assert!(
        nodes.len() >= 3,
        "We need enough nodes to be able to add at least 2 replicas"
    );
    let pools = cluster.rest_v00().pools_api().get_pools().await.unwrap();
    assert!(
        pools.len() >= nodes.len() * 2,
        "We need at least 2 pools per node to be able to test the failure case"
    );

    let replica_count = nodes.len();
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 1,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    // stop the core agent, so we can simulate `replica_count-1` data replicas being faulted at once
    // by increasing the replica count from 1 to `replica_count` under the core agent
    cluster.composer().stop("core").await.unwrap();

    let mut store = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd.");

    let mut volume_spec: VolumeSpec = store.get_obj(&volume.spec().key()).await.unwrap();
    volume_spec.num_replicas = replica_count as u8;
    tracing::info!("VolumeSpec: {:?}", volume_spec);
    store.put_obj(&volume_spec).await.unwrap();

    cluster.restart_core().await;

    let timeout_opts = TimeoutOptions::default()
        .with_max_retries(10)
        .with_timeout(Duration::from_millis(200))
        .with_timeout_backoff(Duration::from_millis(50));
    Liveness::default()
        .request_on_ext(ChannelVs::Volume, timeout_opts.clone())
        .await
        .expect("Should have restarted by now");

    // check we have the new replica_count
    wait_till_volume(volume.uuid(), replica_count).await;

    {
        let volume = cluster
            .rest_v00()
            .volumes_api()
            .get_volume(volume.uuid())
            .await
            .unwrap();

        tracing::info!("Replicas: {:?}", volume.state.replica_topology);

        assert_eq!(volume.spec.num_replicas, replica_count as u8);
        assert_eq!(volume.state.replica_topology.len(), replica_count);

        for node in 0 .. nodes.len() {
            let node = cluster.node(node as u32);
            let replicas = volume
                .state
                .replica_topology
                .values()
                .filter(|r| r.node == Some(node.to_string()));
            assert_eq!(replicas.count(), 1, "each node should have 1 replica");
        }
    }

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Remove a replica that belongs to a volume. Another should be created.
async fn hotspare_replica_count(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let specs = GetSpecs::default().request().await.unwrap();

    let replica_spec = specs.replicas.first().cloned().unwrap();
    let replicas = GetReplicas::new(&replica_spec.uuid).request().await;
    let replica = replicas.unwrap().0.first().unwrap().clone();

    // forcefully destroy a volume replica
    let mut destroy = DestroyReplica::from(replica);
    destroy.disowners = ReplicaOwners::from_volume(volume.uuid());
    destroy.request().await.unwrap();

    // check we have 2 replicas
    wait_till_volume(volume.uuid(), 2).await;

    // now add one extra replica (it should be removed)
    CreateReplica {
        node: cluster.node(1),
        name: Default::default(),
        uuid: ReplicaId::new(),
        pool: cluster.pool(1, 0),
        size: volume.spec().size / 1024 / 1024 + 5,
        thin: false,
        share: Default::default(),
        managed: true,
        owners: ReplicaOwners::from_volume(volume.uuid()),
    }
    .request()
    .await
    .unwrap();

    wait_till_volume(volume.uuid(), 2).await;

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Remove a replica that belongs to a volume. Another should be created.
async fn hotspare_nexus_replica_count(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: "1e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = PublishVolume::new(volume.spec().uuid.clone(), Some(cluster.node(0)), None)
        .request()
        .await
        .unwrap();

    tracing::info!("Volume: {:?}", volume);

    let timeout_opts = TimeoutOptions::default()
        .with_max_retries(10)
        .with_timeout(Duration::from_millis(500))
        .with_timeout_backoff(Duration::from_millis(50));
    let mut store = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd.");

    let mut volume_spec: VolumeSpec = store.get_obj(&volume.spec().key()).await.unwrap();
    volume_spec.num_replicas += 1;
    tracing::info!("VolumeSpec: {:?}", volume_spec);
    store.put_obj(&volume_spec).await.unwrap();

    cluster.restart_core().await;

    Liveness::default()
        .request_on_ext(ChannelVs::Volume, timeout_opts.clone())
        .await
        .expect("Should have restarted by now");

    wait_till_volume_nexus(volume.uuid(), volume_spec.num_replicas as usize, "").await;

    let mut volume_spec: VolumeSpec = store.get_obj(&volume.spec().key()).await.unwrap();
    volume_spec.num_replicas -= 1;
    tracing::info!("VolumeSpec: {:?}", volume_spec);
    store.put_obj(&volume_spec).await.unwrap();

    cluster.restart_core().await;
    Liveness::default()
        .request_on_ext(ChannelVs::Volume, timeout_opts)
        .await
        .expect("Should have restarted by now");

    wait_till_volume_nexus(volume.uuid(), volume_spec.num_replicas as usize, "").await;

    DestroyVolume::new(volume.uuid()).request().await.unwrap();
}

/// Wait for the unpublished volume to have the specified replica count
async fn wait_till_volume(volume: &VolumeId, replicas: usize) {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        // the volume state does not carry replica information, so inspect the replica spec instead.
        let specs = GetSpecs::default().request().await.unwrap();
        let replica_specs = specs
            .replicas
            .iter()
            .filter(|r| r.owners.owned_by(volume))
            .collect::<Vec<_>>();

        if replica_specs.len() == replicas {
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

/// Wait for a volume to reach the provided status
async fn wait_till_volume_status(cluster: &Cluster, volume: &Uuid, status: models::VolumeStatus) {
    let timeout = Duration::from_secs(RECONCILE_TIMEOUT_SECS);
    let start = std::time::Instant::now();
    loop {
        let volume = cluster.rest_v00().volumes_api().get_volume(volume).await;
        if volume.as_ref().unwrap().state.status == status {
            return;
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!(
                "Timeout waiting for the volume to reach the specified status ('{:?}'), current: '{:?}'",
                status, volume
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Either fault the local replica, the remote, or set the nexus as having an unclean shutdown
#[derive(Debug)]
enum FaultTest {
    Local,
    Remote,
    Unclean,
}

#[tracing::instrument(skip(cluster))]
async fn nexus_persistence_test(cluster: &Cluster) {
    for (local, remote) in &vec![
        (cluster.node(0), cluster.node(1)),
        (cluster.node(1), cluster.node(0)),
    ] {
        for test in vec![FaultTest::Local, FaultTest::Remote, FaultTest::Unclean] {
            nexus_persistence_test_iteration(local, remote, test).await;
        }
    }
}
async fn nexus_persistence_test_iteration(local: &NodeId, remote: &NodeId, fault: FaultTest) {
    tracing::debug!("arguments ({:?}, {:?}, {:?})", local, remote, fault);
    let allowed_nodes = vec![local.to_string(), remote.to_string()];
    let preferred_nodes: Vec<String> = vec![];
    let volume = CreateVolume {
        uuid: "6e3cf927-80c2-47a8-adf0-95c486bdd7b7".try_into().unwrap(),
        size: 5242880,
        replicas: 2,
        topology: Some(Topology::from(models::Topology::new_all(
            Some(models::NodeTopology::explicit(
                models::ExplicitNodeTopology::new(allowed_nodes, preferred_nodes),
            )),
            None,
        ))),
        ..Default::default()
    }
    .request()
    .await
    .unwrap();
    tracing::info!("Volume: {:?}", volume);

    let volume = PublishVolume {
        uuid: volume.spec().uuid.clone(),
        // publish it on the remote first, to complicate things
        target_node: Some(remote.clone()),
        share: None,
    }
    .request()
    .await
    .unwrap();

    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    let nexus_uuid = nexus.uuid.clone();

    UnpublishVolume::new(&volume_state.uuid, false)
        .request()
        .await
        .unwrap();

    let mut store = Etcd::new("0.0.0.0:2379")
        .await
        .expect("Failed to connect to etcd.");
    let mut nexus_info: NexusInfo = store
        .get_obj(&NexusInfoKey::from(&nexus_uuid))
        .await
        .unwrap();
    nexus_info.uuid = nexus_uuid.clone();
    tracing::info!("NexusInfo: {:?}", nexus_info);

    let replicas = GetReplicas {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let node_child = |node: &NodeId, nexus: &Nexus, replicas: Replicas| {
        let replica = replicas.into_inner().into_iter().find(|r| &r.node == node);
        nexus
            .children
            .iter()
            .find(|c| Some(c.uri.as_str()) == replica.as_ref().map(|r| r.uri.as_str()))
            .cloned()
            .unwrap()
    };

    let mark_child_unhealthy = |c: &Child, ni: &mut NexusInfo| {
        let uri = url::Url::from_str(c.uri.as_str()).unwrap();
        let uuid = uri.query_pairs().find(|(q, _)| q == "uuid").unwrap().1;
        let child_info = ni.children.iter_mut().find(|c| c.uuid == uuid);
        child_info.unwrap().healthy = false;
    };
    match fault {
        FaultTest::Local => {
            let local_child = node_child(local, &nexus, replicas);
            mark_child_unhealthy(&local_child, &mut nexus_info);
        }
        FaultTest::Remote => {
            let remote_child = node_child(remote, &nexus, replicas);
            mark_child_unhealthy(&remote_child, &mut nexus_info);
        }
        FaultTest::Unclean => {
            nexus_info.clean_shutdown = false;
        }
    }
    store.put_obj(&nexus_info).await.unwrap();
    nexus_info = store
        .get_obj(&NexusInfoKey::from(&nexus_uuid))
        .await
        .unwrap();
    nexus_info.uuid = nexus_uuid.clone();
    tracing::info!("NexusInfo: {:?}", nexus_info);

    let volume = PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: Some(local.clone()),
        share: None,
    }
    .request()
    .await
    .unwrap();
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    let nexus = volume_state.target.unwrap().clone();
    tracing::info!("Nexus: {:?}", nexus);
    assert_eq!(nexus.children.len(), 1);

    let replicas = GetReplicas {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let child = nexus.children.first().unwrap();
    match fault {
        FaultTest::Local => {
            let remote_child = node_child(remote, &nexus, replicas);
            assert_eq!(child.uri, remote_child.uri);
        }
        FaultTest::Remote => {
            let local_child = node_child(local, &nexus, replicas);
            assert_eq!(child.uri, local_child.uri);
        }
        FaultTest::Unclean => {
            // if the shutdown is not clean, then we prefer the local replica
            let local_child = node_child(local, &nexus, replicas);
            assert_eq!(child.uri, local_child.uri);
        }
    }

    DestroyVolume {
        uuid: volume_state.uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

#[tracing::instrument(skip(cluster))]
async fn publishing_test(cluster: &Cluster) {
    let volume = CreateVolume {
        uuid: VolumeId::try_from("359b7e1a-b724-443b-98b4-e6d97fabbb40").unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume.request().await.unwrap();
    let volumes = GetVolumes::default().request().await.unwrap().0;
    tracing::info!("Volumes: {:?}", volumes);
    assert_eq!(Some(&volume), volumes.first());

    let volume = PublishVolume {
        uuid: volume.spec().uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect("Should be able to publish a newly created volume");

    let volume_state = volume.state();

    tracing::info!(
        "Published on: {}",
        volume_state.target.clone().unwrap().node
    );

    let share = ShareVolume {
        uuid: volume_state.uuid.clone(),
        protocol: Default::default(),
    }
    .request()
    .await
    .unwrap();

    tracing::info!("Share: {}", share);

    ShareVolume {
        uuid: volume_state.uuid.clone(),
        protocol: Default::default(),
    }
    .request()
    .await
    .expect_err("Can't share a shared volume");

    UnshareVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .expect("Should be able to unshare a shared volume");

    UnshareVolume {
        uuid: volume_state.uuid.clone(),
    }
    .request()
    .await
    .expect_err("Can't unshare an unshared volume");

    PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: None,
        share: None,
    }
    .request()
    .await
    .expect_err("The Volume cannot be published again because it's already published");

    UnpublishVolume::new(&volume_state.uuid, false)
        .request()
        .await
        .unwrap();

    let volume = PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: Some(cluster.node(0)),
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");

    let volume_state = volume.state();
    let nx = volume_state.target.unwrap();
    tracing::info!("Published on '{}' with share '{}'", nx.node, nx.device_uri);

    let volumes = GetVolumes {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let first_volume_state = volumes.0.first().unwrap().state();
    assert_eq!(
        first_volume_state.target_protocol(),
        Some(VolumeShareProtocol::Iscsi)
    );
    assert_eq!(
        first_volume_state.target_node(),
        Some(Some(cluster.node(0)))
    );

    PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: None,
        share: Some(VolumeShareProtocol::Iscsi),
    }
    .request()
    .await
    .expect_err("The volume is already published");

    UnpublishVolume::new(&volume_state.uuid, false)
        .request()
        .await
        .unwrap();

    let volume = PublishVolume {
        uuid: volume_state.uuid.clone(),
        target_node: Some(cluster.node(1)),
        share: None,
    }
    .request()
    .await
    .expect("The volume is unpublished so we should be able to publish again");

    let volume_state = volume.state();
    tracing::info!(
        "Published on: {}",
        volume_state.target.clone().unwrap().node
    );

    let volumes = GetVolumes {
        filter: Filter::Volume(volume_state.uuid.clone()),
    }
    .request()
    .await
    .unwrap();

    let first_volume_state = volumes.0.first().unwrap().state();
    assert_eq!(
        first_volume_state.target_protocol(),
        None,
        "Was published but not shared"
    );
    assert_eq!(
        first_volume_state.target_node(),
        Some(Some(cluster.node(1)))
    );

    let target_node = first_volume_state.target_node().flatten().unwrap();
    cluster.composer().kill(target_node.as_str()).await.unwrap();

    UnpublishVolume::new(&volume_state.uuid, false)
        .request()
        .await
        .expect_err("The node is not online...");

    UnpublishVolume::new(&volume_state.uuid, true)
        .request()
        .await
        .expect("With force comes great responsibility...");

    cluster
        .composer()
        .start(target_node.as_str())
        .await
        .unwrap();
    wait_for_node_online(cluster, &target_node).await;

    DestroyVolume {
        uuid: volume_state.uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn get_volume(volume: &VolumeState) -> Volume {
    let request = GetVolumes {
        filter: Filter::Volume(volume.uuid.clone()),
    }
    .request()
    .await
    .unwrap();
    request.into_inner().first().cloned().unwrap()
}

async fn wait_for_node_online(cluster: &Cluster, node: &NodeId) {
    let client = cluster.rest_v00();

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);
    loop {
        if let Ok(node) = client.nodes_api().get_node(node.as_str()).await {
            let status = node.state.map(|n| n.status).unwrap_or(NodeStatus::Unknown);
            if status == NodeStatus::Online {
                return;
            }
        }

        if std::time::Instant::now() > (start + timeout) {
            panic!("Timeout waiting for the node to become online");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_volume_online(volume: &VolumeState) -> Result<VolumeState, ()> {
    let mut volume = get_volume(volume).await;
    let mut volume_state = volume.state();
    let mut tries = 0;
    while volume_state.status != VolumeStatus::Online && tries < 20 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        volume = get_volume(&volume_state).await;
        volume_state = volume.state();
        tries += 1;
    }
    if volume_state.status == VolumeStatus::Online {
        Ok(volume_state)
    } else {
        Err(())
    }
}

async fn replica_count_test() {
    let volume = CreateVolume {
        uuid: VolumeId::try_from("359b7e1a-b724-443b-98b4-e6d97fabbb40").unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume.request().await.unwrap();
    let volumes = GetVolumes::default().request().await.unwrap().0;
    tracing::info!("Volumes: {:?}", volumes);
    assert_eq!(Some(&volume), volumes.first());

    let volume = PublishVolume {
        uuid: volume.spec().uuid.clone(),
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let volume = SetVolumeReplica {
        uuid: volume.spec().uuid.clone(),
        replicas: 3,
    }
    .request()
    .await
    .expect("Should have enough nodes/pools to increase replica count");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    let error = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 4,
    }
    .request()
    .await
    .expect_err("The volume is degraded (rebuild in progress)");
    tracing::error!("error: {:?}", error);
    assert!(matches!(
        error,
        mbus_api::Error::ReplyWithError {
            source: ReplyError {
                kind: ReplyErrorKind::ReplicaIncrease,
                resource: ResourceKind::Volume,
                ..
            },
        }
    ));

    let volume = wait_for_volume_online(&volume_state).await.unwrap();

    let error = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 4,
    }
    .request()
    .await
    .expect_err("Not enough pools available");
    tracing::error!("error: {:?}", error);

    assert!(matches!(
        error,
        mbus_api::Error::ReplyWithError {
            source: ReplyError {
                kind: ReplyErrorKind::ResourceExhausted,
                resource: ResourceKind::Pool,
                ..
            },
        }
    ));

    let volume = SetVolumeReplica {
        uuid: volume.uuid.clone(),
        replicas: 2,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back down");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    let volume = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 1,
    }
    .request()
    .await
    .expect("Should be able to bring the replica to 1");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    assert_eq!(volume_state.status, VolumeStatus::Online);
    assert!(!volume_state
        .target
        .iter()
        .any(|n| n.children.iter().any(|c| c.state != ChildState::Online)));

    let error = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 0,
    }
    .request()
    .await
    .expect_err("Can't bring the replica count down to 0");
    tracing::error!("error: {:?}", error);

    assert!(matches!(
        error,
        mbus_api::Error::ReplyWithError {
            source: ReplyError {
                kind: ReplyErrorKind::FailedPrecondition,
                resource: ResourceKind::Volume,
                ..
            },
        }
    ));

    let volume = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 2,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back to 2");
    tracing::info!("Volume: {:?}", volume);

    let volume_state = volume.state();
    UnpublishVolume::new(&volume_state.uuid, false)
        .request()
        .await
        .unwrap();

    let volume = SetVolumeReplica {
        uuid: volume_state.uuid.clone(),
        replicas: 3,
    }
    .request()
    .await
    .expect("Should be able to bring the replica count back to 3");
    tracing::info!("Volume: {:?}", volume);

    DestroyVolume {
        uuid: volume.spec().uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}

async fn smoke_test() {
    let volume = CreateVolume {
        uuid: VolumeId::try_from("359b7e1a-b724-443b-98b4-e6d97fabbb40").unwrap(),
        size: 5242880,
        replicas: 2,
        ..Default::default()
    };

    let volume = volume.request().await.unwrap();
    let volumes = GetVolumes::default().request().await.unwrap().0;
    tracing::info!("Volumes: {:?}", volumes);

    assert_eq!(Some(&volume), volumes.first());

    DestroyVolume {
        uuid: volume.spec().uuid,
    }
    .request()
    .await
    .expect("Should be able to destroy the volume");

    assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
}
