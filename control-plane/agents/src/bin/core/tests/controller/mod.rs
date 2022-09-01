use common_lib::{
    store::etcd::Etcd,
    types::v0::{
        openapi::models,
        store::{
            definitions::Store,
            registry::{ControlPlaneService, StoreLeaseOwner, StoreLeaseOwnerKey},
        },
        transport,
    },
};
use deployer_cluster::{etcd_client::Client, *};

/// Test that the content of the registry is correctly loaded from the persistent store on start up.
#[tokio::test]
async fn bootstrap_registry() {
    let size = 15 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_pools(1)
        .with_replicas(1, size, transport::Protocol::None)
        .with_agents(vec!["core"])
        .build()
        .await
        .unwrap();

    let client = cluster.rest_v00();

    let replica = client
        .replicas_api()
        .get_replica(&Cluster::replica(0, 0, 0))
        .await
        .unwrap();
    client
        .nexuses_api()
        .put_node_nexus(
            cluster.node(0).as_str(),
            &transport::NexusId::new(),
            models::CreateNexusBody::new(vec![replica.uri], size),
        )
        .await
        .expect("Failed to create nexus");

    // Get all resource specs.
    let specs = client
        .specs_api()
        .get_specs()
        .await
        .expect("Failed to get resource specs");

    // Restart the core agent with the expectation that the registry will have all its resource
    // specs loaded from the persistent store.
    cluster.restart_core().await;

    // Wait for core service to restart.
    cluster
        .node_service_liveness(None)
        .await
        .expect("Should have restarted by now");

    // Get the specs after the core agent has restarted and check that they match what was there
    // before.
    let restart_specs = client
        .specs_api()
        .get_specs()
        .await
        .expect("Failed to get resource specs after restart");
    assert_eq!(specs, restart_specs);
}

/// Test that store lease lock in the core agent works as expected
#[tokio::test]
async fn store_lease_lock() {
    // deploy etcd only...
    let _cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_jaeger(false)
        .with_io_engines(0)
        .with_agents(vec![])
        .build()
        .await
        .unwrap();

    let lease_ttl = std::time::Duration::from_secs(2);
    let _core_agent = Etcd::new_leased(["0.0.0.0:2379"], ControlPlaneService::CoreAgent, lease_ttl)
        .await
        .unwrap();

    let mut store = Client::connect(["0.0.0.0:2379"], None)
        .await
        .expect("Failed to connect to etcd.");

    let leases = store.leases().await.unwrap();
    let lease_id = leases.leases().first().unwrap().id();
    tracing::info!("lease_id: {:?}", lease_id);

    tokio::time::sleep(lease_ttl).await;

    let mut etcd = Etcd::new("0.0.0.0:2379").await.unwrap();
    let svc = ControlPlaneService::CoreAgent;
    let obj: StoreLeaseOwner = etcd
        .get_obj(&StoreLeaseOwnerKey::new(&svc))
        .await
        .expect("Should exist!");
    tracing::info!("EtcdLeaseOwnerKey: {:?}", obj);
    assert_eq!(
        obj.lease_id(),
        format!("{:x}", lease_id),
        "Lease should be the same!"
    );

    let _core_agent2 =
        Etcd::new_leased(["0.0.0.0:2379"], ControlPlaneService::CoreAgent, lease_ttl)
            .await
            .expect_err("One core-agent is already running!");
}

/// Test that store lease lock works as expected
#[tokio::test]
async fn core_agent_lease_lock() {
    let lease_ttl = std::time::Duration::from_secs(2);
    let lease_ttl_wait = lease_ttl + std::time::Duration::from_secs(1);
    let cluster = ClusterBuilder::builder()
        .with_io_engines(1)
        .with_agents(vec!["core"])
        .with_store_lease_ttl(lease_ttl)
        .build()
        .await
        .unwrap();

    let mut store = Client::connect(["0.0.0.0:2379"], None)
        .await
        .expect("Failed to connect to etcd.");

    let leases = store.leases().await.unwrap();
    let lease_id = leases.leases().first().unwrap().id();
    tracing::info!("lease_id: {:?}", lease_id);

    tokio::time::sleep(lease_ttl).await;

    let mut etcd = Etcd::new("0.0.0.0:2379").await.unwrap();
    let svc = ControlPlaneService::CoreAgent;
    let obj: StoreLeaseOwner = etcd
        .get_obj(&StoreLeaseOwnerKey::new(&svc))
        .await
        .expect("Should exist!");
    tracing::info!("EtcdLeaseOwnerKey: {:?}", obj);
    assert_eq!(
        obj.lease_id(),
        format!("{:x}", lease_id),
        "Lease should be the same!"
    );

    let _core_agent2 =
        Etcd::new_leased(["0.0.0.0:2379"], ControlPlaneService::CoreAgent, lease_ttl)
            .await
            .expect_err("One core-agent is already running!");

    // pause the core agent
    cluster.composer().pause("core").await.unwrap();
    // let its lease expire
    tokio::time::sleep(lease_ttl_wait).await;

    let leases = store.leases().await.unwrap();
    tracing::info!("Leases: {:?}", leases);
    assert!(leases.leases().is_empty());

    // bring back the core-agent which should be able to reestablish the lease since it hasn't lost
    // it to another core-agent instance
    cluster.composer().thaw("core").await.unwrap();

    tokio::time::sleep(lease_ttl_wait).await;

    let leases = store.leases().await.unwrap();
    let current_lease_id = leases.leases().first().unwrap().id();
    tracing::info!("lease_id: {:?}", current_lease_id);
    // it's the same lease as it's the same core-agent instance...
    assert_eq!(lease_id, current_lease_id);

    // pause the core agent
    cluster.composer().pause("core").await.unwrap();
    // let its lease expire
    tokio::time::sleep(lease_ttl_wait).await;

    let _core_agent2 =
        Etcd::new_leased(["0.0.0.0:2379"], ControlPlaneService::CoreAgent, lease_ttl)
            .await
            .expect("First core-agent expired, the second one can now run!");

    let leases = store.leases().await.unwrap();
    let current_lease_id = leases.leases().first().unwrap().id();
    tracing::info!("lease_id: {:?}", current_lease_id);
    // it's a new lease, from the new core-agent instance
    assert_ne!(lease_id, current_lease_id);

    // unpause the core agent
    cluster.composer().thaw("core").await.unwrap();
    // it should not be able to regain the lock and will panic!

    tokio::time::sleep(lease_ttl_wait).await;
    let core = cluster.composer().inspect("core").await.unwrap();
    tracing::info!("core: {:?}", core.state);
    assert_eq!(Some(false), core.state.unwrap().running);
}
