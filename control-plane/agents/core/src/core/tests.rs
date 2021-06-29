#![cfg(test)]

use common_lib::{
    mbus_api::Message,
    types::v0::message_bus::{
        mbus,
        mbus::{ChannelVs, Liveness},
    },
};
use testlib::*;

/// Test that the content of the registry is correctly loaded from the persistent store on start up.
#[actix_rt::test]
async fn bootstrap_registry() {
    let size = 15 * 1024 * 1024;
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_pools(1)
        .with_replicas(1, size, mbus::Protocol::Off)
        .with_agents(vec!["core"])
        .build()
        .await
        .unwrap();

    let replica = format!("loopback:///{}", Cluster::replica(0, 0, 0));
    cluster
        .rest_v0()
        .create_nexus(mbus::CreateNexus {
            node: cluster.node(0),
            uuid: mbus::NexusId::new(),
            size,
            children: vec![replica.into()],
            ..Default::default()
        })
        .await
        .expect("Failed to create nexus");

    // Get all resource specs.
    let specs = cluster
        .rest_v0()
        .get_specs()
        .await
        .expect("Failed to get resource specs");

    // Restart the core agent with the expectation that the registry will have all its resource
    // specs loaded from the persistent store.
    cluster
        .composer()
        .restart("core")
        .await
        .expect("Failed to restart core agent");

    // Wait for core service to restart.
    Liveness {}.request_on(ChannelVs::Core).await.unwrap();

    // Get the specs after the core agent has restarted and check that they match what was there
    // before.
    let restart_specs = cluster
        .rest_v0()
        .get_specs()
        .await
        .expect("Failed to get resource specs after restart");
    assert_eq!(specs, restart_specs);
}
