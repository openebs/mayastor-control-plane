#![allow(unused)]

/**
This file contains tests for any policies that are present
in the control plane.
*/
use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{nexus::traits::NexusOperations, node::traits::NodeOperations};

use stor_port::types::v0::transport::{CreateNexus, Filter, Nexus, NexusId};

mod pmodule {
    include!("../../controller/policies/rebuild_policies.rs");
}

async fn build_cluster(num_ioe: u32, num_pools: u32) -> Cluster {
    ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(num_ioe)
        .with_pools(num_pools)
        .build()
        .await
        .unwrap()
}

/// Test that rebuild policy assignment based on volume size works correctly
#[tokio::test]
async fn assign_policy() {
    let cluster = build_cluster(1, 2).await;

    let io_engine = cluster.node(0);
    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let nexus_client = cluster.grpc_client().nexus();
    let local = "malloc:///local?size_mb=12&uuid=4a7b0566-8ec6-49e0-a8b2-1d9a292cf59b".into();
    let nexus = nexus_client
        .create(
            &CreateNexus {
                node: io_engine.clone(),
                uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
                size: 5242880,
                children: vec![local],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    assert!(!nexus.children.is_empty());

    // Test if the policy is returning expected duration value.
    // Validations need to become more elaborate as we expand the
    // purview of policies.
    let twait = pmodule::RuleSet::faulted_child_wait(&nexus);
    assert!(!twait.is_zero());
    assert!(twait.cmp(&pmodule::TWAIT_SPERF).is_eq() || twait.cmp(&pmodule::TWAIT_SAVAIL).is_eq());
}
