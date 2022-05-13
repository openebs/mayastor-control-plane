#![cfg(test)]

use common_lib::{
    mbus_api::*,
    types::v0::{
        message_bus::{
            AddNexusChild, CreateNexus, CreateReplica, DestroyNexus, DestroyReplica, Filter,
            GetNexuses, GetSpecs, Nexus, NexusId, NexusShareProtocol, Protocol, RemoveNexusChild,
            ReplicaId, ShareNexus, UnshareNexus,
        },
        store::nexus::NexusSpec,
    },
};
use grpc::operations::{
    node::traits::NodeOperations, registry::traits::RegistryOperations,
    replica::traits::ReplicaOperations,
};
use std::{convert::TryFrom, time::Duration};
use testlib::{Cluster, ClusterBuilder};

#[tokio::test]
async fn nexus() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_pools(2)
        .build()
        .await
        .unwrap();

    let io_engine = cluster.node(0);
    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let rep_client = cluster.grpc_client().replica();

    let replica = rep_client
        .create(
            &CreateReplica {
                node: cluster.node(1),
                uuid: ReplicaId::new(),
                pool: cluster.pool(1, 0),
                size: 12582912, /* actual size will be a multiple of 4MB so just
                                 * create it like so */
                thin: true,
                share: Protocol::Nvmf,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let local = "malloc:///local?size_mb=12&uuid=4a7b0566-8ec6-49e0-a8b2-1d9a292cf59b".into();

    let nexus = CreateNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
        size: 5242880,
        children: vec![replica.uri.clone().into(), local],
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let nexuses = GetNexuses::default().request().await.unwrap().0;
    tracing::info!("Nexuses: {:?}", nexuses);
    assert_eq!(Some(&nexus), nexuses.first());

    ShareNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
        key: None,
        protocol: NexusShareProtocol::Nvmf,
    }
    .request()
    .await
    .unwrap();

    DestroyNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
    }
    .request()
    .await
    .unwrap();

    rep_client
        .destroy(&DestroyReplica::from(replica), None)
        .await
        .unwrap();

    assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
}

/// The tests below revolve around transactions and are dependent on the core agent's command line
/// arguments for timeouts.
/// This is required because as of now, we don't have a good mocking strategy

/// default timeout options for every bus request
fn bus_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::default()
        .with_max_retries(0)
        .with_timeout(Duration::from_millis(250))
        .with_req_timeout(None)
}

/// Get the nexus spec
async fn nexus_spec(replica: &Nexus, client: &dyn RegistryOperations) -> Option<NexusSpec> {
    let specs = client.get_specs(&GetSpecs {}, None).await.unwrap().nexuses;
    specs.iter().find(|r| r.uuid == replica.uuid).cloned()
}

/// Tests nexus share and unshare operations as a transaction
#[tokio::test]
async fn nexus_share_transaction() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        .with_agents(vec!["core"])
        .with_req_timeouts(Duration::from_millis(350), Duration::from_millis(350))
        .with_bus_timeouts(bus_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);

    let node_client = cluster.grpc_client().node();
    let registry_client = cluster.grpc_client().registry();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let local = "malloc:///local?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into();
    let nexus = CreateNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
        size: 5242880,
        children: vec![local],
        ..Default::default()
    }
    .request()
    .await
    .unwrap();
    let share = ShareNexus::from((&nexus, None, NexusShareProtocol::Nvmf));

    async fn check_share_operation(
        nexus: &Nexus,
        protocol: Protocol,
        registry_client: &dyn RegistryOperations,
    ) {
        // operation in progress
        assert!(nexus_spec(nexus, registry_client)
            .await
            .unwrap()
            .operation
            .is_some());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // operation is completed
        assert!(nexus_spec(nexus, registry_client)
            .await
            .unwrap()
            .operation
            .is_none());
        assert_eq!(
            nexus_spec(nexus, registry_client).await.unwrap().share,
            protocol
        );
    }

    // pause io_engine
    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    share
        .request_ext(bus_timeout_opts())
        .await
        .expect_err("io_engine is down");

    check_share_operation(&nexus, Protocol::None, &registry_client).await;

    // unpause io_engine
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    // now it should be shared successfully
    let uri = share.request().await.unwrap();
    println!("Share uri: {}", uri);

    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    UnshareNexus::from(&nexus)
        .request_ext(bus_timeout_opts())
        .await
        .expect_err("io_engine down");

    check_share_operation(&nexus, Protocol::Nvmf, &registry_client).await;

    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    UnshareNexus::from(&nexus).request().await.unwrap();

    assert_eq!(
        nexus_spec(&nexus, &registry_client).await.unwrap().share,
        Protocol::None
    );
}

/// Tests Store Write Failures for Nexus Child Operations
/// As it stands, the tests expects the operation to not be undone, and
/// a reconcile thread should eventually sync the specs when the store reappears
async fn nexus_child_op_transaction_store<R>(
    nexus: &Nexus,
    cluster: &Cluster,
    (store_timeout, reconcile_period, grpc_timeout): (Duration, Duration, Duration),
    (request, children, share): (R, usize, Protocol),
) where
    R: Message,
    R::Reply: std::fmt::Debug,
{
    let io_engine = cluster.node(0);

    // pause io_engine
    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    request
        .request_ext(bus_timeout_opts())
        .await
        .expect_err("io_engine down");

    // ensure the op will succeed but etcd store will fail
    // by pausing etcd and releasing io_engine
    cluster.composer().pause("etcd").await.unwrap();
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    let registry_client = cluster.grpc_client().registry();
    // hopefully we have enough time before the store times out
    let spec = nexus_spec(nexus, &registry_client).await.unwrap();
    assert!(spec.operation.unwrap().result.is_none());

    // let the store write time out
    tokio::time::sleep(grpc_timeout + store_timeout).await;

    // and now we have a result but the operation is still pending until
    // we can sync the spec
    let spec = nexus_spec(nexus, &registry_client).await.unwrap();
    assert!(spec.operation.unwrap().result.is_some());

    // thaw etcd allowing the worker thread to sync the "dirty" spec
    cluster.composer().thaw("etcd").await.unwrap();

    // wait for the reconciler to do its thing
    tokio::time::sleep(reconcile_period * 2).await;

    // and now we're in sync and the pending operation is no more
    let spec = nexus_spec(nexus, &registry_client).await.unwrap();
    assert!(spec.operation.is_none());
    assert_eq!(spec.children.len(), children);
    assert_eq!(spec.share, share);

    request
        .request_ext(bus_timeout_opts())
        .await
        .expect_err("operation already performed");
}

/// Tests nexus share and unshare operations when the store is temporarily down
/// TODO: these tests don't work anymore because the io_engine also writes child healthy states
/// to etcd so we can't simply pause etcd anymore..
#[tokio::test]
#[ignore]
async fn nexus_share_transaction_store() {
    let store_timeout = Duration::from_millis(250);
    let reconcile_period = Duration::from_millis(250);
    let grpc_timeout = Duration::from_millis(350);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_bus_timeouts(bus_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);

    let local = "malloc:///local?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into();
    let nexus = CreateNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
        size: 5242880,
        children: vec![local],
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    // test the share operation
    let share = ShareNexus::from((&nexus, None, NexusShareProtocol::Nvmf));
    nexus_child_op_transaction_store(
        &nexus,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        (share, 1, Protocol::Nvmf),
    )
    .await;

    // test the unshare operation
    let unshare = UnshareNexus::from(&nexus);
    nexus_child_op_transaction_store(
        &nexus,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        (unshare, 1, Protocol::None),
    )
    .await;
}

/// Tests child add and remove operations as a transaction
#[tokio::test]
async fn nexus_child_transaction() {
    let grpc_timeout = Duration::from_millis(350);
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_pools(1)
        .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_bus_timeouts(bus_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);
    let node_client = cluster.grpc_client().node();
    let registry_client = cluster.grpc_client().registry();
    let nodes = node_client.get(Filter::None, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let child2 = "malloc:///ch2?size_mb=12&uuid=4a7b0566-8ec6-49e0-a8b2-1d9a292cf59b";
    let nexus = CreateNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
        size: 5242880,
        children: vec!["malloc:///ch1?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into()],
        ..Default::default()
    }
    .request()
    .await
    .unwrap();
    let add_child = AddNexusChild {
        node: io_engine.clone(),
        nexus: nexus.uuid.clone(),
        uri: child2.into(),
        auto_rebuild: true,
    };
    let rm_child = RemoveNexusChild {
        node: io_engine.clone(),
        nexus: nexus.uuid.clone(),
        uri: child2.into(),
    };

    async fn check_child_operation(
        nexus: &Nexus,
        children: usize,
        registry_client: &dyn RegistryOperations,
    ) {
        // operation in progress
        assert!(nexus_spec(nexus, registry_client)
            .await
            .unwrap()
            .operation
            .is_some());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // operation is complete
        assert!(nexus_spec(nexus, registry_client)
            .await
            .unwrap()
            .operation
            .is_none());
        assert_eq!(
            nexus_spec(nexus, registry_client)
                .await
                .unwrap()
                .children
                .len(),
            children
        );
    }

    // pause io_engine
    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    add_child
        .request_ext(bus_timeout_opts())
        .await
        .expect_err("io_engine is down");

    check_child_operation(&nexus, 1, &registry_client).await;

    // unpause io_engine
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    // now it should be shared successfully
    let uri = add_child.request().await.unwrap();
    println!("Share uri: {:?}", uri);

    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    rm_child
        .request_ext(bus_timeout_opts())
        .await
        .expect_err("io_engine down");

    check_child_operation(&nexus, 2, &registry_client).await;

    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    rm_child.request().await.unwrap();

    assert_eq!(
        nexus_spec(&nexus, &registry_client)
            .await
            .unwrap()
            .children
            .len(),
        1
    );
}

/// Tests child add and remove operations when the store is temporarily down
/// TODO: these tests don't work anymore because the io_engine also writes child healthy states
/// to etcd so we can't simply pause etcd anymore..
#[tokio::test]
#[ignore]
async fn nexus_child_transaction_store() {
    let store_timeout = Duration::from_millis(250);
    let reconcile_period = Duration::from_millis(250);
    let grpc_timeout = Duration::from_millis(450);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_bus_timeouts(bus_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);

    let nexus = CreateNexus {
        node: io_engine.clone(),
        uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
        size: 5242880,
        children: vec!["malloc:///ch1?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into()],
        ..Default::default()
    }
    .request()
    .await
    .unwrap();

    let child2 = "malloc:///ch2?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0db";
    let add_child = AddNexusChild {
        node: io_engine.clone(),
        nexus: nexus.uuid.clone(),
        uri: child2.into(),
        auto_rebuild: true,
    };
    nexus_child_op_transaction_store(
        &nexus,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        (add_child, 2, Protocol::None),
    )
    .await;

    let del_child = RemoveNexusChild {
        node: io_engine.clone(),
        nexus: nexus.uuid.clone(),
        uri: child2.into(),
    };
    nexus_child_op_transaction_store(
        &nexus,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        (del_child, 1, Protocol::None),
    )
    .await;
}
