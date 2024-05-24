use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{
    nexus::traits::NexusOperations, node::traits::NodeOperations,
    registry::traits::RegistryOperations, replica::traits::ReplicaOperations,
};
use std::{convert::TryFrom, time::Duration};
use stor_port::{
    transport_api::*,
    types::v0::{
        store::nexus::NexusSpec,
        transport::{
            AddNexusChild, CreateNexus, CreateReplica, DestroyNexus, DestroyReplica, Filter,
            GetNexuses, GetSpecs, Nexus, NexusId, NexusShareProtocol, Protocol, RemoveNexusChild,
            ReplicaId, ShareNexus, UnshareNexus,
        },
    },
};

#[tokio::test]
async fn nexus() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        //  .with_agents(vec!["core"])
        .with_io_engines(2)
        .with_pools(2)
        .build()
        .await
        .unwrap();

    let io_engine = cluster.node(0);
    let node_client = cluster.grpc_client().node();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let rep_client = cluster.grpc_client().replica();
    let nexus_client = cluster.grpc_client().nexus();

    let replica = rep_client
        .create(
            &CreateReplica {
                node: cluster.node(1),
                uuid: ReplicaId::new(),
                pool_id: cluster.pool(1, 0),
                pool_uuid: None,
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

    let nexus = nexus_client
        .create(
            &CreateNexus {
                node: io_engine.clone(),
                uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
                size: 5242880,
                children: vec![replica.uri.clone().into(), local],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    let nexuses = nexus_client
        .get(GetNexuses::default().filter, None)
        .await
        .unwrap()
        .0;
    tracing::info!("Nexuses: {:?}", nexuses);
    assert_eq!(Some(&nexus), nexuses.first());

    nexus_client
        .share(
            &ShareNexus {
                node: io_engine.clone(),
                uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
                key: None,
                protocol: NexusShareProtocol::Nvmf,
                allowed_hosts: vec![],
            },
            None,
        )
        .await
        .unwrap();

    nexus_client
        .destroy(
            &DestroyNexus::new(
                io_engine.clone(),
                NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
            ),
            None,
        )
        .await
        .unwrap();

    rep_client
        .destroy(&DestroyReplica::from(replica), None)
        .await
        .unwrap();

    assert!(nexus_client
        .get(GetNexuses::default().filter, None)
        .await
        .unwrap()
        .0
        .is_empty());
}

/// The tests below revolve around transactions and are dependent on the core agent's command line
/// arguments for timeouts.
/// This is required because as of now, we don't have a good mocking strategy

/// default timeout options for every rpc request
fn grpc_timeout_opts() -> TimeoutOptions {
    TimeoutOptions::default()
        .with_max_retries(0)
        .with_req_timeout(Duration::from_millis(250))
        .with_min_req_timeout(None)
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
        // .with_agents(vec!["core"])
        .with_req_timeouts(Duration::from_millis(350), Duration::from_millis(350))
        .with_grpc_timeouts(grpc_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);

    let node_client = cluster.grpc_client().node();
    let registry_client = cluster.grpc_client().registry();
    let nexus_client = cluster.grpc_client().nexus();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let local = "malloc:///local?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into();
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

    let share = ShareNexus::new(&nexus, NexusShareProtocol::Nvmf, vec![]);

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

    nexus_client
        .share(&share, None)
        .await
        .expect_err("io_engine is down");

    check_share_operation(&nexus, Protocol::None, &registry_client).await;

    // unpause io_engine
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    // now it should be shared successfully
    let uri = nexus_client.share(&share, None).await.unwrap();

    println!("Share uri: {uri}");

    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    nexus_client
        .unshare(&UnshareNexus::from(&nexus), None)
        .await
        .expect_err("io_engine down");

    check_share_operation(&nexus, Protocol::Nvmf, &registry_client).await;

    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    nexus_client
        .unshare(&UnshareNexus::from(&nexus), None)
        .await
        .unwrap();

    assert_eq!(
        nexus_spec(&nexus, &registry_client).await.unwrap().share,
        Protocol::None
    );
}

/// Tests Store Write Failures for Nexus Child Operations
/// As it stands, the tests expects the operation to not be undone, and
/// a reconcile thread should eventually sync the specs when the store reappears
async fn nexus_child_op_transaction_store(
    nexus: &Nexus,
    cluster: &Cluster,
    (store_timeout, reconcile_period, grpc_timeout): (Duration, Duration, Duration),
    (share_request, unshare_request, add_nexus_child_request, remove_nexus_child_request): (
        Option<ShareNexus>,
        Option<UnshareNexus>,
        Option<AddNexusChild>,
        Option<RemoveNexusChild>,
    ),
    nexus_client: &dyn NexusOperations,
    (children, share): (usize, Protocol),
) {
    let io_engine = cluster.node(0);

    // pause io_engine
    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    if share_request.is_some() {
        nexus_client
            .share(&share_request.clone().unwrap(), None)
            .await
            .expect_err("io_engine down");
    }
    if unshare_request.is_some() {
        nexus_client
            .unshare(&unshare_request.clone().unwrap(), None)
            .await
            .expect_err("io_engine down");
    }
    if add_nexus_child_request.is_some() {
        nexus_client
            .add_nexus_child(&add_nexus_child_request.clone().unwrap(), None)
            .await
            .expect_err("io_engine down");
    }
    if remove_nexus_child_request.is_some() {
        nexus_client
            .remove_nexus_child(&remove_nexus_child_request.clone().unwrap(), None)
            .await
            .expect_err("io_engine down");
    }

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

    if let Some(request) = share_request {
        nexus_client
            .share(&request, None)
            .await
            .expect_err("operation already performed");
    }
    if let Some(request) = unshare_request {
        nexus_client
            .unshare(&request, None)
            .await
            .expect_err("operation already performed");
    }
    if let Some(request) = add_nexus_child_request {
        nexus_client
            .add_nexus_child(&request, None)
            .await
            .expect_err("operation already performed");
    }
    if let Some(request) = remove_nexus_child_request {
        nexus_client
            .remove_nexus_child(&request, None)
            .await
            .expect_err("operation already performed");
    }
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
        //  .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_grpc_timeouts(grpc_timeout_opts())
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);
    let nexus_client = cluster.grpc_client().nexus();
    let local = "malloc:///local?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into();
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

    // test the share operation
    let share = ShareNexus::new(&nexus, NexusShareProtocol::Nvmf, vec![]);

    nexus_child_op_transaction_store(
        &nexus,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        (Some(share), None, None, None),
        &nexus_client,
        (1, Protocol::Nvmf),
    )
    .await;

    // test the unshare operation
    let unshare = UnshareNexus::from(&nexus);

    nexus_child_op_transaction_store(
        &nexus,
        &cluster,
        (store_timeout, reconcile_period, grpc_timeout),
        (None, Some(unshare), None, None),
        &nexus_client,
        (1, Protocol::None),
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
        //  .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_grpc_timeouts(grpc_timeout_opts())
        .with_reconcile_period(Duration::from_secs(100), Duration::from_secs(100))
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);
    let node_client = cluster.grpc_client().node();
    let registry_client = cluster.grpc_client().registry();
    let nexus_client = cluster.grpc_client().nexus();
    let nodes = node_client.get(Filter::None, false, None).await.unwrap();
    tracing::info!("Nodes: {:?}", nodes);

    let child2 = "malloc:///ch2?size_mb=12&uuid=4a7b0566-8ec6-49e0-a8b2-1d9a292cf59b";
    let nexus = nexus_client
        .create(
            &CreateNexus {
                node: io_engine.clone(),
                uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
                size: 5242880,
                children: vec![
                    "malloc:///ch1?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into(),
                ],
                ..Default::default()
            },
            None,
        )
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

    nexus_client
        .add_nexus_child(&add_child, None)
        .await
        .expect_err("io_engine is down");

    check_child_operation(&nexus, 1, &registry_client).await;

    // unpause io_engine
    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    // now it should be added successfully
    let child = nexus_client
        .add_nexus_child(&add_child, None)
        .await
        .unwrap();
    println!("Child: {child:?}");

    cluster.composer().pause(io_engine.as_str()).await.unwrap();

    nexus_client
        .remove_nexus_child(&rm_child, None)
        .await
        .expect_err("io_engine down");

    check_child_operation(&nexus, 2, &registry_client).await;

    cluster.composer().thaw(io_engine.as_str()).await.unwrap();

    nexus_client
        .remove_nexus_child(&rm_child, None)
        .await
        .unwrap();

    assert_eq!(
        nexus_spec(&nexus, &registry_client)
            .await
            .unwrap()
            .children
            .len(),
        1
    );

    let mut io_engine = cluster.grpc_handle(&cluster.node(0)).await.unwrap();
    io_engine
        .add_child(add_child.nexus.as_str(), add_child.uri.as_str(), true)
        .await
        .unwrap();

    // now it should be added successfully
    let child = nexus_client
        .add_nexus_child(&add_child, None)
        .await
        .unwrap();
    println!("Child: {child:?}");
}

/// Tests child add and remove operations when the store is temporarily down.
#[tokio::test]
async fn nexus_child_transaction_store() {
    let store_timeout = Duration::from_millis(250);
    let reconcile_period = Duration::from_millis(250);
    let grpc_timeout = Duration::from_millis(450);
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_pools(1)
        // .with_agents(vec!["core"])
        .with_req_timeouts(grpc_timeout, grpc_timeout)
        .with_reconcile_period(reconcile_period, reconcile_period)
        .with_store_timeout(store_timeout)
        .with_grpc_timeouts(grpc_timeout_opts())
        .with_options(|b| b.with_io_engine_no_pstor(true))
        .build()
        .await
        .unwrap();
    let io_engine = cluster.node(0);
    let nexus_client = cluster.grpc_client().nexus();

    let nexus = nexus_client
        .create(
            &CreateNexus {
                node: io_engine.clone(),
                uuid: NexusId::try_from("f086f12c-1728-449e-be32-9415051090d6").unwrap(),
                size: 5242880,
                children: vec![
                    "malloc:///ch1?size_mb=12&uuid=281b87d3-0401-459c-a594-60f76d0ce0da".into(),
                ],
                ..Default::default()
            },
            None,
        )
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
        (None, None, Some(add_child), None),
        &nexus_client,
        (2, Protocol::None),
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
        (None, None, None, Some(del_child)),
        &nexus_client,
        (1, Protocol::None),
    )
    .await;
}
