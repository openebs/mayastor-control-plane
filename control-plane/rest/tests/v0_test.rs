use common_lib::{
    mbus_api::{Message, TimeoutOptions},
    types::v0::message_bus::{ChannelVs, Liveness, NodeId, WatchResourceId},
};
use composer::{Binary, Builder, ComposeTest, ContainerSpec};
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use rest_client::{versions::v0::*, ActixRestClient};
use rpc::mayastor::Null;
use std::{
    io,
    net::{SocketAddr, TcpStream},
    str::FromStr,
    time::Duration,
};
use tracing::info;

async fn wait_for_services() {
    Liveness {}.request_on(ChannelVs::Node).await.unwrap();
    Liveness {}.request_on(ChannelVs::Pool).await.unwrap();
    Liveness {}.request_on(ChannelVs::Nexus).await.unwrap();
    Liveness {}.request_on(ChannelVs::Volume).await.unwrap();
    Liveness {}.request_on(ChannelVs::JsonGrpc).await.unwrap();
}

// Returns the path to the JWK file.
fn jwk_file() -> String {
    let jwk_file = std::env::current_dir()
        .unwrap()
        .join("authentication")
        .join("jwk");
    jwk_file.to_str().unwrap().into()
}

// Setup the infrastructure ready for the tests.
async fn test_setup(auth: &bool) -> ((String, String), ComposeTest) {
    let jwk_file = jwk_file();
    let mut rest_args = match auth {
        true => vec!["--jwk", &jwk_file],
        false => vec!["--no-auth"],
    };
    rest_args.append(&mut vec!["-j", "10.1.0.7:6831", "--dummy-certificates"]);

    let mayastor1 = "node-test-name-1";
    let mayastor2 = "node-test-name-2";
    // todo: this is getting unwieldy... we should make use of the deployer
    let test = Builder::new()
        .name("rest")
        .add_container_spec(
            ContainerSpec::from_binary("nats", Binary::from_path("nats-server").with_arg("-DV"))
                .with_portmap("4222", "4222"),
        )
        .add_container_bin(
            "core",
            Binary::from_dbg("core")
                .with_nats("-n")
                .with_args(vec!["--store", "http://etcd.rest:2379"])
                .with_args(vec!["-j", "10.1.0.7:6831"]),
        )
        .add_container_spec(
            ContainerSpec::from_binary(
                "rest",
                Binary::from_dbg("rest")
                    .with_nats("-n")
                    .with_args(rest_args),
            )
            .with_portmap("8080", "8080")
            .with_portmap("8081", "8081"),
        )
        .add_container_bin(
            "mayastor-1",
            Binary::from_path("mayastor")
                .with_nats("-n")
                .with_args(vec!["-N", mayastor1])
                .with_args(vec!["-g", "10.1.0.5:10124"]),
        )
        .add_container_bin(
            "mayastor-2",
            Binary::from_path("mayastor")
                .with_nats("-n")
                .with_args(vec!["-N", mayastor2])
                .with_args(vec!["-g", "10.1.0.6:10124"]),
        )
        .add_container_spec(
            ContainerSpec::from_image("jaeger", "jaegertracing/all-in-one:latest")
                .with_portmap("16686", "16686")
                .with_portmap("6831/udp", "6831/udp"),
        )
        .add_container_bin("jsongrpc", Binary::from_dbg("jsongrpc").with_nats("-n"))
        .add_container_spec(
            ContainerSpec::from_binary(
                "etcd",
                Binary::from_path("etcd").with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .with_default_tracing()
        .autorun(false)
        .build()
        .await
        .unwrap();
    ((mayastor1.into(), mayastor2.into()), test)
}

/// Wait to establish a connection to etcd.
/// Returns 'Ok' if connected otherwise 'Err' is returned.
fn wait_for_etcd_ready(endpoint: &str) -> io::Result<TcpStream> {
    let sa = SocketAddr::from_str(endpoint).unwrap();
    TcpStream::connect_timeout(&sa, Duration::from_secs(3))
}

/// connect to message bus helper for the cargo test code
async fn connect_to_bus(test: &ComposeTest, name: &str) {
    let timeout = TimeoutOptions::new()
        .with_timeout(Duration::from_millis(500))
        .with_timeout_backoff(Duration::from_millis(500))
        .with_max_retries(10);
    connect_to_bus_timeout(test, name, timeout).await;
}

/// connect to message bus helper for the cargo test code with bus timeouts
async fn connect_to_bus_timeout(test: &ComposeTest, name: &str, bus_timeout: TimeoutOptions) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        mbus_api::message_bus_init_options(test.container_ip(name), bus_timeout).await
    })
    .await
    .unwrap();
}

// to avoid waiting for timeouts
async fn orderly_start(test: &ComposeTest) {
    test.start_containers(vec!["nats", "jsongrpc", "rest", "jaeger", "etcd"])
        .await
        .unwrap();
    assert!(
        wait_for_etcd_ready("0.0.0.0:2379").is_ok(),
        "etcd not ready"
    );

    connect_to_bus(test, "nats").await;
    test.start("core").await.unwrap();
    wait_for_services().await;

    test.start("mayastor-1").await.unwrap();
    test.start("mayastor-2").await.unwrap();

    let mut hdl = test.grpc_handle("mayastor-1").await.unwrap();
    hdl.mayastor.list_nexus(Null {}).await.unwrap();
    let mut hdl = test.grpc_handle("mayastor-2").await.unwrap();
    hdl.mayastor.list_nexus(Null {}).await.unwrap();
}

// Return a bearer token to be sent with REST requests.
fn bearer_token() -> String {
    let token_file = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("authentication")
        .join("token");
    std::fs::read_to_string(token_file).expect("Failed to get bearer token")
}

#[actix_rt::test]
async fn client() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let _tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("rest-client")
        .install_simple()
        .unwrap();
    // Run the client test both with and without authentication.
    for auth in &[true, false] {
        let (mayastor, test) = test_setup(auth).await;
        client_test(&mayastor.0.into(), &mayastor.1.into(), &test, auth).await;
    }
}

async fn client_test(mayastor1: &NodeId, mayastor2: &NodeId, test: &ComposeTest, auth: &bool) {
    orderly_start(test).await;

    let client = ActixRestClient::new(
        "https://localhost:8080",
        true,
        match auth {
            true => Some(bearer_token()),
            false => None,
        },
    )
    .unwrap()
    .v00();

    let nodes = client.nodes_api().get_nodes().await.unwrap();
    info!("Nodes: {:#?}", nodes);
    assert_eq!(nodes.len(), 2);

    let listed_node = client.nodes_api().get_node(mayastor1.as_str()).await;
    let mut node = models::Node {
        id: mayastor1.to_string(),
        spec: Some(models::NodeSpec {
            id: mayastor1.to_string(),
            grpc_endpoint: "10.1.0.5:10124".to_string(),
        }),
        state: Some(models::NodeState {
            id: mayastor1.to_string(),
            grpc_endpoint: "10.1.0.5:10124".to_string(),
            status: models::NodeStatus::Online,
        }),
    };
    assert_eq!(listed_node.unwrap(), node);

    let _ = client.pools_api().get_pools().await.unwrap();
    let pool = client
        .pools_api()
        .put_node_pool(
            mayastor1.as_str(),
            "pooloop",
            models::CreatePoolBody::new(vec![
                "malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0",
            ]),
        )
        .await
        .unwrap();

    info!("Pools: {:#?}", pool);
    assert_eq!(
        pool,
        models::Pool::new_all(
            "pooloop",
            models::PoolSpec::new(vec!["malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0"], "pooloop", Vec::<String>::new(), mayastor1, models::SpecStatus::Created),
            models::PoolState::new(100663296u64, vec!["malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0"], "pooloop", mayastor1, models::PoolStatus::Online, 0u64)
        )
    );

    assert_eq!(
        Some(&pool),
        client.pools_api().get_pools().await.unwrap().first()
    );

    let pool = client
        .pools_api()
        .put_node_pool(
            mayastor2.as_str(),
            "pooloop2",
            models::CreatePoolBody::new(vec![
                "malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b1",
            ]),
        )
        .await
        .unwrap();

    info!("Pools: {:#?}", pool);

    let _ = client.replicas_api().get_replicas().await.unwrap();
    let replica = client
        .replicas_api()
        .put_node_pool_replica(
            &pool.spec.as_ref().unwrap().node,
            &pool.id,
            "e6e7d39d-e343-42f7-936a-1ab05f1839db",
            /* actual size will be a multiple of 4MB so just
             * create it like so */
            models::CreateReplicaBody::new(models::Protocol::Nvmf, 12582912u64, true),
        )
        .await
        .unwrap();
    info!("Replica: {:#?}", replica);

    let uri = replica.uri.clone();
    assert_eq!(
        replica,
        models::Replica {
            node: pool.spec.clone().unwrap().node,
            uuid: FromStr::from_str("e6e7d39d-e343-42f7-936a-1ab05f1839db").unwrap(),
            pool: pool.id.clone(),
            thin: false,
            size: 12582912,
            share: models::Protocol::Nvmf,
            uri,
            state: models::ReplicaState::Online
        }
    );
    assert_eq!(
        Some(&replica),
        client.replicas_api().get_replicas().await.unwrap().first()
    );
    client
        .replicas_api()
        .del_node_pool_replica(&replica.node, &replica.pool, &replica.uuid.to_string())
        .await
        .unwrap();

    let replicas = client.replicas_api().get_replicas().await.unwrap();
    assert!(replicas.is_empty());

    let nexuses = client.nexuses_api().get_nexuses().await.unwrap();
    assert_eq!(nexuses.len(), 0);
    let nexus = client
        .nexuses_api()
        .put_node_nexus(
            mayastor1.as_str(),
            "058a95e5-cee6-4e81-b682-fe864ca99b9c",
            models::CreateNexusBody::new(
                vec!["malloc:///malloc1?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0"],
                12582912u64
            ),
        )
        .await
        .unwrap();
    info!("Nexus: {:#?}", nexus);

    assert_eq!(
        nexus,
        models::Nexus {
            node: mayastor1.to_string(),
            uuid: FromStr::from_str("058a95e5-cee6-4e81-b682-fe864ca99b9c").unwrap(),
            size: 12582912,
            state: models::NexusState::Online,
            children: vec![models::Child {
                uri: "malloc:///malloc1?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".into(),
                state: models::ChildState::Online,
                rebuild_progress: None
            }],
            device_uri: "".to_string(),
            rebuilds: 0,
            share: models::Protocol::None
        }
    );

    let mut child = client
        .children_api()
        .put_node_nexus_child(
            &nexus.node,
            &nexus.uuid.to_string(),
            "malloc:///malloc2?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b1",
        )
        .await
        .unwrap();

    let children = client
        .children_api()
        .get_nexus_children(&nexus.uuid.to_string())
        .await
        .unwrap();

    // It's possible that the rebuild progress will change between putting a child and getting the
    // list of children. Just check that they are both rebuilding and then set them to the same
    // thing so that we can compare them in subsequent asserts.
    assert!(child.rebuild_progress.is_some());
    assert!(children.last().unwrap().rebuild_progress.is_some());
    child.rebuild_progress = children.last().unwrap().rebuild_progress;
    assert_eq!(Some(&child), children.last());

    client
        .nexuses_api()
        .del_node_nexus(&nexus.node, &nexus.uuid.to_string())
        .await
        .unwrap();
    let nexuses = client.nexuses_api().get_nexuses().await.unwrap();
    assert!(nexuses.is_empty());

    let volume = client
        .volumes_api()
        .put_volume(
            "058a95e5-cee6-4e81-b682-fe864ca99b9c",
            models::CreateVolumeBody::new(
                models::VolumeHealPolicy::default(),
                1,
                12582912u64,
                models::Topology::default(),
            ),
        )
        .await
        .unwrap();

    tracing::info!("Volume: {:#?}", volume);
    assert_eq!(
        volume,
        client
            .volumes_api()
            .get_volume("058a95e5-cee6-4e81-b682-fe864ca99b9c")
            .await
            .unwrap()
    );

    let volume = client
        .volumes_api()
        .put_volume_target(
            &volume.state.unwrap().uuid.to_string(),
            mayastor1.as_str(),
            models::VolumeShareProtocol::Nvmf,
        )
        .await
        .unwrap();
    let volume_state = volume.state.expect("Volume state not found.");
    let nexus = volume_state.children.first().unwrap();
    tracing::info!("Published on '{}'", nexus.node);

    let volume = client
        .volumes_api()
        .put_volume_replica_count(&volume_state.uuid.to_string(), 2)
        .await
        .expect("We have 2 nodes with a pool each");
    tracing::info!("Volume: {:#?}", volume);
    let volume_state = volume.state.expect("No volume state");
    let nexus = volume_state.children.first().unwrap();
    assert_eq!(nexus.children.len(), 2);

    let volume = client
        .volumes_api()
        .put_volume_replica_count(&volume_state.uuid.to_string(), 1)
        .await
        .expect("Should be able to reduce back to 1");
    tracing::info!("Volume: {:#?}", volume);
    let volume_state = volume.state.expect("No volume state");
    let nexus = volume_state.children.first().unwrap();
    assert_eq!(nexus.children.len(), 1);

    let volume = client
        .volumes_api()
        .del_volume_target(&volume_state.uuid.to_string())
        .await
        .unwrap();
    tracing::info!("Volume: {:#?}", volume);
    let volume_state = volume.state.expect("No volume state");
    assert!(volume_state.children.is_empty());

    let volume_uuid = volume_state.uuid.to_string();

    let _watch_volume = WatchResourceId::Volume(volume_uuid.clone().into());
    let callback = url::Url::parse("http://lala/test").unwrap();

    let watchers = client
        .watches_api()
        .get_watch_volume(&volume_uuid)
        .await
        .unwrap();
    assert!(watchers.is_empty());

    client
        .watches_api()
        .put_watch_volume(&volume_uuid, &callback.to_string())
        .await
        .expect_err("volume does not exist in the store");

    client
        .watches_api()
        .del_watch_volume(&volume_uuid, &callback.to_string())
        .await
        .expect_err("Does not exist");

    let watchers = client
        .watches_api()
        .get_watch_volume(&volume_uuid)
        .await
        .unwrap();
    assert!(watchers.is_empty());

    client
        .volumes_api()
        .del_volume("058a95e5-cee6-4e81-b682-fe864ca99b9c")
        .await
        .unwrap();

    let volumes = client.volumes_api().get_volumes().await.unwrap();
    assert!(volumes.is_empty());

    client
        .pools_api()
        .del_node_pool(&pool.spec.as_ref().unwrap().node, &pool.id)
        .await
        .unwrap();
    let pools = client
        .pools_api()
        .get_node_pools(&pool.spec.as_ref().unwrap().node)
        .await
        .unwrap();
    assert!(pools.is_empty());

    client
        .json_grpc_api()
        .put_node_jsongrpc(mayastor1.as_str(), "rpc_get_methods", serde_json::json!({}))
        .await
        .expect("Failed to call JSON gRPC method");

    client
        .block_devices_api()
        .get_node_block_devices(mayastor1.as_str(), Some(true))
        .await
        .expect("Failed to get block devices");

    test.stop("mayastor-1").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    node.state.as_mut().unwrap().status = models::NodeStatus::Unknown;
    assert_eq!(
        client
            .nodes_api()
            .get_node(mayastor1.as_str())
            .await
            .unwrap(),
        node
    );
}

#[actix_rt::test]
async fn client_invalid_token() {
    let (_, test) = test_setup(&true).await;
    orderly_start(&test).await;

    // Use an invalid token to make requests.
    let mut token = bearer_token();
    token.push_str("invalid");

    let client = ActixRestClient::new("https://localhost:8080", true, Some(token))
        .unwrap()
        .v00();

    let error = client
        .nodes_api()
        .get_nodes()
        .await
        .expect_err("Request should fail with invalid token");

    assert!(matches!(
        error,
        apis::client::Error::ResponseError(apis::client::ResponseContent {
            status: apis::StatusCode::UNAUTHORIZED,
            ..
        })
    ));
}
