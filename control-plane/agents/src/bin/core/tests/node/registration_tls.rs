use deployer_cluster::{Cluster, ClusterBuilder};
use grpc::operations::{node::traits::NodeOperations, pool::traits::PoolOperations};
use rpc::v1::host::host_rpc_client::HostRpcClient;
use std::time::Duration;
use stor_port::types::v0::transport::{CreatePool, Filter, Node, NodeStatus};

/// A plaintext gRPC client for an io-engine, as an older control-plane would use.
fn plaintext_io_engine_client(ip: &str) -> HostRpcClient<tonic::transport::Channel> {
    let endpoint = tonic::transport::Endpoint::try_from(format!("http://{ip}:10124"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2));
    HostRpcClient::new(endpoint.connect_lazy())
}

/// Assert the single io-engine registered and is online (io-engine -> core), then exercise the
/// Core -> io-engine direction by creating a pool on its gRPC server, and return the online node.
///
/// This is the shared proof that both directions of the encrypted transport work, regardless of
/// whether it was negotiated via auto-TLS or file-backed mutual TLS.
async fn assert_registered_and_manageable(cluster: &Cluster) -> Node {
    let node_id = cluster.node(0);
    let nodes = cluster
        .grpc_client()
        .node()
        .get(Filter::Node(node_id.clone()), false, None)
        .await
        .unwrap();
    let node = nodes.0.first().expect("the io-engine must have registered");
    assert_eq!(node.state().map(|s| &s.status), Some(&NodeStatus::Online));

    cluster
        .grpc_client()
        .pool()
        .create(
            &CreatePool {
                node: node_id,
                id: cluster.pool(0, 0),
                disks: vec!["malloc:///disk?size_mb=100".into()],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("the core must reach the io-engine over the encrypted transport");

    node.clone()
}

/// During a rolling TLS rollout the fleet is briefly mixed: some io-engines still speak plaintext
/// (the old release) and some already speak TLS (the new release). With gRPC TLS enabled but NOT
/// enforced on the Core, both must be accepted and manageable - the Core talks plaintext to the
/// old io-engine and TLS to the new one - which is the happy path for the rollout.
#[tokio::test]
async fn grpc_tls_io_engine_registration() {
    // The cluster build waits for the (plaintext) io-engine node to be online, which requires both
    // directions to work: registration (io-engine -> core) and the node probe (core -> io-engine).
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_io_engines(1)
        // TLS is enabled on the Core but not enforced, so plaintext io-engines are still accepted.
        .with_grpc_tls(true)
        .with_options(|o| {
            // The regular io-engine is the old, plaintext release; the idle io-engine is the new,
            // TLS release that later takes over the node, leaving the Core to manage a mix.
            o.with_io_engine_no_grpc_tls(true)
                .with_idle_io_engines(1)
                // A short node deadline and frequent heartbeats so the node's offline/online
                // transitions during the swap below are detected quickly.
                .with_node_deadline("1s")
                .with_io_engine_env("MAYASTOR_HB_INTERVAL", "50ms")
        })
        .build()
        .await
        .expect("the plaintext io-engine must register and come online when TLS is not enforced");

    // The active io-engine speaks plaintext: it registers, comes online (so the Core reaches it,
    // core -> io-engine), and does not advertise gRPC TLS.
    let nodes = cluster
        .grpc_client()
        .node()
        .get(Filter::Node(cluster.node(0)), false, None)
        .await
        .unwrap();
    let node = nodes
        .0
        .first()
        .expect("the plaintext io-engine must have registered");
    assert_eq!(node.state().map(|s| &s.status), Some(&NodeStatus::Online));
    assert_ne!(
        node.spec()
            .and_then(|spec| spec.features().as_ref())
            .and_then(|features| features.grpc_tls),
        Some(true),
        "the plaintext io-engine must not advertise gRPC TLS"
    );
    // It serves plaintext clients, as an older control-plane would use.
    plaintext_io_engine_client(&cluster.node_ip(0))
        .get_mayastor_info(())
        .await
        .expect("the plaintext io-engine must serve plaintext clients");

    // Roll out TLS: swap the plaintext io-engine for the TLS one, which registers under the same
    // node name. The Core now manages it over TLS (both directions encrypted).
    cluster
        .replace_node(cluster.node(0), cluster.node(1))
        .await
        .expect("the TLS io-engine must take over the node");
    cluster
        .wait_node_status_tmo(cluster.node(0), NodeStatus::Online, Duration::from_secs(2))
        .await
        .expect("the TLS io-engine must come online over TLS");

    let node = assert_registered_and_manageable(&cluster).await;
    assert_eq!(
        node.spec()
            .and_then(|spec| spec.features().as_ref())
            .and_then(|features| features.grpc_tls),
        Some(true),
        "the io-engine must advertise gRPC TLS"
    );

    // The TLS io-engine serves TLS exclusively: a plaintext client (an older control-plane) is
    // rejected on the very same port a TLS client is served on. The TLS engine runs in the idle
    // container (node index 1), though the Core knows it under the original node id (index 0).
    plaintext_io_engine_client(&cluster.node_ip(1))
        .get_mayastor_info(())
        .await
        .expect_err("the io-engine must reject plaintext clients when serving TLS");
    let mut tls = cluster.grpc_handle(cluster.node(1).as_str()).await.unwrap();
    tls.ping()
        .await
        .expect("the io-engine must serve TLS clients");
}

/// When gRPC TLS is enforced on the Core, a real "older" io-engine which speaks plaintext must be
/// rejected: its registration never succeeds and so the Core never knows about it.
#[tokio::test]
async fn grpc_tls_enforced_rejects_plaintext_io_engine() {
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        // The plaintext io-engine would never come online, so the deployer must not wait for it:
        // configure it as an idle io-engine and start it by hand below.
        .with_io_engines(0)
        .with_grpc_tls(true)
        .with_grpc_tls_enforced(true)
        .with_options(|o| {
            o.with_idle_io_engines(1)
                // An io-engine which does not speak TLS, like an older release.
                .with_io_engine_env("GRPC_AUTO_TLS", "false")
                // Register frequently so the rejection is attempted many times below.
                .with_io_engine_env("MAYASTOR_HB_INTERVAL", "50ms")
        })
        .build()
        .await
        .unwrap();

    let node_id = cluster.node(0);
    cluster.composer().start(node_id.as_str()).await.unwrap();

    // Wait for the io-engine to be serving, over plaintext, as it has TLS disabled.
    let ip = cluster.composer().container_ip(node_id.as_str());
    let mut plaintext = plaintext_io_engine_client(&ip);
    let start = std::time::Instant::now();
    while plaintext.get_mayastor_info(()).await.is_err() {
        assert!(
            start.elapsed() <= Duration::from_secs(2),
            "the plaintext io-engine must come up"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Give the io-engine time for several registration attempts, every one of which the Core
    // must reject: the TCP connect succeeds (the Core accepts every socket) but the TLS handshake
    // on the plaintext HTTP/2 preface fails and the connection is dropped.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let nodes = cluster
        .grpc_client()
        .node()
        .get(Filter::Node(node_id.clone()), false, None)
        .await
        .map(|nodes| nodes.0)
        .unwrap_or_default();
    assert!(
        nodes.is_empty(),
        "a plaintext io-engine must not register when gRPC TLS is enforced: {nodes:?}"
    );
}

/// With file-backed mutual TLS on both sides, each peer must authenticate to the other:
/// - the io-engine registers with the Core (io-engine -> core), presenting its certificate to the
///   Core server, which verifies it against the CA;
/// - the Core dials the io-engine (core -> io-engine), presenting its certificate to the io-engine
///   server, which verifies it against the CA.
/// Only when both directions succeed does the node come online and can the Core manage it.
#[tokio::test]
async fn grpc_mtls_registration() {
    // The cluster build waits for the io-engine node to be online, which requires both
    // directions to work: registration (io-engine -> core) and the node probe (core -> io-engine).
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_io_engines(1)
        .with_grpc_mtls(true)
        .with_grpc_tls_enforced(true)
        .build()
        .await
        .expect("the io-engine must register and come online over mTLS");

    assert_registered_and_manageable(&cluster).await;
}
