use deployer_cluster::ClusterBuilder;
use rpc::v1::registration::{registration_client::RegistrationClient, RegisterRequest};

/// When gRPC TLS is enforced on the Core server an "older" io-engine that speaks plaintext must be
/// rejected, while a TLS-speaking io-engine still registers successfully on the very same port.
#[tokio::test]
async fn grpc_tls_enforced_rejects_plaintext_registration() {
    // Start without io-engines: a real io-engine registers over plaintext, which enforced TLS
    // rejects, so it would never come online. Registration is driven directly below instead.
    let cluster = ClusterBuilder::builder()
        .with_rest(false)
        .with_io_engines(0)
        .with_grpc_tls(true)
        .with_grpc_tls_enforced(true)
        .build()
        .await
        .unwrap();

    let core_ip = cluster.composer().container_ip("core");
    let register_request = |id: &str| RegisterRequest {
        id: id.to_string(),
        grpc_endpoint: "10.1.0.99:10124".to_string(),
        api_version: vec![1],
        ..Default::default()
    };

    // An older io-engine registers over plaintext; enforced TLS must reject the connection.
    // The TCP connect succeeds (the server accepts every socket) but the server then attempts a
    // TLS handshake on the plaintext HTTP/2 preface, fails, and drops the connection, so the
    // client sees a prompt transport error rather than a timeout.
    let plaintext_endpoint =
        tonic::transport::Endpoint::try_from(format!("http://{core_ip}:50051")).unwrap();
    let mut plaintext = RegistrationClient::new(plaintext_endpoint.connect_lazy());
    plaintext
        .register(register_request("io-engine-plaintext"))
        .await
        .expect_err("plaintext registration must be rejected when gRPC TLS is enforced");

    // A TLS-speaking io-engine registers successfully on the same port. The auto-tls connector
    // performs the TLS handshake itself, so the endpoint uses an http scheme to stop tonic from
    // applying (and rejecting) its own TLS logic.
    let tls_endpoint =
        tonic::transport::Endpoint::try_from(format!("http://{core_ip}:50051")).unwrap();
    let tls_channel = grpc::tls::auto_tls_connect(&tls_endpoint).await.unwrap();
    let mut tls = RegistrationClient::new(tls_channel);
    tls.register(register_request("io-engine-tls"))
        .await
        .expect("TLS registration must succeed when gRPC TLS is enforced");
}
