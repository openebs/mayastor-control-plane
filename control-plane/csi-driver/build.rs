fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["node/proto/node-service.proto"], &["node/proto"])
        .expect("node grpc service protobuf compilation failed");
}
