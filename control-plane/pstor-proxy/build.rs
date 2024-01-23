fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/v1/frontend_node/registration.proto",
                "proto/v1/frontend_node/frontend_node.proto",
                "proto/v1/misc/common.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
