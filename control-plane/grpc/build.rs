extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/v1/pool/pool.proto",
                "proto/v1/replica/replica.proto",
                "proto/v1/misc/common.proto",
                "proto/v1/nexus/nexus.proto",
                "proto/v1/volume/volume.proto",
                "proto/v1/node/node.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
