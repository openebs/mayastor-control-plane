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
                "proto/v1/blockdevice/blockdevice.proto",
                "proto/v1/registry/registry.proto",
                "proto/v1/jsongrpc/jsongrpc.proto",
                "proto/v1/watch/watch.proto",
                "proto/v1/ha/cluster_agent.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
