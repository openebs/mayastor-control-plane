extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .compile(
            &["proto/v1/pool/pool.proto", "proto/v1/misc/common.proto"],
            &["proto/"],
        )
        .unwrap();
}
