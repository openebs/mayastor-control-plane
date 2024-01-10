use std::{
    path::{Path, PathBuf},
    process::Command,
};

extern crate tonic_build;

trait PrefixPath {
    fn prefixed(&self, prefix: impl AsRef<Path>) -> Vec<PathBuf>;
}
impl PrefixPath for [&str] {
    fn prefixed(&self, prefix: impl AsRef<Path>) -> Vec<PathBuf> {
        let prefix = prefix.as_ref();
        self.iter().map(|s| prefix.join(s)).collect()
    }
}

fn main() {
    if !Path::new("../utils/dependencies/.git").exists() {
        let output = Command::new("git")
            .args(["submodule", "update", "--init"])
            .output()
            .expect("failed to execute git command ");

        if !output.status.success() {
            panic!("API repository checkout failed");
        }
    }
    let io_api = "../utils/dependencies/apis/io-engine/";
    let csi_api = "../utils/dependencies/apis/csi/";

    tonic_build::configure()
        .build_server(false)
        .compile(
            &["protobuf/mayastor.proto"].prefixed(io_api),
            &["protobuf"].prefixed(io_api),
        )
        .unwrap_or_else(|e| panic!("io-engine v0 protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .compile(
            &["protobuf/csi.proto"].prefixed(csi_api),
            &["protobuf"].prefixed(csi_api),
        )
        .unwrap_or_else(|e| panic!("CSI protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "protobuf/v1/host.proto",
                "protobuf/v1/replica.proto",
                "protobuf/v1/registration.proto",
                "protobuf/v1/nexus.proto",
                "protobuf/v1/pool.proto",
                "protobuf/v1/json.proto",
                "protobuf/v1/snapshot.proto",
                "protobuf/v1/stats.proto",
            ]
            .prefixed(io_api),
            &["protobuf/v1"].prefixed(io_api),
        )
        .unwrap_or_else(|e| panic!("io-engine v1 protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .compile(
            &["protobuf/v1-alpha/registration.proto"].prefixed(io_api),
            &["protobuf/v1-alpha"].prefixed(io_api),
        )
        .unwrap_or_else(|e| panic!("v1-alpha registration protobuf compilation failed: {}", e));
}
