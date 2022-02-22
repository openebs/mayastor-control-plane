use std::{path::Path, process::Command};

extern crate tonic_build;

fn main() {
    if !Path::new("mayastor-api/.git").exists() {
        let output = Command::new("git")
            .args(&["submodule", "update", "--init"])
            .output()
            .expect("failed to execute git command ");

        if !output.status.success() {
            panic!("submodule checkout failed");
        }
    }

    tonic_build::configure()
        .build_server(false)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &["mayastor-api/protobuf/mayastor.proto"],
            &["mayastor-api/protobuf"],
        )
        .unwrap_or_else(|e| panic!("mayastor protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .compile(
            &["mayastor-api/protobuf/csi.proto"],
            &["mayastor-api/protobuf"],
        )
        .unwrap_or_else(|e| panic!("CSI protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .compile(
            &["mayastor-api/protobuf/v1/registration.proto"],
            &["mayastor-api/protobuf/v1"],
        )
        .unwrap_or_else(|e| panic!("Registration protobuf compilation failed: {}", e));
}
