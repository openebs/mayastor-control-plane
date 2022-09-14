use std::{path::Path, process::Command};

extern crate tonic_build;

fn main() {
    if !Path::new("api/.git").exists() {
        let output = Command::new("git")
            .args(&["submodule", "update", "--init"])
            .output()
            .expect("failed to execute git command ");

        if !output.status.success() {
            panic!("API repository checkout failed");
        }
    }

    tonic_build::configure()
        .build_server(false)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(&["api/protobuf/mayastor.proto"], &["api/protobuf"])
        .unwrap_or_else(|e| panic!("io-engine protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .compile(&["api/protobuf/csi.proto"], &["api/protobuf"])
        .unwrap_or_else(|e| panic!("CSI protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &[
                "api/protobuf/v1/host.proto",
                "api/protobuf/v1/replica.proto",
                "api/protobuf/v1/registration.proto",
                "api/protobuf/v1/nexus.proto",
                "api/protobuf/v1/pool.proto",
                "api/protobuf/v1/json.proto",
            ],
            &["api/protobuf/v1"],
        )
        .unwrap_or_else(|e| panic!("mayastor v1 protobuf compilation failed: {}", e));

    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize, Eq)]")
        .compile(
            &["api/protobuf/v1-alpha/registration.proto"],
            &["api/protobuf/v1-alpha"],
        )
        .unwrap_or_else(|e| panic!("v1-alpha registration protobuf compilation failed: {}", e));
}
