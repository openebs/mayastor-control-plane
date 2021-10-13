use std::process::Command;

fn main() {
    let output = Command::new("bash")
        .args(&[
            "-c",
            "../scripts/generate-openapi-bindings.sh --if-rev-changed",
        ])
        .output()
        .expect("failed to execute bash command");

    if !output.status.success() {
        panic!("openapi update failed: {:?}", output);
    }

    println!("cargo:rerun-if-changed=../nix/pkgs/openapi-generator");
}
