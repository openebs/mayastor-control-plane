use std::path::Path;
use std::process::Command;

fn main() {
    if !Path::new("src/lib.rs").exists() {
        let output = Command::new("sh")
            .args(&["../scripts/generate-openapi-bindings.sh"])
            .output()
            .expect("failed to execute bash command");

        if !output.status.success() {
            panic!("openapi update failed: {:?}", output);
        }
    }
}
