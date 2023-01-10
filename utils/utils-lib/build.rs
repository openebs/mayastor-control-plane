type BuildResult = Result<(), Box<dyn std::error::Error>>;

fn main() -> BuildResult {
    let output = std::process::Command::new("bash")
        .args(&["-c", "../../scripts/rust/branch_ancestor.sh"])
        .output()
        .expect("failed to execute bash command");

    assert!(
        output.status.success(),
        "script failed: {:?}/{:?}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let git_path = std::path::Path::new("../../.git");
    if git_path.exists() {
        println!("cargo:rerun-if-changed=../../.git/HEAD");
    }

    Ok(())
}
