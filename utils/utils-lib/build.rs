type BuildResult = Result<(), Box<dyn std::error::Error>>;

fn main() -> BuildResult {
    let output = std::process::Command::new("bash")
        .args(["-c", "../../scripts/rust/branch_ancestor.sh"])
        .output()
        .expect("failed to execute bash command");

    assert!(
        output.status.success(),
        "script failed: {:?}/{:?}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    fn add_git_head(root: &str) {
        let git_path = std::path::Path::new(root).join(".git/HEAD");
        if git_path.exists() {
            println!("cargo:rerun-if-changed={}", git_path.display());
        }
    }

    println!("cargo:rerun-if-env-changed=TARGET_REGISTRY");

    add_git_head("../..");
    if let Ok(ext) = std::env::var("EXTENSIONS_SRC") {
        add_git_head(&ext);
    }

    Ok(())
}
