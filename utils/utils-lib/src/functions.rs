/// Return the git version.
pub fn git_version() -> &'static str {
    git_version::git_version!(args = ["--abbrev=12", "--always"])
}
