//! Settings for the deployer's S3-compatible object store.
//!
//! Disaster recovery mediates every relationship through an object store, and local
//! testing needs one that comes up and goes away with the cluster. That store is
//! [`crate::infra::Rustfs`], started by `deployer start --rustfs`.
//!
//! The bucket, credentials, region and published host port live here rather than in the
//! component, because `RC_HOST_local` in `nix/shells/test.nix` names the same values — that is what
//! makes `rc` work against a freshly started store with no flags. Keeping them in one place
//! is also what lets the store be swapped without touching anything that talks to it.

use crate::StartOptions;

/// Name of the bucket created on every object store startup, and granted to [`S3_ACCESS_KEY_ID`].
pub(crate) const S3_BUCKET: &str = "default";

/// Fixed S3 access key ID granted access to [`S3_BUCKET`] on every startup.
/// Kept in sync with `RC_HOST_local` in `nix/shells/test.nix`.
pub(crate) const S3_ACCESS_KEY_ID: &str = "GKde9ade9ade9ade9ade9ade9a";

/// Fixed S3 secret access key for [`S3_ACCESS_KEY_ID`] (see there).
pub(crate) const S3_SECRET_ACCESS_KEY: &str =
    "a9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9eda9ed";

/// Region the S3 API is configured with, required by clients to compute a valid request signature.
/// RustFS does not validate it, but a client must still send one.
/// `rc` takes the region from `RC_HOST_<alias>` rather than an argument, so nothing here reads it.
/// It is kept as the record of what a client must scope its signature to, for anything talking to
/// the store that is not `rc`.
#[allow(dead_code)]
pub(crate) const S3_REGION: &str = "local-deployer";

/// Host port the store's S3 API is published on. Named here rather than in the component
/// so that it and `RC_HOST_local` in `nix/shells/test.nix` have one place to agree with.
pub(crate) const S3_HOST_PORT: &str = "3900";

/// Alias `rc` is given for the store. Supplied through `RC_HOST_<alias>` in the
/// environment rather than written into the user's own `~/.config/rc/config.toml`, both
/// here and in `nix/shells/test.nix`.
pub(crate) const RC_ALIAS: &str = "local";

/// The `RC_HOST_<alias>` value naming the store, its credentials inline.
///
/// Safe to build by formatting because the key and secret are fixed above and contain no
/// characters that would need escaping in a URL.
pub(crate) fn rc_host() -> String {
    format!("http://{S3_ACCESS_KEY_ID}:{S3_SECRET_ACCESS_KEY}@localhost:{S3_HOST_PORT}")
}

/// A `<alias>/<bucket>[/key]` path for `rc`.
pub(crate) fn rc_path(suffix: &str) -> String {
    format!("{RC_ALIAS}/{S3_BUCKET}{suffix}")
}

/// Endpoint of the object store, as reachable *from another container* on the same compose
/// network, or [`None`] if no store was enabled.
// Consumed by the dr agent, which is not wired up yet.
#[allow(dead_code)]
pub(crate) fn s3_endpoint(options: &StartOptions) -> Option<String> {
    options.rustfs.then(|| {
        format!(
            "http://{}.{}:{}",
            super::rustfs::CONTAINER,
            options.cluster_label.name(),
            super::rustfs::S3_PORT
        )
    })
}
