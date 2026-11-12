use crate::infra::{
    async_trait,
    s3::{
        rc_host, rc_path, RC_ALIAS, S3_ACCESS_KEY_ID, S3_BUCKET, S3_HOST_PORT, S3_SECRET_ACCESS_KEY,
    },
    Builder, ComponentAction, Components, Error, Rustfs, StartOptions,
};

/// Image for this single-node, ephemeral test deployment of RustFS. Pinned rather than
/// `latest` so that what the conditional-`DELETE` behaviour is asserted against does not
/// change under us.
const RUSTFS_IMAGE: &str = "rustfs/rustfs:1.0.4";

/// Name of the RustFS container, and therefore the name its S3 API is reachable under from
/// other containers on the same compose network.
pub(crate) const CONTAINER: &str = "rustfs";
/// Container port RustFS's S3 API listens on.
pub(crate) const S3_PORT: &str = "9000";
/// Directory RustFS stores its data under, overriding the image's own `RUSTFS_VOLUMES`
/// default. Under `/tmp` so that it lands on the tmpfs composer mounts and goes away with
/// the container.
const DATA_DIR: &str = "/tmp/rustfs-data";
/// As [`DATA_DIR`], but under the bind-mounted host `/tmp`, so the data outlives the
/// container. Used when `--persistent-etcd` is given: the flag is named for etcd but
/// carries the same meaning here, rather than growing a second one for the same choice.
const PERSISTENT_DATA_DIR: &str = "/host-tmp/rustfs-data";

#[async_trait]
impl ComponentAction for Rustfs {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        Ok(if !options.rustfs {
            cfg
        } else {
            let data_dir = if options.persistent_etcd {
                PERSISTENT_DATA_DIR
            } else {
                DATA_DIR
            };
            let spec = composer::ContainerSpec::from_image(CONTAINER, RUSTFS_IMAGE)
                .with_env("RUSTFS_ACCESS_KEY", S3_ACCESS_KEY_ID)
                .with_env("RUSTFS_SECRET_KEY", S3_SECRET_ACCESS_KEY)
                .with_env("RUSTFS_VOLUMES", data_dir)
                .with_env("RUSTFS_OBS_LOGGER_LEVEL", "warn")
                .with_portmap(S3_PORT, S3_HOST_PORT);
            let spec = if options.persistent_etcd {
                spec.with_bind("/tmp/", "/host-tmp")
            } else {
                spec
            };

            cfg.add_container_spec(spec)
        })
    }

    async fn start(&self, options: &StartOptions, cfg: &crate::ComposeTestNt) -> Result<(), Error> {
        if !options.rustfs {
            return Ok(());
        }
        cfg.start(CONTAINER).await?;
        Ok(())
    }

    async fn wait_on(
        &self,
        options: &StartOptions,
        _cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        if !options.rustfs {
            return Ok(());
        }
        Components::wait_url(&format!("http://localhost:{S3_HOST_PORT}/")).await?;

        // Create the bucket, so that the S3 CLIs work against this instance with no manual
        // setup: see the `RC_HOST_local` variable in `nix/shells/test.nix`.
        create_default_bucket().await
    }
}

/// Creates an [`S3_BUCKET`] by running `rc`, RustFS's own cli client.
async fn create_default_bucket() -> Result<(), Error> {
    let output = tokio::process::Command::new("rc")
        .args(["bucket", "create", &rc_path("")])
        // Supplied explicitly rather than inherited, so this depends on neither the
        // caller's environment nor any alias they have configured for themselves.
        .env(format!("RC_HOST_{RC_ALIAS}"), rc_host())
        .output()
        .await?;

    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "Failed to create the '{S3_BUCKET}' bucket (exit: {:?}): {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    )
    .into())
}
