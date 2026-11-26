use crate::{
    build_error,
    infra::{async_trait, Builder, ComponentAction, DrAgent, Error, StartOptions},
};
use composer::Binary;
use grpc::operations::dr::client::DrClient;
use std::str::FromStr;

/// Name of the dr agent container, and the name it is reachable under from other containers
/// on the same compose network.
pub(crate) const CONTAINER: &str = "agent-dr";
/// Port the dr agent's gRPC server listens on.
pub(crate) const GRPC_PORT: u16 = 11700;
/// Where the host's CA bundle is mounted in the dr agent's container.
const CA_BUNDLE: &str = "/etc/ssl/certs/ca-certificates.crt";

/// The host's CA bundle, wherever this distribution keeps it, with its symlinks resolved
/// so that the bind is of the file itself.
///
/// The dr agent's http client loads the system's roots when it is built, whatever the
/// scheme of the store it is for, and refuses to be built without any. A container started
/// from a host binary has none of its own - the agent's image carries `cacert`, which is
/// what this stands in for.
fn ca_bundle() -> Option<std::path::PathBuf> {
    let configured = ["SSL_CERT_FILE", "NIX_SSL_CERT_FILE"]
        .iter()
        .filter_map(|var| std::env::var_os(var).map(std::path::PathBuf::from));
    let known = [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/cert.pem",
    ]
    .iter()
    .map(std::path::PathBuf::from);
    configured.chain(known).find_map(|path| {
        std::fs::canonicalize(path)
            .ok()
            .filter(|path| path.is_file())
    })
}

#[async_trait]
impl ComponentAction for DrAgent {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        if options.build {
            let status = std::process::Command::new("cargo")
                .args(["build", "-p", "agents", "--bin", CONTAINER])
                .status()?;
            build_error(&format!("the {CONTAINER} agent"), status.code())?;
        }
        let etcd = format!("etcd.{}:2379", options.cluster_label.name());
        let mut binary = Binary::from_dbg(CONTAINER)
            .with_args(vec!["-g", &format!("[::]:{GRPC_PORT}")])
            // The pair is the dr agent's own resource, so it holds its own connection to
            // the persistent store rather than reaching it through core.
            .with_args(vec!["--store", &etcd]);

        // Core answers whether a volume exists. Its client picks TLS from the scheme, so
        // the scheme has to match how core was started.
        let scheme = if options.no_grpc_tls { "http" } else { "https" };
        binary = binary.with_args(vec!["--core-grpc", &format!("{scheme}://core:50051")]);

        if let Some(timeout) = &options.store_timeout {
            binary = binary.with_args(vec!["--store-timeout", &timeout.to_string()]);
        }

        if let Some(env) = &options.agents_env {
            for kv in env {
                binary = binary.with_env(kv.key.as_str(), kv.value.as_str().as_ref());
            }
        }

        if !options.no_grpc_tls {
            binary = binary.with_arg("--grpc-auto-tls");
        }

        // Without it the agent cannot open a pair's store at all.
        if let Some(bundle) = ca_bundle() {
            binary = binary
                .with_bind(&bundle.to_string_lossy(), CA_BUNDLE)
                .with_env("SSL_CERT_FILE", CA_BUNDLE);
        }

        if cfg.container_exists("jaeger") {
            let jaeger = format!("jaeger.{}", cfg.get_name());
            binary = binary.with_args(vec!["--jaeger", &jaeger]);
        }

        if let Some(size) = &options.otel_max_batch_size {
            binary = binary.with_env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", size);
        }

        Ok(cfg.add_container_bin(CONTAINER, options.fips_env(binary)))
    }

    async fn start(
        &self,
        _options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        cfg.start(CONTAINER).await?;
        Ok(())
    }

    async fn wait_on(
        &self,
        options: &StartOptions,
        cfg: &crate::ComposeTestNt,
    ) -> Result<(), Error> {
        let ip = cfg.container_ip(CONTAINER);
        let scheme = if options.no_grpc_tls { "http" } else { "https" };
        let uri = tonic::transport::Uri::from_str(&format!("{scheme}://{ip}:{GRPC_PORT}"))
            .expect("a well-formed uri");
        let timeout = grpc::context::TimeoutOptions::new()
            .with_req_timeout(std::time::Duration::from_millis(5));
        let dr = DrClient::new(uri, Some(timeout.with_max_retries(Some(10)))).await;
        dr.wait_ready(None).await.map_err(|_| {
            let error = "Failed to wait for the dr service to get ready";
            std::io::Error::new(std::io::ErrorKind::TimedOut, error)
        })?;
        Ok(())
    }
}
