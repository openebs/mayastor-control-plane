//! File-backed mutual TLS (mTLS) for the gRPC transports of a deployment.
//!
//! When enabled, a throwaway certificate authority is generated per cluster and a leaf certificate
//! is issued for every TLS component (the core agent and each io-engine), along with a client
//! certificate for the deployer itself. The material lives in the workspace tmp folder and is
//! bind-mounted read-only into the containers at [`CONTAINER_DIR`], so components are configured
//! with the usual `--grpc-tls-{cert,key,ca}-file` arguments.
//!
//! Certificates carry the container name and IP as subject alternative names: components reach
//! each other by name (e.g. the io-engine registers against `core`) and by IP (e.g. the core
//! dials the io-engine's registered gRPC endpoint), and the deployer reaches them by IP.

use crate::{
    infra::{Error, IoEngine, StartOptions},
    ComposeTestNt,
};
use grpc_tls::pki::CertificateAuthority;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tonic::transport::{Channel, Endpoint};

/// Where the certificate files are mounted inside the containers.
pub const CONTAINER_DIR: &str = "/grpc-tls";

/// The common name of the CA.
const CA_NAME: &str = "deployer-grpc-ca";
/// The identity used by the deployer (and test clients) to authenticate to the components.
const CLIENT_NAME: &str = "deployer";

/// The mTLS material of a deployment.
#[derive(Clone, Debug)]
pub struct GrpcMtls {
    dir: PathBuf,
}

impl GrpcMtls {
    /// The mTLS material of the deployment, if mTLS is enabled.
    pub fn from_options(options: &StartOptions) -> Result<Option<Self>, Error> {
        if !options.grpc_mtls {
            return Ok(None);
        }
        let dir = PathBuf::from(crate::host_tmp()?)
            .join("grpc-tls")
            .join(options.cluster_label.name());
        Ok(Some(Self { dir }))
    }

    /// Create the (empty) certificate directory, so it exists before being bind-mounted.
    pub(crate) fn prepare(&self) -> Result<(), Error> {
        if self.dir.exists() {
            std::fs::remove_dir_all(&self.dir)?;
        }
        std::fs::create_dir_all(&self.dir)?;
        Ok(())
    }

    /// Bind-mount the certificate directory into a container spec.
    pub(crate) fn bind(&self, spec: composer::ContainerSpec) -> composer::ContainerSpec {
        spec.with_bind(&self.dir.to_string_lossy(), CONTAINER_DIR)
    }

    /// The `--grpc-tls-*` arguments for a TLS component, using the in-container paths.
    pub(crate) fn args(name: &str) -> Vec<String> {
        vec![
            "--grpc-tls-cert-file".to_string(),
            format!("{CONTAINER_DIR}/{name}.crt"),
            "--grpc-tls-key-file".to_string(),
            format!("{CONTAINER_DIR}/{name}.key"),
            "--grpc-tls-ca-file".to_string(),
            format!("{CONTAINER_DIR}/ca.pem"),
        ]
    }

    /// Generate the CA and issue certificates for every TLS component and for the deployer.
    ///
    /// This must run after the containers are created (so their IPs are known) and before any
    /// TLS component is started.
    pub(crate) fn provision(
        &self,
        options: &StartOptions,
        cfg: &ComposeTestNt,
    ) -> Result<(), Error> {
        let ca = CertificateAuthority::generate(CA_NAME)?;
        ca.write_certificate(&self.ca_path())?;

        let mut components = vec!["core".to_string()];
        components.extend(
            (0..options.io_engines + options.idle_io_engines).map(|i| IoEngine::name(i, options)),
        );
        for name in components {
            let Some((_, ip)) = cfg.containers().get(&name) else {
                continue;
            };
            let sans = vec![
                name.clone(),
                ip.to_string(),
                "localhost".to_string(),
                "127.0.0.1".to_string(),
            ];
            ca.issue(&name, &sans)?
                .write(&self.cert_path(&name), &self.key_path(&name))?;
        }

        ca.issue(CLIENT_NAME, &[CLIENT_NAME.to_string()])?
            .write(&self.cert_path(CLIENT_NAME), &self.key_path(CLIENT_NAME))?;

        Ok(())
    }

    fn ca_path(&self) -> PathBuf {
        self.dir.join("ca.pem")
    }
    fn cert_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.crt"))
    }
    fn key_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.key"))
    }

    /// The deployer's client TLS configuration (host paths), for the control-plane gRPC clients.
    pub fn client_tls(&self) -> Result<grpc::tls::TlsConfig, Error> {
        Ok(grpc::tls::TlsConfig::new(
            Some(self.ca_path()),
            Some(self.cert_path(CLIENT_NAME)),
            Some(self.key_path(CLIENT_NAME)),
        )?)
    }

    /// Eagerly connect to an io-engine `endpoint` over mTLS, presenting the deployer's client
    /// certificate.
    pub async fn io_connect(&self, endpoint: Endpoint) -> Result<Channel, tonic::transport::Error> {
        let config = self
            .client_tls()
            .and_then(|tls| Ok(tls.client_config()?))
            .expect("the deployer mTLS client certificate files must be readable");
        grpc_tls::tls_connect(&endpoint, Arc::new(config)).await
    }

    /// The directory holding the certificate files on the host.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}
