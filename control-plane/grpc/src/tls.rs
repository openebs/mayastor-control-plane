//! gRPC TLS for the control-plane transports, on top of the shared [`grpc_tls`] primitives.
//!
//! Servers and clients come in two flavours: auto-TLS (an ephemeral self-signed server
//! certificate, with the client bypassing certificate verification) and file-backed TLS, where
//! the certificate files are reloaded when they rotate on disk and a CA enables mutual TLS.

use tonic::transport::{Channel, Endpoint};

pub use grpc_tls::{
    auto_tls_connect, file_tls_connect, Error, Listener, MaybeTlsConnection, TlsConfig,
};

/// Connect a channel to an io-engine `endpoint`, using auto-TLS when `tls` is set, otherwise a
/// plaintext connection. The endpoint must carry an `http` scheme in both cases: for auto-TLS the
/// handshake is performed by the custom connector, not tonic.
pub async fn io_connect(endpoint: Endpoint, tls: bool) -> Result<Channel, tonic::transport::Error> {
    if tls {
        auto_tls_connect(&endpoint).await
    } else {
        endpoint.connect().await
    }
}

/// The gRPC TLS client policy towards the io-engines.
///
/// An io-engine advertises whether it serves gRPC over TLS in its registration. When it does and
/// we have file-backed certificates, we verify it against our CA and present our certificate for
/// mutual TLS; without certificates we fall back to auto-TLS (encryption without
/// authentication). When it doesn't we connect over plaintext, unless TLS is enforced in which
/// case the caller must reject the io-engine.
#[derive(Clone, Debug, Default)]
pub struct IoEngineTls {
    files: Option<TlsConfig>,
    enforced: bool,
}

impl IoEngineTls {
    /// A new policy from the optional file-backed certificates and whether TLS is enforced.
    pub fn new(files: Option<TlsConfig>, enforced: bool) -> Self {
        Self {
            files: files.filter(TlsConfig::enabled),
            enforced,
        }
    }

    /// Whether gRPC TLS is enforced, meaning io-engines that don't support TLS must be rejected
    /// rather than connected over plaintext.
    pub fn enforced(&self) -> bool {
        self.enforced
    }

    /// Eagerly connect a channel to an io-engine `endpoint` (which must carry an `http` scheme)
    /// where `node_tls` is whether the io-engine advertises gRPC TLS.
    ///
    /// The certificate files are read for every connection so a reconnect after a certificate
    /// rotation uses the latest material.
    pub async fn connect(&self, endpoint: &Endpoint, node_tls: bool) -> Result<Channel, Error> {
        match (&self.files, node_tls) {
            (Some(files), true) => grpc_tls::file_tls_connect(endpoint, files).await,
            (None, true) => auto_tls_connect(endpoint)
                .await
                .map_err(|source| Error::Connect { source }),
            (_, false) => endpoint
                .connect()
                .await
                .map_err(|source| Error::Connect { source }),
        }
    }
}

/// Build a lazy client channel to `endpoint` under the given TLS policy.
///
/// File-backed TLS reloads its certificate material when the files rotate on disk; auto-TLS and
/// plaintext channels have nothing to reload. The channel is lazy, so the connection is deferred
/// until the first request.
pub fn client_channel(
    endpoint: Endpoint,
    tls: Option<TlsConfig>,
    auto_tls: bool,
) -> Result<Channel, Error> {
    match tls.filter(TlsConfig::enabled) {
        Some(tls) => grpc_tls::file_tls_connect_lazy(&endpoint, &tls),
        None if auto_tls => Ok(grpc_tls::auto_tls_connect_lazy(&endpoint)),
        None => Ok(endpoint.connect_lazy()),
    }
}
