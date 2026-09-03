//! File-backed TLS configuration for gRPC transports.

use futures::StreamExt;
use hyper_util::rt::TokioIo;
use rcgen::generate_simple_self_signed;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, PoisonError, RwLock},
    task::{Context as TaskContext, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor, TlsConnector};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    body::Body,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig},
};
use tower::{Service, ServiceExt};

#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer,
        _intermediates: &[rustls::pki_types::CertificateDer],
        _server_name: &rustls::pki_types::ServerName,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // this is expected in auto-tls mode, so don't spam the logs with warnings
        // tracing::warn!("gRPC server certificate verification bypassed");
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _certificate: &rustls::pki_types::CertificateDer,
        _signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _certificate: &rustls::pki_types::CertificateDer,
        _signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme;

        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

fn auto_client_config() -> rustls::ClientConfig {
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification));
    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN for clarity even though tonic
    // handles this with with `assume_http2` enabled.
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

/// Establish a single TLS connection to the endpoint described by `uri`, bypassing certificate
/// verification (auto-TLS). The returned stream is wrapped for use with hyper/tonic.
async fn auto_tls_connect_io(
    tls: TlsConnector,
    uri: http::Uri,
) -> io::Result<TokioIo<tokio_rustls::client::TlsStream<TcpStream>>> {
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "gRPC endpoint has no host"))?;
    let port = uri.port_u16().unwrap_or(443);
    let stream = TcpStream::connect((host, port)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    tls.connect(server_name, stream)
        .await
        .map(TokioIo::new)
        .map_err(io::Error::other)
}

/// Eagerly establish an auto-TLS channel to `endpoint`, bypassing certificate verification.
///
/// This matches the REST client's `TlsMode::Auto` behaviour and is used to connect to io-engine
/// instances that advertise gRPC TLS support in their registration.
pub async fn auto_tls_connect(endpoint: &Endpoint) -> Result<Channel, tonic::transport::Error> {
    let tls = TlsConnector::from(Arc::new(auto_client_config()));
    let connector = tower::service_fn(move |uri: http::Uri| auto_tls_connect_io(tls.clone(), uri));
    endpoint.connect_with_connector(connector).await
}

/// TLS certificate files used by a gRPC endpoint.
///
/// The files are intentionally retained rather than converted into certificate bytes so callers
/// can rebuild a configuration after certificates are rotated on disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    ca_certificate: Option<PathBuf>,
    certificate: Option<PathBuf>,
    private_key: Option<PathBuf>,
}

impl TlsConfig {
    /// Create a TLS configuration from optional CA and client/server identity files.
    pub fn new(
        ca_certificate: Option<PathBuf>,
        certificate: Option<PathBuf>,
        private_key: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        if certificate.is_some() != private_key.is_some() {
            anyhow::bail!("both the TLS certificate and private key files must be specified");
        }

        Ok(Self {
            ca_certificate,
            certificate,
            private_key,
        })
    }

    /// Whether TLS has been enabled for this endpoint.
    pub fn enabled(&self) -> bool {
        self.ca_certificate.is_some() || self.certificate.is_some()
    }

    /// Certificate files that must be watched for automatic reload.
    pub fn paths(&self) -> Vec<PathBuf> {
        [
            self.ca_certificate.clone(),
            self.certificate.clone(),
            self.private_key.clone(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Filesystem targets to watch for certificate rotation.
    ///
    /// Kubernetes projected secrets replace directory entries atomically, so watch their shared
    /// parent directory when all TLS files live below it.
    pub fn watch_targets(&self) -> Vec<PathBuf> {
        cert_watcher::watch_targets(self.paths())
    }

    /// Build a fresh client TLS configuration from the current certificate file contents.
    pub fn client_config(&self) -> anyhow::Result<ClientTlsConfig> {
        let mut config = ClientTlsConfig::new();

        if let Some(ca_certificate) = &self.ca_certificate {
            config = config.ca_certificate(Certificate::from_pem(std::fs::read(ca_certificate)?));
        }
        if let (Some(certificate), Some(private_key)) = (&self.certificate, &self.private_key) {
            config = config.identity(Identity::from_pem(
                std::fs::read(certificate)?,
                std::fs::read(private_key)?,
            ));
        }
        Ok(config)
    }

    /// Build a fresh server TLS configuration from the current certificate file contents.
    pub fn server_config(&self) -> anyhow::Result<ServerTlsConfig> {
        let (Some(certificate), Some(private_key)) = (&self.certificate, &self.private_key) else {
            anyhow::bail!("a TLS server requires both certificate and private key files");
        };

        let mut config = ServerTlsConfig::new().identity(Identity::from_pem(
            std::fs::read(certificate)?,
            std::fs::read(private_key)?,
        ));
        if let Some(ca_certificate) = &self.ca_certificate {
            config = config.client_ca_root(Certificate::from_pem(std::fs::read(ca_certificate)?));
        }

        Ok(config)
    }

    fn fingerprint(&self) -> anyhow::Result<Vec<std::time::SystemTime>> {
        Ok(cert_watcher::fingerprint(&self.paths())?)
    }
}

/// A Tonic channel that is replaced when its file-backed TLS material changes.
///
/// Existing requests retain their current channel. Requests issued after a successful reload use
/// a fresh lazy channel, and therefore negotiate TLS with the newly loaded certificates.
#[derive(Clone)]
pub struct ReloadableChannel {
    current: Arc<RwLock<Channel>>,
}

struct ChannelState {
    endpoint: Endpoint,
    tls: TlsConfig,
    fingerprint: Vec<std::time::SystemTime>,
    current: Arc<RwLock<Channel>>,
}

impl ReloadableChannel {
    /// Build a channel from an endpoint and optional file-backed TLS configuration.
    pub fn new(endpoint: Endpoint, tls: Option<TlsConfig>, auto_tls: bool) -> anyhow::Result<Self> {
        let tls = tls.filter(TlsConfig::enabled);
        let current = Arc::new(RwLock::new(Self::connect_lazy(
            &endpoint,
            tls.as_ref(),
            auto_tls,
        )?));

        if let Some(tls) = tls {
            let targets = tls.watch_targets();
            let paths = tls.paths();
            let state = Arc::new(RwLock::new(ChannelState {
                fingerprint: tls.fingerprint()?,
                endpoint,
                tls,
                current: current.clone(),
            }));
            cert_watcher::spawn("grpc-tls-cert-watcher", targets, move || {
                Self::reload_logged(&state, &paths)
            });
        }

        Ok(Self { current })
    }

    fn connect_lazy(
        endpoint: &Endpoint,
        tls: Option<&TlsConfig>,
        auto_tls: bool,
    ) -> anyhow::Result<Channel> {
        if auto_tls {
            let tls = TlsConnector::from(Arc::new(auto_client_config()));
            let connector =
                tower::service_fn(move |uri: http::Uri| auto_tls_connect_io(tls.clone(), uri));
            return Ok(endpoint.connect_with_connector_lazy(connector));
        }

        let endpoint = match tls {
            Some(tls) => endpoint.clone().tls_config(tls.client_config()?)?,
            None => endpoint.clone(),
        };
        Ok(endpoint.connect_lazy())
    }

    fn channel(&self) -> Channel {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn reload(state: &Arc<RwLock<ChannelState>>) -> anyhow::Result<bool> {
        let state_guard = state.read().unwrap_or_else(PoisonError::into_inner);
        let fingerprint = state_guard.tls.fingerprint()?;
        if fingerprint == state_guard.fingerprint {
            return Ok(false);
        }
        let channel = Self::connect_lazy(&state_guard.endpoint, Some(&state_guard.tls), false)?;
        *state_guard
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = channel;
        drop(state_guard);
        state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .fingerprint = fingerprint;
        Ok(true)
    }

    fn reload_logged(state: &Arc<RwLock<ChannelState>>, paths: &[PathBuf]) {
        match Self::reload(state) {
            Ok(true) => tracing::info!(?paths, "Reloaded gRPC TLS certificates"),
            Ok(false) => {}
            Err(error) => tracing::warn!(?paths, %error, "Failed to reload gRPC TLS certificates"),
        }
    }
}

type ChannelRequest = http::Request<Body>;
type ChannelFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    <Channel as Service<ChannelRequest>>::Response,
                    <Channel as Service<ChannelRequest>>::Error,
                >,
            > + Send,
    >,
>;

impl Service<ChannelRequest> for ReloadableChannel {
    type Response = <Channel as Service<ChannelRequest>>::Response;
    type Error = <Channel as Service<ChannelRequest>>::Error;
    type Future = ChannelFuture;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ChannelRequest) -> Self::Future {
        let channel = self.channel();
        Box::pin(async move { channel.oneshot(request).await })
    }
}

/// The content type of a TLS handshake record, which is the first byte of every TLS `ClientHello`.
const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// A gRPC server connection accepted on a port that serves both plaintext and TLS.
///
/// The transport is chosen by peeking the first byte of the stream: a TLS `ClientHello` record
/// always starts with `0x16`, whereas cleartext HTTP/2 (h2c) starts with the connection preface
/// `PRI * HTTP/2.0` (i.e. `0x50`). This lets a single listener accept legacy plaintext clients and
/// TLS clients at the same time, which is what allows a non-TLS to TLS rolling upgrade.
pub enum MaybeTlsConnection {
    /// A plaintext connection.
    Plain(Pin<Box<TcpStream>>),
    /// A TLS connection.
    Tls(Pin<Box<TlsStream<TcpStream>>>),
}

impl AsyncRead for MaybeTlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsConnection::Plain(stream) => stream.as_mut().poll_read(cx, buf),
            MaybeTlsConnection::Tls(stream) => stream.as_mut().poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsConnection::Plain(stream) => stream.as_mut().poll_write(cx, buf),
            MaybeTlsConnection::Tls(stream) => stream.as_mut().poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsConnection::Plain(stream) => stream.as_mut().poll_flush(cx),
            MaybeTlsConnection::Tls(stream) => stream.as_mut().poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsConnection::Plain(stream) => stream.as_mut().poll_shutdown(cx),
            MaybeTlsConnection::Tls(stream) => stream.as_mut().poll_shutdown(cx),
        }
    }
}

impl tonic::transport::server::Connected for MaybeTlsConnection {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

/// Sniff the first byte of an accepted connection and either complete a TLS handshake or use the
/// connection as plaintext.
///
/// Any failure is mapped to a recoverable error kind so a single bad connection never tears down
/// the listener.
async fn sniff_and_accept(
    connection: TcpStream,
    acceptor: TlsAcceptor,
) -> io::Result<MaybeTlsConnection> {
    let mut first = [0u8; 1];
    let peeked = connection
        .peek(&mut first)
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if peeked == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed before any data was received",
        ));
    }

    if first[0] == TLS_HANDSHAKE_RECORD {
        acceptor
            .accept(connection)
            .await
            .map(|stream| MaybeTlsConnection::Tls(Box::pin(stream)))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    } else {
        Ok(MaybeTlsConnection::Plain(Box::pin(connection)))
    }
}

/// Complete a TLS handshake on an accepted connection, rejecting plaintext clients.
///
/// Unlike [`sniff_and_accept`], this never falls back to plaintext: it is used by servers that
/// only ever speak to TLS clients and therefore do not need to accept a mixed-transport port.
async fn accept_tls(
    connection: TcpStream,
    acceptor: TlsAcceptor,
) -> io::Result<MaybeTlsConnection> {
    acceptor
        .accept(connection)
        .await
        .map(|stream| MaybeTlsConnection::Tls(Box::pin(stream)))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Clone)]
struct ReloadableServerTls {
    current: Arc<RwLock<Arc<rustls::ServerConfig>>>,
}

struct ServerState {
    tls: TlsConfig,
    fingerprint: Vec<std::time::SystemTime>,
    current: Arc<RwLock<Arc<rustls::ServerConfig>>>,
}

impl ReloadableServerTls {
    fn new(tls: TlsConfig) -> anyhow::Result<Self> {
        let current = Arc::new(RwLock::new(Arc::new(build_rustls_server_config(&tls)?)));
        let targets = tls.watch_targets();
        let paths = tls.paths();
        let state = Arc::new(RwLock::new(ServerState {
            fingerprint: tls.fingerprint()?,
            tls,
            current: current.clone(),
        }));
        cert_watcher::spawn("grpc-tls-cert-watcher", targets, move || {
            Self::reload_logged(&state, &paths)
        });
        Ok(Self { current })
    }

    fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(
            self.current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
    }

    fn reload(state: &Arc<RwLock<ServerState>>) -> anyhow::Result<bool> {
        let state_guard = state.read().unwrap_or_else(PoisonError::into_inner);
        let fingerprint = state_guard.tls.fingerprint()?;
        if fingerprint == state_guard.fingerprint {
            return Ok(false);
        }
        let config = Arc::new(build_rustls_server_config(&state_guard.tls)?);
        *state_guard
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = config;
        drop(state_guard);
        state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .fingerprint = fingerprint;
        Ok(true)
    }

    fn reload_logged(state: &Arc<RwLock<ServerState>>, targets: &[PathBuf]) {
        match Self::reload(state) {
            Ok(true) => tracing::info!(?targets, "Reloaded gRPC TLS certificates"),
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(?targets, %error, "Failed to reload gRPC TLS certificates")
            }
        }
    }
}

fn build_rustls_server_config(tls: &TlsConfig) -> anyhow::Result<rustls::ServerConfig> {
    use rustls::{server::WebPkiClientVerifier, RootCertStore};
    use std::{fs::File, io::BufReader};

    let (Some(certificate), Some(private_key)) = (&tls.certificate, &tls.private_key) else {
        anyhow::bail!("a TLS server requires both certificate and private key files");
    };
    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(certificate)?))
        .collect::<Result<Vec<_>, _>>()?;
    let private_key = rustls_pemfile::private_key(&mut BufReader::new(File::open(private_key)?))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in the TLS key file"))?;

    let builder = rustls::ServerConfig::builder();
    let mut config = if let Some(ca_certificate) = &tls.ca_certificate {
        let client_ca_certificates =
            rustls_pemfile::certs(&mut BufReader::new(File::open(ca_certificate)?))
                .collect::<Result<Vec<_>, _>>()?;
        let mut roots = RootCertStore::empty();
        let (valid, _) = roots.add_parsable_certificates(client_ca_certificates);
        if valid == 0 {
            anyhow::bail!("no valid certificates found in the TLS client CA file");
        }
        builder
            .with_client_cert_verifier(WebPkiClientVerifier::builder(Arc::new(roots)).build()?)
            .with_single_cert(certificates, private_key)?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)?
    };

    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN for clarity even though tonic
    // handles this with with `assume_http2` enabled.
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(config)
}

/// Maximum number of connections whose TLS sniff/handshake may be in progress at once.
///
/// This bounds the work performed off the accept path and provides backpressure: once this many
/// handshakes are in flight, no further connections are accepted until one completes.
const MAX_CONCURRENT_HANDSHAKES: usize = 256;

/// Accept connections and perform the TLS handshake concurrently.
///
/// When `sniff` is set the listener also accepts plaintext connections by peeking the first byte
/// (see [`MaybeTlsConnection`]); otherwise plaintext clients are rejected. Only a server sitting at
/// a mixed-version boundary (a plaintext peer and a TLS peer during a rolling upgrade) needs
/// sniffing; every other server speaks TLS to TLS-only clients.
///
/// [`StreamExt::then`] drives each sniff/handshake future to completion before accepting the next
/// connection, so a single slow or stalled peer would block every other client on the listener.
/// [`StreamExt::buffer_unordered`] instead keeps up to [`MAX_CONCURRENT_HANDSHAKES`] handshakes in
/// flight at once and yields whichever completes first, so no connection is held up by another.
fn accepted_incoming(
    listener: tokio::net::TcpListener,
    acceptor: impl Fn() -> TlsAcceptor + Send + 'static,
    sniff: bool,
) -> impl futures::Stream<Item = Result<MaybeTlsConnection, io::Error>> {
    TcpListenerStream::new(listener)
        .map(move |connection| {
            let acceptor = acceptor();
            async move {
                match connection {
                    Ok(connection) if sniff => sniff_and_accept(connection, acceptor).await,
                    Ok(connection) => accept_tls(connection, acceptor).await,
                    Err(error) => Err(error),
                }
            }
        })
        .buffer_unordered(MAX_CONCURRENT_HANDSHAKES)
}

/// Bind a TCP listener and accept gRPC connections using reloadable TLS material.
///
/// When `sniff` is set the listener serves both TLS and plaintext connections on the same port,
/// choosing per connection by sniffing the first byte (see [`MaybeTlsConnection`]); otherwise only
/// TLS clients are accepted.
pub async fn incoming(
    socket: SocketAddr,
    tls: TlsConfig,
    sniff: bool,
) -> anyhow::Result<impl futures::Stream<Item = Result<MaybeTlsConnection, io::Error>>> {
    let listener = tokio::net::TcpListener::bind(socket).await?;
    let tls = ReloadableServerTls::new(tls)?;

    Ok(accepted_incoming(listener, move || tls.acceptor(), sniff))
}

/// Generate an ephemeral self-signed server configuration for local or test gRPC listeners.
///
/// Because its certificate only exists in memory, this configuration cannot be reloaded and does
/// not support client authentication.
pub fn auto_server_config(
    subject_alt_names: Vec<String>,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    let certificate_material = generate_simple_self_signed(subject_alt_names)
        .map_err(|error| anyhow::anyhow!("Failed to generate self-signed certificate: {error}"))?;
    let certificate = certificate_material.cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certificate_material.key_pair.serialize_der(),
    ));

    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], private_key)?;
    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN for clarity even though tonic
    // handles this with with `assume_http2` enabled.
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Arc::new(config))
}

/// Bind a TCP listener with an in-memory TLS server configuration.
///
/// When `sniff` is set the listener serves both TLS and plaintext connections on the same port,
/// choosing per connection by sniffing the first byte (see [`MaybeTlsConnection`]); otherwise only
/// TLS clients are accepted.
pub async fn incoming_with_server_config(
    socket: SocketAddr,
    config: Arc<rustls::ServerConfig>,
    sniff: bool,
) -> anyhow::Result<impl futures::Stream<Item = Result<MaybeTlsConnection, io::Error>>> {
    let listener = tokio::net::TcpListener::bind(socket).await?;
    let acceptor = TlsAcceptor::from(config);

    Ok(accepted_incoming(listener, move || acceptor.clone(), sniff))
}

#[cfg(test)]
mod tests {
    use super::TlsConfig;
    use std::path::PathBuf;

    #[test]
    fn rejects_incomplete_identity() {
        assert!(TlsConfig::new(None, Some(PathBuf::from("client.pem")), None).is_err());
        assert!(TlsConfig::new(None, None, Some(PathBuf::from("client.key"))).is_err());
    }

    #[test]
    fn tracks_all_tls_files() {
        let tls = TlsConfig::new(
            Some(PathBuf::from("ca.pem")),
            Some(PathBuf::from("client.pem")),
            Some(PathBuf::from("client.key")),
        )
        .unwrap();

        assert!(tls.enabled());
        assert_eq!(
            tls.paths(),
            vec![
                PathBuf::from("ca.pem"),
                PathBuf::from("client.pem"),
                PathBuf::from("client.key"),
            ]
        );
    }
}
