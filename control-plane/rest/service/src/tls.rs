//! Hot-reloading of the REST server's TLS material.
//!
//! The server certificate/key are served through a [`ResolvesServerCert`], and (when mTLS is
//! enabled) client certificates are validated through a swappable [`ClientCertVerifier`]. A
//! single background watcher swaps both whenever the underlying files change on disk (e.g.
//! cert-manager rotations or a full secret recreation that also replaces the client CA bundle).
//! Renewals therefore take effect without a restart: existing TLS sessions are unaffected and the
//! new material is used on subsequent handshakes.

use rustls::{
    client::danger::HandshakeSignatureValid,
    crypto::CryptoProvider,
    pki_types::{CertificateDer, UnixTime},
    server::{
        danger::{ClientCertVerified, ClientCertVerifier},
        ClientHello, ResolvesServerCert, WebPkiClientVerifier,
    },
    sign::CertifiedKey,
    DigitallySignedStruct, DistinguishedName, Error as RustlsError, RootCertStore, SignatureScheme,
};
use rustls_pemfile::{certs, private_key};
use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{Arc, PoisonError, RwLock},
};

/// A [`ResolvesServerCert`] whose certificate can be swapped at runtime.
#[derive(Debug)]
pub(crate) struct ReloadableCertResolver {
    current: RwLock<Arc<CertifiedKey>>,
}

impl ReloadableCertResolver {
    fn new(certified_key: Arc<CertifiedKey>) -> Self {
        Self {
            current: RwLock::new(certified_key),
        }
    }
    fn store(&self, certified_key: Arc<CertifiedKey>) {
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = certified_key;
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        Some(
            self.current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
    }
}

/// A [`ClientCertVerifier`] whose backing verifier (the trusted client CA roots) can be swapped
/// at runtime, so a rotated or fully recreated client CA bundle is honoured without a restart.
///
/// All decisions are delegated to the inner verifier. `root_hint_subjects` returns an empty slice
/// because the trait requires it to borrow from `&self`, which is incompatible with swapping the
/// inner verifier behind a lock; the hint is optional and only advertises accepted CA names to the
/// client, so clients configured with an explicit certificate are unaffected.
#[derive(Debug)]
pub(crate) struct ReloadableClientCertVerifier {
    current: RwLock<Arc<dyn ClientCertVerifier>>,
}

impl ReloadableClientCertVerifier {
    fn new(verifier: Arc<dyn ClientCertVerifier>) -> Self {
        Self {
            current: RwLock::new(verifier),
        }
    }
    fn store(&self, verifier: Arc<dyn ClientCertVerifier>) {
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = verifier;
    }
    fn get(&self) -> Arc<dyn ClientCertVerifier> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ClientCertVerifier for ReloadableClientCertVerifier {
    fn offer_client_auth(&self) -> bool {
        self.get().offer_client_auth()
    }
    fn client_auth_mandatory(&self) -> bool {
        self.get().client_auth_mandatory()
    }
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        self.get()
            .verify_client_cert(end_entity, intermediates, now)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.get().verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.get().verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.get().supported_verify_schemes()
    }
}

/// Create the reloadable server certificate resolver and (optionally) client certificate verifier.
///
/// The initial material is loaded from the given files and a single background watcher is spawned
/// that swaps the server certificate and/or the client CA verifier whenever their files change.
/// When `client_ca_path` is `None`, client authentication is disabled and the returned verifier
/// accepts anonymous clients.
pub(crate) fn reloadable_tls(
    cert_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: Option<PathBuf>,
    provider: Arc<CryptoProvider>,
) -> anyhow::Result<(Arc<ReloadableCertResolver>, Arc<dyn ClientCertVerifier>)> {
    let certified_key = load_certified_key(&cert_path, &key_path, &provider)?;
    let resolver = Arc::new(ReloadableCertResolver::new(certified_key));

    let (verifier, reloadable_verifier): (Arc<dyn ClientCertVerifier>, _) = match &client_ca_path {
        Some(ca_path) => {
            let inner = build_client_verifier(ca_path, &provider)?;
            let reloadable = Arc::new(ReloadableClientCertVerifier::new(inner));
            (reloadable.clone(), Some(reloadable))
        }
        None => (WebPkiClientVerifier::no_client_auth(), None),
    };

    spawn_cert_watcher(WatchContext {
        cert_path,
        key_path,
        client_ca_path,
        provider,
        resolver: resolver.clone(),
        verifier: reloadable_verifier,
        targets: vec![],
    });

    Ok((resolver, verifier))
}

/// Build a [`CertifiedKey`] from certificate and key files, using `provider`'s key loader so the
/// signing key matches the process-wide crypto provider.
fn load_certified_key(
    cert_path: &Path,
    key_path: &Path,
    provider: &CryptoProvider,
) -> anyhow::Result<Arc<CertifiedKey>> {
    let cert_file = &mut BufReader::new(File::open(cert_path)?);
    let key_file = &mut BufReader::new(File::open(key_path)?);
    let cert_chain: Vec<CertificateDer<'static>> = certs(cert_file)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            anyhow::anyhow!("Failed to retrieve certificates from the certificate file")
        })?;
    let key = private_key(key_file)
        .map_err(|_| anyhow::anyhow!("Failed to retrieve private key from the key file"))?
        .ok_or_else(|| anyhow::anyhow!("No private key found in the key file"))?;
    let signing_key = provider
        .key_provider
        .load_private_key(key)
        .map_err(|error| anyhow::anyhow!("Failed to load TLS private key: {error}"))?;
    Ok(Arc::new(CertifiedKey::new(cert_chain, signing_key)))
}

/// Build a client certificate verifier from a client CA bundle file, trusting its certificates as
/// roots for client authentication.
fn build_client_verifier(
    ca_path: &Path,
    provider: &Arc<CryptoProvider>,
) -> anyhow::Result<Arc<dyn ClientCertVerifier>> {
    let ca_file = &mut BufReader::new(File::open(ca_path)?);
    let client_certs = certs(ca_file)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| anyhow::anyhow!("Failed to retrieve certificates from client CA file"))?;
    if client_certs.is_empty() {
        anyhow::bail!("No certificates found in the client CA file");
    }

    let mut roots = RootCertStore::empty();
    let (valid, invalid) = roots.add_parsable_certificates(client_certs);
    if valid == 0 {
        anyhow::bail!("No valid certificates found in the client CA file");
    }
    if invalid > 0 {
        tracing::warn!(
            ignored = invalid,
            "Some certificates from the client CA file were ignored"
        );
    }

    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|error| {
            anyhow::anyhow!("Failed to configure client certificate verifier: {error}")
        })
}

/// Inputs shared with the background watcher.
struct WatchContext {
    cert_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: Option<PathBuf>,
    provider: Arc<CryptoProvider>,
    resolver: Arc<ReloadableCertResolver>,
    verifier: Option<Arc<ReloadableClientCertVerifier>>,
    targets: Vec<PathBuf>,
}

/// Spawn the certificate watch loop on a background OS thread.
///
/// A plain OS thread is used because the watch blocks in `inotify` reads and the reload path is
/// fully synchronous/blocking.
#[cfg(target_os = "linux")]
fn spawn_cert_watcher(ctx: WatchContext) {
    std::thread::Builder::new()
        .name("tls-cert-watch".into())
        .spawn(move || ctx.watch())
        .ok();
}

#[cfg(not(target_os = "linux"))]
fn spawn_cert_watcher(_ctx: WatchContext) {
    tracing::warn!(
        "TLS certificate watch is only supported on Linux; automatic reload is disabled"
    );
}

/// Last-modified time of a file, used to skip redundant reloads.
#[cfg(target_os = "linux")]
fn modified(path: &Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Reload the server certificate into the resolver, but only when the files actually changed, so
/// duplicate or spurious watch events are no-ops.
///
/// Returns `Ok(true)` when the certificate was reloaded, `Ok(false)` when the files were unchanged
/// since the last reload, and `Err` when the modification times cannot be read or the certificate
/// material fails to load (a rotation can briefly leave the files inconsistent; the next event
/// retries).
#[cfg(target_os = "linux")]
fn maybe_reload_cert(
    ctx: &WatchContext,
    last: &mut Option<(std::time::SystemTime, std::time::SystemTime)>,
) -> anyhow::Result<bool> {
    let current = modified(&ctx.cert_path)
        .zip(modified(&ctx.key_path))
        .ok_or_else(|| {
            anyhow::anyhow!("Failed to read modification times for TLS certificate/key files")
        })?;
    if *last == Some(current) {
        return Ok(false);
    }
    let certified_key = load_certified_key(&ctx.cert_path, &ctx.key_path, &ctx.provider)?;
    ctx.resolver.store(certified_key);
    *last = Some(current);
    Ok(true)
}

/// Reload the client CA verifier, but only when the client CA file actually changed.
///
/// Returns `Ok(true)` when the verifier was reloaded, `Ok(false)` when the file was unchanged (or
/// client authentication is disabled), and `Err` when the modification time cannot be read or the
/// CA bundle fails to load.
#[cfg(target_os = "linux")]
fn maybe_reload_verifier(
    ctx: &WatchContext,
    last: &mut Option<std::time::SystemTime>,
) -> anyhow::Result<bool> {
    let (Some(ca_path), Some(verifier)) = (&ctx.client_ca_path, &ctx.verifier) else {
        return Ok(false);
    };
    let current = modified(ca_path)
        .ok_or_else(|| anyhow::anyhow!("Failed to read modification time for client CA file"))?;
    if *last == Some(current) {
        return Ok(false);
    }
    let inner = build_client_verifier(ca_path, &ctx.provider)?;
    verifier.store(inner);
    *last = Some(current);
    Ok(true)
}

impl WatchContext {
    /// Filesystem paths to watch for certificate changes.
    ///
    /// If all certs share a parent, watch that directory. This catches atomic
    /// symlink-swap rotations where individual file symlinks may not emit events.
    #[cfg(target_os = "linux")]
    fn paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.cert_path.clone(), self.key_path.clone()];
        if let Some(ca_path) = &self.client_ca_path {
            paths.push(ca_path.clone());
        }
        if paths.is_empty() || std::env::var("KUBERNETES_SERVICE_HOST").is_err() {
            return paths;
        }
        let mut parents = paths.iter().map(|path| path.parent().map(PathBuf::from));
        if let Some(Some(first)) = parents.next() {
            if parents.all(|parent| parent.as_ref() == Some(&first)) {
                return vec![first];
            }
        }
        paths
    }

    // Reload both the server certificate and the client CA verifier, returning `Ok(true)` if
    // either was actually swapped, `Ok(false)` if nothing had changed, and `Err` on failure.
    fn reload_all_(
        &self,
        last_cert: &mut Option<(std::time::SystemTime, std::time::SystemTime)>,
        last_ca: &mut Option<std::time::SystemTime>,
    ) -> anyhow::Result<bool> {
        let cert_changed = maybe_reload_cert(self, last_cert)?;
        let verifier_changed = maybe_reload_verifier(self, last_ca)?;
        Ok(cert_changed || verifier_changed)
    }
    fn reload_all(
        &self,
        last_cert: &mut Option<(std::time::SystemTime, std::time::SystemTime)>,
        last_ca: &mut Option<std::time::SystemTime>,
    ) {
        let targets = &self.targets;
        match self.reload_all_(last_cert, last_ca) {
            Ok(true) => {
                tracing::info!(?targets, "Reloaded TLS certificates");
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(?targets, %error, "Failed to reload TLS certificates");
            }
        }
    }

    /// Watch the certificate files and reload after changes.
    ///
    /// The parent directories are watched (rather than the files) so atomic symlink-swap rotations,
    /// as used by Kubernetes secret mounts, are observed even when the individual file symlinks do
    /// not emit events. Fingerprint guards collapse the batch of events a single rotation produces
    /// into one reload each. When inotify cannot be armed, this falls back to a polling reload on a
    /// timer.
    #[cfg(target_os = "linux")]
    fn watch(mut self) {
        use inotify::{Inotify, WatchMask};
        use std::time::Duration;

        /// The polling interval used whenever the inotify watch is unavailable.
        const POLL_INTERVAL: Duration = Duration::from_secs(60);

        self.targets = self.paths();

        let mut last_cert = modified(&self.cert_path).zip(modified(&self.key_path));
        let mut last_ca = self.client_ca_path.as_deref().and_then(modified);

        let mut inotify = loop {
            match Inotify::init() {
                Ok(inotify) => break inotify,
                Err(error) => {
                    tracing::warn!(%error, "Failed to initialise inotify for TLS watch; polling for changes");
                    std::thread::sleep(POLL_INTERVAL);
                    if let Err(error) = self.reload_all_(&mut last_cert, &mut last_ca) {
                        tracing::warn!(%error, "Failed to reload TLS certificates during polling fallback");
                    }
                }
            }
        };

        let mask = WatchMask::MODIFY | WatchMask::MOVED_TO;
        let mut buffer = [0u8; 4096];
        'outer: loop {
            for target in &self.targets {
                if let Err(error) = inotify.watches().add(target, mask) {
                    tracing::warn!(%error, path = %target.display(), "Failed to watch TLS path; will retry");
                    std::thread::sleep(POLL_INTERVAL);
                    if let Err(error) = self.reload_all_(&mut last_cert, &mut last_ca) {
                        tracing::warn!(%error, "Failed to reload TLS certificates during polling fallback");
                    }
                    continue 'outer;
                }
            }

            // Catch up on any change that happened before the watches were (re-)armed, since
            // such changes emit no events; the fingerprint guards make this a no-op otherwise.
            self.reload_all(&mut last_cert, &mut last_ca);

            loop {
                // Blocks until an event is available, then returns the whole batch of
                // currently-queued events in one read.
                let events = match inotify.read_events_blocking(&mut buffer) {
                    Ok(events) => events,
                    Err(error) => {
                        tracing::warn!(%error, "TLS watch read error; re-arming");
                        break;
                    }
                };

                tracing::debug!(targets = ?self.targets, "TLS watch events received");

                // The watch descriptor is only dropped when the watched inode itself is
                // removed (delivered as `IGNORED`); in-directory rotations keep the inode
                // and the watch keeps reporting `MOVED_TO`/`MODIFY`.
                let ignored = events
                    .into_iter()
                    .any(|event| event.mask.contains(inotify::EventMask::IGNORED));

                self.reload_all(&mut last_cert, &mut last_ca);

                if ignored {
                    break;
                }
            }

            // Brief settle time before re-arming.
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
