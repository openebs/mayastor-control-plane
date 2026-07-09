use crate::Error;
use anyhow::anyhow;
use openapi::{
    apis::Url,
    clients::tower::{Configuration, Uri},
    tower::client::{configuration::ClientSecurity, hyper},
};
use std::{convert::TryFrom, path::PathBuf};
use tower::{util::BoxService, ServiceExt};

pub use openapi::tower::client::configuration::TlsMode;

/// A builder type for the openapi `Configuration`.
/// The configuration is tailored for a kubernetes proxy using the `kube_forward::HttpProxy`.
/// # Example:
/// ```ignore
/// let config = kube_proxy::ConfigBuilder::default_api_rest()
///     .with_kube_config(kube_config_path.clone())
///     .with_timeout(timeout)
///     .with_target_mod(|t| t.with_namespace(&args.namespace))
///     .with_forwarding(ForwardingProxy::HTTP)
///     .build()
///     .await?;
/// ```
pub struct ConfigBuilder<T> {
    kube_config: Option<PathBuf>,
    context: Option<String>,
    target: kube_forward::Target,
    timeout: Option<std::time::Duration>,
    method: ForwardingProxy,
    scheme: Scheme,
    /// Prefix for the kind of material for auto-discovery (ie: `rest`).
    security_prefix: Option<String>,
    client: Option<kube::Client>,
    builder_target: std::marker::PhantomData<T>,
}

/// Internal for type-state.
pub struct ApiRest {}
/// Internal for type-state.
pub struct Etcd {}
/// Internal for type-state.
pub struct Loki {}

/// The scheme component of the URI.
#[derive(Clone, Debug)]
pub enum Scheme {
    /// HTTP.
    HTTP,
    /// HTTPS with optional explicit client-side security material.
    /// If `ClientSecurity::discover`, security material is discovered from pod secret volumes.
    HTTPS(ClientSecurity),
}
impl Scheme {
    fn name(&self) -> &'static str {
        match self {
            Self::HTTP => "http",
            Self::HTTPS(_) => "https",
        }
    }

    fn client_security(&self) -> Option<&ClientSecurity> {
        match self {
            Self::HTTP => None,
            Self::HTTPS(client_security) => Some(client_security),
        }
    }
}
impl From<&Scheme> for hyper::http::uri::Scheme {
    fn from(value: &Scheme) -> Self {
        match value {
            Scheme::HTTP => hyper::http::uri::Scheme::HTTP,
            Scheme::HTTPS(_) => hyper::http::uri::Scheme::HTTPS,
        }
    }
}
impl From<Scheme> for hyper::http::uri::Scheme {
    fn from(value: Scheme) -> Self {
        Self::from(&value)
    }
}

/// Type of forwarding proxy to use.
#[derive(Clone, Copy)]
pub enum ForwardingProxy {
    /// HTTP via the kube-api proxy.
    HTTP,
    /// TCP via the kube-api port forwarding.
    TCP,
}

impl Default for ConfigBuilder<ApiRest> {
    fn default() -> Self {
        Self {
            target: kube_forward::Target::new(
                kube_forward::TargetSelector::ServiceLabel(utils::API_REST_LABEL.to_string()),
                utils::API_REST_HTTPS_PORT,
                utils::DEFAULT_NAMESPACE,
            ),
            timeout: Some(std::time::Duration::from_secs(5)),
            method: ForwardingProxy::HTTP,
            scheme: Scheme::HTTPS(ClientSecurity::default()),
            kube_config: None,
            context: None,
            security_prefix: None,
            client: None,
            builder_target: std::marker::PhantomData,
        }
    }
}
impl Default for ConfigBuilder<Etcd> {
    fn default() -> Self {
        Self {
            target: kube_forward::Target::new(
                kube_forward::TargetSelector::PodLabel(utils::ETCD_LABEL.to_string()),
                utils::ETCD_PORT,
                utils::DEFAULT_NAMESPACE,
            ),
            timeout: Some(std::time::Duration::from_secs(5)),
            method: ForwardingProxy::TCP,
            scheme: Scheme::HTTP,
            kube_config: None,
            context: None,
            security_prefix: None,
            client: None,
            builder_target: std::marker::PhantomData,
        }
    }
}
impl Default for ConfigBuilder<Loki> {
    fn default() -> Self {
        Self {
            target: kube_forward::Target::new(
                kube_forward::TargetSelector::ServiceLabel(utils::LOKI_LABEL.to_string()),
                utils::LOKI_PORT,
                utils::DEFAULT_NAMESPACE,
            ),
            timeout: Some(std::time::Duration::from_secs(5)),
            method: ForwardingProxy::HTTP,
            scheme: Scheme::HTTP,
            kube_config: None,
            context: None,
            security_prefix: None,
            client: None,
            builder_target: std::marker::PhantomData,
        }
    }
}

impl ConfigBuilder<ApiRest> {
    /// Returns a `Self` with sane defaults for the api-rest.
    pub fn default_api_rest() -> ConfigBuilder<ApiRest> {
        ConfigBuilder::<ApiRest>::default()
    }
}
impl ConfigBuilder<Etcd> {
    /// Returns a `Self` with sane defaults for the etcd.
    pub fn default_etcd() -> ConfigBuilder<Etcd> {
        ConfigBuilder::<Etcd>::default()
    }
}
impl ConfigBuilder<Loki> {
    /// Returns a `Self` with sane defaults for the Loki.
    pub fn default_loki() -> ConfigBuilder<Loki> {
        ConfigBuilder::<Loki>::default()
    }
}

impl<T> ConfigBuilder<T> {
    /// Move self with the following kube_config_path.
    pub fn with_kube_config(mut self, kube_config_path: Option<PathBuf>) -> Self {
        self.kube_config = kube_config_path;
        self
    }
    /// Move self with the following target.
    pub fn with_target(mut self, target: kube_forward::Target) -> Self {
        self.target = target;
        self
    }
    /// Move self with the following context.
    pub fn with_context(mut self, context: Option<String>) -> Self {
        self.context = context;
        self
    }
    /// Move self with the following target closure.
    pub fn with_target_mod(
        mut self,
        modify: impl FnOnce(kube_forward::Target) -> kube_forward::Target,
    ) -> Self {
        self.target = modify(self.target);
        self
    }
    /// Override the prefix used for TLS and JWT auto-discovery.
    pub fn with_secret_prefix(mut self, security_prefix: impl Into<Option<String>>) -> Self {
        self.security_prefix = security_prefix.into();
        self
    }
}

impl ConfigBuilder<ApiRest> {
    /// Move self with the following timeout.
    pub fn with_timeout<TO: Into<Option<std::time::Duration>>>(mut self, timeout: TO) -> Self {
        self.timeout = timeout.into();
        self
    }
    /// Move self with the following forwarding method.
    pub fn with_forwarding(mut self, method: ForwardingProxy) -> Self {
        self.method = method;
        self
    }
    /// Move self with the following connection scheme.
    pub fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = scheme;
        self
    }
    /// Move self with the following security information.
    /// We now default to HTTPS with auto-discovered security, though this is reverted back to
    /// HTTP if the discovered security is none.
    pub fn with_security(mut self, security: ClientSecurity) -> Self {
        self.scheme = Scheme::HTTPS(security);
        self
    }

    async fn build_client(&mut self) -> Result<kube::Client, Error> {
        if let Some(client) = self.client.clone() {
            Ok(client)
        } else {
            let client =
                super::client_from_kubeconfig(self.kube_config.clone(), self.context.clone())
                    .await?;
            self.client = Some(client.clone());
            Ok(client)
        }
    }
    async fn client(&self) -> Result<kube::Client, Error> {
        if let Some(client) = self.client.clone() {
            Ok(client)
        } else {
            let client =
                super::client_from_kubeconfig(self.kube_config.clone(), self.context.clone())
                    .await?;
            Ok(client)
        }
    }

    /// Tries to build a `Configuration` from the current self.
    pub async fn build(mut self) -> Result<Configuration, Error> {
        let mut method = self.method;

        if matches!(method, ForwardingProxy::HTTP) {
            let client = self.build_client().await?;
            let pf = self.port_forward(client);
            if let Scheme::HTTPS(client_security) = &mut self.scheme {
                *client_security = pf.discover_client_security(client_security).await?;

                // kube-apiserver HTTP proxy can only be used when discovered auth is none, auto-tls and no jwt.
                if !matches!(client_security.tls, TlsMode::Auto | TlsMode::None)
                    || client_security.jwt.is_some()
                {
                    method = ForwardingProxy::TCP;
                }
                // If the api-rest is not configured for tls, then switch to HTTP.
                if client_security.tls.is_none() {
                    self.target = self.target.with_port(utils::API_REST_HTTP_PORT);
                    self.scheme = Scheme::HTTP;
                }
            }
        }

        match method {
            ForwardingProxy::HTTP => self.build_http().await,
            ForwardingProxy::TCP => self.build_tcp().await,
        }
    }

    /// Tries to build an HTTP `Configuration` from the current self.
    async fn build_http(self) -> Result<Configuration, Error> {
        let client = self.client().await?;
        let uri = kube_forward::HttpForward::new(
            self.target,
            Some((&self.scheme).into()),
            client.clone(),
        )
        .uri()
        .await?;

        let proxy = kube_forward::HttpProxy::new(client);

        let jwt = match self.scheme.client_security() {
            Some(client_security) => client_security.jwt.clone(),
            None => None,
        };

        let config = Configuration::builder()
            .with_timeout(self.timeout)
            .with_bearer_token(jwt)
            .with_tracing(true)
            .build_with_svc(uri, proxy)
            .map_err(|e| anyhow!("Failed to Create OpenApi config: {e:?}"))?;
        Ok(config)
    }
    /// Tries to build a TCP `Configuration` from the current self.
    async fn build_tcp(self) -> Result<Configuration, Error> {
        let client = self.client().await?;

        let pf = self.port_forward(client);
        let (port, client_security, _handle) = pf
            .secure_port_forward(
                self.scheme
                    .client_security()
                    .unwrap_or(&ClientSecurity::default()),
            )
            .await?;

        let timeout = self
            .timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(5));

        let scheme = self.scheme.name();
        let url = Url::parse(&format!("{scheme}://localhost:{port}"))?;

        Configuration::builder()
            .with_timeout(timeout)
            .with_tracing(true)
            .with_client_security(Some(client_security))
            .build_url(url)
            .map_err(|e| anyhow!("Failed to Create OpenApi config: {:?}", e).into())
    }
    fn port_forward(&self, client: kube::Client) -> kube_forward::PortForward {
        kube_forward::PortForward::new_with_secrets(
            self.target.clone(),
            None,
            client,
            self.security_prefix.clone(),
        )
    }
}

impl ConfigBuilder<Etcd> {
    /// Tries to build a TCP `Configuration` from the current self.
    pub async fn build(self) -> Result<Uri, Error> {
        let client = super::client_from_kubeconfig(self.kube_config, self.context).await?;

        let pf = kube_forward::PortForward::new_with_secrets(
            self.target,
            None,
            client,
            self.security_prefix,
        );

        let (port, _handle) = pf.port_forward().await?;

        let scheme = self.scheme.name();
        let uri = Uri::try_from(&format!("{scheme}://localhost:{port}"))?;
        Ok(uri)
    }
}

/// A loki client which is essentially a boxed `tower::Service`.
pub type LokiClient = BoxService<
    hyper::Request<hyper_body::Body>,
    hyper::Response<hyper_body::Body>,
    tower::BoxError,
>;

impl ConfigBuilder<Loki> {
    /// Move self with the following timeout.
    pub fn with_timeout<TO: Into<Option<std::time::Duration>>>(mut self, timeout: TO) -> Self {
        self.timeout = timeout.into();
        self
    }
    /// Move self with the following forwarding method.
    pub fn with_forwarding(mut self, method: ForwardingProxy) -> Self {
        self.method = method;
        self
    }
    /// Move self with the following connection scheme.
    pub fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = scheme;
        self
    }

    /// Tries to build a `LokiClient` from the current self.
    /// This is simply a boxed `tower::Service` so can be used for any HTTP requests.
    pub async fn build(self) -> Result<(Uri, LokiClient), Error> {
        match self.method {
            ForwardingProxy::HTTP => self.build_http().await,
            ForwardingProxy::TCP => self.build_tcp().await,
        }
    }
    /// Tries to build an HTTP `Configuration` from the current self.
    async fn build_http(self) -> Result<(Uri, LokiClient), Error> {
        let client = super::client_from_kubeconfig(self.kube_config, self.context).await?;

        let uri =
            kube_forward::HttpForward::new(self.target, Some(self.scheme.into()), client.clone())
                .uri()
                .await?;

        let proxy = kube_forward::HttpProxy::new(client);

        let service = tower::ServiceBuilder::new()
            .option_layer(self.timeout.map(tower::timeout::TimeoutLayer::new))
            .service(proxy);
        Ok((uri, LokiClient::new(service)))
    }
    /// Tries to build a TCP `Configuration` from the current self.
    async fn build_tcp(self) -> Result<(Uri, LokiClient), Error> {
        let client = super::client_from_kubeconfig(self.kube_config, self.context).await?;

        let pf = kube_forward::PortForward::new_with_secrets(
            self.target,
            None,
            client,
            self.security_prefix,
        );

        let (port, _handle) = pf.port_forward().await?;

        let keep_alive_timeout = self
            .timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(5));

        let scheme = self.scheme.name();
        let uri = Uri::try_from(&format!("{scheme}://localhost:{port}"))?;

        let service = match &self.scheme {
            Scheme::HTTP => {
                let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
                connector.set_connect_timeout(self.timeout);
                let client = hyper_util::client::legacy::Client::builder(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .http2_keep_alive_timeout(keep_alive_timeout)
                .http2_keep_alive_interval(keep_alive_timeout / 2)
                .build(connector)
                .map_err(tower::BoxError::from)
                .map_response(|r| r.map(hyper_body::Body::wrap_body));
                tower::ServiceBuilder::new()
                    .option_layer(self.timeout.map(tower::timeout::TimeoutLayer::new))
                    .service(client)
            }
            Scheme::HTTPS(_) => {
                unimplemented!()
            }
        };

        Ok((uri, LokiClient::new(service)))
    }
}
