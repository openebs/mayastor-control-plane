use crate::Error;
use anyhow::anyhow;
use openapi::{
    apis::Url,
    clients::tower::{Configuration, Uri},
    tower::client::hyper,
};
use std::{convert::TryFrom, path::PathBuf};
use tower::{util::BoxService, ServiceExt};

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
    jwt: Option<String>,
    method: ForwardingProxy,
    scheme: Scheme,
    builder_target: std::marker::PhantomData<T>,
}

/// Internal for type-state.
pub struct ApiRest {}
/// Internal for type-state.
pub struct Etcd {}
/// Internal for type-state.
pub struct Loki {}

/// The scheme component of the URI.
pub enum Scheme {
    /// HTTP.
    HTTP,
    /// HTTPS with/without Certificate.
    /// todo: What should the certificate format be here?
    HTTPS(Option<String>),
}
impl Scheme {
    fn parts(&self) -> (String, Option<&[u8]>) {
        match self {
            Self::HTTP => ("http".to_string(), None),
            Self::HTTPS(certificate) => (
                "https".to_string(),
                certificate.as_ref().map(|i| i.as_bytes()),
            ),
        }
    }
}
impl From<Scheme> for hyper::http::uri::Scheme {
    fn from(value: Scheme) -> Self {
        match value {
            Scheme::HTTP => hyper::http::uri::Scheme::HTTP,
            Scheme::HTTPS(_) => hyper::http::uri::Scheme::HTTPS,
        }
    }
}

/// Type of forwarding proxy to use.
pub enum ForwardingProxy {
    /// HTTP via the kube-api proxy.
    HTTP,
    /// TCP via the kube-api port forwarding.
    TCP,
}

impl Default for ConfigBuilder<ApiRest> {
    fn default() -> Self {
        Self {
            kube_config: None,
            context: None,
            target: kube_forward::Target::new(
                kube_forward::TargetSelector::ServiceLabel(utils::API_REST_LABEL.to_string()),
                utils::API_REST_HTTP_PORT,
                utils::DEFAULT_NAMESPACE,
            ),
            timeout: Some(std::time::Duration::from_secs(5)),
            jwt: None,
            method: ForwardingProxy::HTTP,
            scheme: Scheme::HTTP,
            builder_target: Default::default(),
        }
    }
}
impl Default for ConfigBuilder<Etcd> {
    fn default() -> Self {
        Self {
            kube_config: None,
            context: None,
            target: kube_forward::Target::new(
                kube_forward::TargetSelector::PodLabel(utils::ETCD_LABEL.to_string()),
                utils::ETCD_PORT,
                utils::DEFAULT_NAMESPACE,
            ),
            timeout: Some(std::time::Duration::from_secs(5)),
            jwt: None,
            method: ForwardingProxy::TCP,
            scheme: Scheme::HTTP,
            builder_target: Default::default(),
        }
    }
}
impl Default for ConfigBuilder<Loki> {
    fn default() -> Self {
        Self {
            kube_config: None,
            context: None,
            target: kube_forward::Target::new(
                kube_forward::TargetSelector::ServiceLabel(utils::LOKI_LABEL.to_string()),
                utils::LOKI_PORT,
                utils::DEFAULT_NAMESPACE,
            ),
            timeout: Some(std::time::Duration::from_secs(5)),
            jwt: None,
            method: ForwardingProxy::HTTP,
            scheme: Scheme::HTTP,
            builder_target: Default::default(),
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

    /// Tries to build a `Configuration` from the current self.
    pub async fn build(self) -> Result<Configuration, Error> {
        match self.method {
            ForwardingProxy::HTTP => self.build_http().await,
            ForwardingProxy::TCP => self.build_tcp().await,
        }
    }
    /// Tries to build an HTTP `Configuration` from the current self.
    async fn build_http(self) -> Result<Configuration, Error> {
        let client = super::client_from_kubeconfig(self.kube_config, self.context).await?;
        let uri =
            kube_forward::HttpForward::new(self.target, Some(self.scheme.into()), client.clone())
                .uri()
                .await?;

        let proxy = kube_forward::HttpProxy::new(client);

        let config = Configuration::builder()
            .with_timeout(self.timeout)
            .with_bearer_token(self.jwt)
            .with_tracing(true)
            .build_with_svc(uri, proxy)
            .map_err(|e| anyhow!("Failed to Create OpenApi config: {:?}", e))?;
        Ok(config)
    }
    /// Tries to build a TCP `Configuration` from the current self.
    async fn build_tcp(self) -> Result<Configuration, Error> {
        let client = super::client_from_kubeconfig(self.kube_config, self.context).await?;

        let pf = kube_forward::PortForward::new(self.target, None, client);

        let (port, _handle) = pf.port_forward().await?;

        let timeout = self
            .timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(5));
        let (scheme, certificate) = self.scheme.parts();
        let url = Url::parse(&format!("{scheme}://localhost:{port}"))?;

        let config = Configuration::builder()
            .with_timeout(timeout)
            .with_bearer_token(self.jwt)
            .with_tracing(true);
        // todo: fix client to receive option...
        match certificate {
            Some(certificate) => config.with_certificate(certificate),
            None => config,
        }
        .build_url(url)
        .map_err(|e| anyhow!("Failed to Create OpenApi config: {:?}", e).into())
    }
}

impl ConfigBuilder<Etcd> {
    /// Tries to build a TCP `Configuration` from the current self.
    pub async fn build(self) -> Result<Uri, Error> {
        let client = super::client_from_kubeconfig(self.kube_config, self.context).await?;

        let pf = kube_forward::PortForward::new(self.target, None, client);

        let (port, _handle) = pf.port_forward().await?;

        let (scheme, _certificate) = self.scheme.parts();
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

        let pf = kube_forward::PortForward::new(self.target, None, client);

        let (port, _handle) = pf.port_forward().await?;

        let keep_alive_timeout = self
            .timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(5));

        let (scheme, _certificate) = self.scheme.parts();
        let uri = Uri::try_from(&format!("{scheme}://localhost:{port}"))?;

        let service = match self.scheme {
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
