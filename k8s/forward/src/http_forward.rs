use crate::{
    pod_selection::{AnyReady, PodSelection},
    vx::{Pod, Service},
};
use hyper::{body, http::uri::Scheme, Response};
use kube::{
    api::{Api, ListParams},
    ResourceExt,
};
use std::{future::Future, pin::Pin};

/// Used to retrieve the `hyper::Uri` that can be used to proxy with the kubeapi server.
/// This uri may then be used with `HttpProxy` which is a `tower::Service`.
/// # Example
/// ```ignore
/// let selector = kube_forward::TargetSelector::svc_label("app", "api-rest");
/// let target = kube_forward::Target::new(selector, "http", "mayastor");
/// let hf = kube_forward::HttpForward::new(target, None).await?;
///
/// let uri = hf.uri().await?;
/// tracing::info!(%uri, "generated kube-api");
/// ```
#[derive(Clone)]
pub struct HttpForward {
    target: crate::Target,
    pod_api: Api<Pod>,
    svc_api: Api<Service>,
    scheme: Scheme,
}

impl HttpForward {
    /// Return a new `Self`.
    /// # Arguments
    /// * `target` - the target we'll forward to
    pub async fn new<SO: Into<Option<Scheme>>>(
        target: crate::Target,
        scheme: SO,
    ) -> anyhow::Result<Self> {
        let client = kube::Client::try_default().await?;
        let namespace = target.namespace.name_any();

        Ok(Self {
            target,
            pod_api: Api::namespaced(client.clone(), &namespace),
            svc_api: Api::namespaced(client, &namespace),
            scheme: scheme.into().unwrap_or(Scheme::HTTP),
        })
    }

    /// Returns the `hyper::Uri` that can be used to proxy with the kubeapi server.
    pub async fn uri(self) -> anyhow::Result<hyper::Uri> {
        let target = self.finder().find(&self.target).await?;
        let uri = hyper::Uri::try_from(target.with_scheme(self.scheme))?;
        tracing::info!(%uri, "generated kube-api");
        Ok(uri)
    }

    fn finder(&self) -> TargetFinder {
        TargetFinder {
            pod_api: &self.pod_api,
            svc_api: &self.svc_api,
        }
    }
}

/// A `tower::Service` that proxies requests to services/pods via the kubeapi server.
/// The client must connect using the appropriate `hyper::Uri`, which can be easily
/// generated using `HttpForward::uri`.
/// # Example
/// ```ignore
/// let selector = kube_forward::TargetSelector::svc_label("app", "api-rest");
/// let target = kube_forward::Target::new(selector, "http", "mayastor");
/// let pf = kube_forward::HttpForward::new(target, None).await?;
///
/// let uri = pf.uri().await?;
/// tracing::info!(%uri, "generated kube-api");
///
/// let proxy = kube_forward::HttpProxy::try_default().await?;
/// let mut svc = hyper::service::service_fn(|request: hyper::Request<hyper::body::Body>| {
///     let mut proxy = proxy.clone();
///     async move { proxy.call(request).await }
/// });
///
/// let request = hyper::Request::builder()
///     .method("GET")
///     .uri(&format!("{}/v0/nodes", uri))
///     .body(hyper::Body::empty())
///     .unwrap();
///
/// let result = svc.call(request).await?;
/// tracing::info!(?result, "http request complete");
/// ```
#[derive(Clone)]
pub struct HttpProxy {
    client: kube::Client,
}
impl HttpProxy {
    /// Returns a new `HttpProxy` using the provided `kube::Client`.
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }
    /// Tries to return a default `HttpProxy` with a default `kube::Client`.
    pub async fn try_default() -> anyhow::Result<Self> {
        Ok(Self {
            client: kube::Client::try_default().await?,
        })
    }
}

impl hyper::service::Service<hyper::Request<body::Body>> for HttpProxy {
    type Response = Response<body::Body>;
    type Error = kube::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response<body::Body>, kube::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: hyper::Request<body::Body>) -> Self::Future {
        let client = self.client.clone();
        Box::pin(async move {
            let (parts, body) = request.into_parts();

            let body_bytes = body::to_bytes(body).await;
            let bytes = body_bytes.map_err(kube::Error::HyperError)?.to_vec();

            let request = hyper::Request::from_parts(parts, bytes);
            match client.request_text(request).await {
                Ok(r) => Ok(Response::new(body::Body::from(r))),
                Err(error) => match error {
                    // without https://github.com/kube-rs/kube-rs/pull/972 all errors get translated
                    // to a kube-api error type `ErrorResponse` so it's not possible to distinguish
                    // where the error came from, i.e. kubeapi proxy or the target service.
                    kube::Error::Api(response) => {
                        let message =
                            match serde_json::from_str::<serde_json::Value>(&response.message) {
                                // undo the debug print which created response.message
                                Ok(message) => message.as_str().unwrap_or("").to_string(),
                                Err(_) => response.message,
                            };

                        Response::builder()
                            .status(response.code)
                            .body(body::Body::from(message))
                            .map_err(kube::Error::HttpError)
                    }
                    _ => Err(error),
                },
            }
        })
    }
}

/// Finds an `HttpTarget` which is either a pod/service name and port.
#[derive(Clone)]
struct TargetFinder<'a> {
    pod_api: &'a Api<Pod>,
    svc_api: &'a Api<Service>,
}
impl<'a> TargetFinder<'a> {
    /// Finds the `HttpTarget` according to the specified target.
    /// # Arguments
    /// * `target` - the target to be found
    async fn find(&self, target: &crate::Target) -> anyhow::Result<HttpTarget> {
        let pod_api = self.pod_api;
        let svc_api = self.svc_api;

        let target = target.clone();
        let namespace = target.namespace;
        match target.selector {
            crate::TargetSelector::PodName(name) => Ok(HttpTarget::new(
                TargetName::Pod(name),
                target.port,
                namespace,
            )),
            crate::TargetSelector::PodLabel(selector) => {
                let pods = pod_api.list(&Self::pod_params(&selector)).await?;
                let pod = AnyReady {}.select(&pods.items, &selector)?;
                Ok(HttpTarget::new(
                    TargetName::Pod(pod.name_any()),
                    target.port,
                    namespace,
                ))
            }
            crate::TargetSelector::ServiceLabel(selector) => {
                let services = svc_api.list(&Self::svc_params(&selector)).await?;
                let service = match services.items.into_iter().next() {
                    Some(service) => Ok(service),
                    None => Err(anyhow::anyhow!("Service '{}' not found", selector)),
                }?;

                Ok(HttpTarget::new(
                    TargetName::Service(service.name_any()),
                    target.port,
                    namespace,
                ))
            }
        }
    }
    fn pod_params(selector: &str) -> ListParams {
        ListParams::default()
            .labels(selector)
            .fields("status.phase=Running")
    }
    fn svc_params(selector: &str) -> ListParams {
        ListParams::default().labels(selector)
    }
}

enum TargetName {
    Pod(String),
    Service(String),
}

/// A target which is can either be a pod or a service.
/// The port can be specified by name or number.
struct HttpTarget {
    name: TargetName,
    port: crate::Port,
    namespace: crate::NameSpace,
    scheme: Scheme,
}
impl HttpTarget {
    fn new(name: TargetName, port: crate::Port, namespace: crate::NameSpace) -> Self {
        Self {
            name,
            port,
            namespace,
            scheme: Scheme::HTTP,
        }
    }
    fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = scheme;
        self
    }
}

impl TryFrom<HttpTarget> for hyper::Uri {
    type Error = hyper::http::uri::InvalidUri;

    fn try_from(value: HttpTarget) -> Result<Self, Self::Error> {
        let (resource, name) = match value.name {
            TargetName::Pod(name) => ("pods", name),
            TargetName::Service(name) => ("services", name),
        };
        let port = value.port.any();
        let namespace = value.namespace.name_any();
        let scheme = value.scheme;

        hyper::Uri::try_from(format!(
            "/api/v1/namespaces/{namespace}/{resource}/{scheme}:{name}:{port}/proxy"
        ))
    }
}
