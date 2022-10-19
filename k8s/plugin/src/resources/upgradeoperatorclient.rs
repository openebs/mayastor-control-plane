#[derive(Debug)]
pub(crate) struct UpgradeOperatorClient {
    /// Address of UpgradeOperatorClient service
    pub uri: String,
    /// UpgradeOperatorClient client
    pub inner_client: kube_proxy::UpgradeOperatorClient,
    // Endpoint of upgrade operator service
    pub service_endpoint: String,
}

use tower::{util::BoxService, Service, ServiceExt};

/// Upgrade endpoint
const UPGRADE_OPERATOR_END_POINT: &str = "/upgrade";

impl UpgradeOperatorClient {
    /// Instantiate new instance of Http UpgradeOperatorClient client
    pub(crate) async fn new(
        uri: Option<String>,
        kube_config_path: Option<std::path::PathBuf>,
        namespace: String,
        timeout: humantime::Duration,
    ) -> Option<Self> {
        let (uri, client) = match uri {
            None => {
                let (uri, svc) = match kube_proxy::ConfigBuilder::default_upgrade()
                    .with_kube_config(kube_config_path)
                    .with_target_mod(|t| t.with_namespace(namespace))
                    .build()
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        println!("Failed to create upgrade operator client {:?}", error);
                        return None;
                    }
                };
                (uri.to_string(), svc)
            }
            Some(uri) => {
                let mut connector = hyper::client::HttpConnector::new();
                connector.set_connect_timeout(Some(*timeout));
                let client = hyper::Client::builder()
                    .http2_keep_alive_timeout(*timeout)
                    .http2_keep_alive_interval(*timeout / 2)
                    .build(connector);
                let service = tower::ServiceBuilder::new()
                    .timeout(*timeout)
                    .service(client);
                (uri, BoxService::new(service))
            }
        };

        Some(UpgradeOperatorClient {
            uri,
            inner_client: client,
            service_endpoint: UPGRADE_OPERATOR_END_POINT.to_string(),
        })
    }

    pub async fn apply_upgrade(&mut self) -> Result<Option<Vec<String>>, UpgradeClientError> {
        let request_str = format!("{}{}", self.uri.clone(), self.service_endpoint,);
        let request = http::Request::builder()
            .method("PUT")
            .uri(&request_str)
            .body(hyper::body::Body::empty())
            .unwrap();
        let response = self
            .inner_client
            .ready()
            .await?
            .call(request)
            .await
            .unwrap();
        if !response.status().is_success() {
            let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
            let text = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();
            return Err(UpgradeClientError::Response(text));
        } else {
            println!("{:?}", response.body());
        }
        Ok(None)
    }

    pub async fn get_upgrade(&mut self) -> Result<Option<Vec<String>>, UpgradeClientError> {
        let request_str = format!("{}{}", self.uri.clone(), self.service_endpoint,);
        let request = http::Request::builder()
            .method("GET")
            .uri(&request_str)
            .body(hyper::body::Body::empty())
            .unwrap();
        let response = self
            .inner_client
            .ready()
            .await?
            .call(request)
            .await
            .unwrap();
        println!(" Response {:?}", response);
        if !response.status().is_success() {
            let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
            let text = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();
            return Err(UpgradeClientError::Response(text));
        } else {
            println!("{:?}", response.body());
        }
        Ok(None)
    }
}

/// Possible errors can occur while interacting with Loki service
#[derive(Debug)]
pub(crate) enum UpgradeClientError {
    Request(http::Error),
    Response(String),
    Tower(tower::BoxError),
    Serde(serde_json::Error),
    Hyper(hyper::Error),
    IOError(std::io::Error),
}

impl From<http::Error> for UpgradeClientError {
    fn from(e: http::Error) -> UpgradeClientError {
        UpgradeClientError::Request(e)
    }
}
impl From<tower::BoxError> for UpgradeClientError {
    fn from(e: tower::BoxError) -> UpgradeClientError {
        UpgradeClientError::Tower(e)
    }
}
impl From<serde_json::Error> for UpgradeClientError {
    fn from(e: serde_json::Error) -> UpgradeClientError {
        UpgradeClientError::Serde(e)
    }
}
impl From<hyper::Error> for UpgradeClientError {
    fn from(e: hyper::Error) -> UpgradeClientError {
        UpgradeClientError::Hyper(e)
    }
}
impl From<std::io::Error> for UpgradeClientError {
    fn from(e: std::io::Error) -> UpgradeClientError {
        UpgradeClientError::IOError(e)
    }
}
