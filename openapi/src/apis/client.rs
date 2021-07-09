use super::configuration::Configuration;
use std::{error, fmt, rc::Rc};

#[derive(Clone)]
pub struct ApiClient {
    block_devices_api: Box<dyn crate::apis::block_devices_api_client::BlockDevices>,
    children_api: Box<dyn crate::apis::children_api_client::Children>,
    json_grpc_api: Box<dyn crate::apis::json_grpc_api_client::JsonGrpc>,
    nexuses_api: Box<dyn crate::apis::nexuses_api_client::Nexuses>,
    nodes_api: Box<dyn crate::apis::nodes_api_client::Nodes>,
    pools_api: Box<dyn crate::apis::pools_api_client::Pools>,
    replicas_api: Box<dyn crate::apis::replicas_api_client::Replicas>,
    specs_api: Box<dyn crate::apis::specs_api_client::Specs>,
    volumes_api: Box<dyn crate::apis::volumes_api_client::Volumes>,
    watches_api: Box<dyn crate::apis::watches_api_client::Watches>,
}

impl ApiClient {
    pub fn new(configuration: Configuration) -> ApiClient {
        let rc = Rc::new(configuration);

        ApiClient {
            block_devices_api: Box::new(
                crate::apis::block_devices_api_client::BlockDevicesClient::new(rc.clone()),
            ),
            children_api: Box::new(crate::apis::children_api_client::ChildrenClient::new(
                rc.clone(),
            )),
            json_grpc_api: Box::new(crate::apis::json_grpc_api_client::JsonGrpcClient::new(
                rc.clone(),
            )),
            nexuses_api: Box::new(crate::apis::nexuses_api_client::NexusesClient::new(
                rc.clone(),
            )),
            nodes_api: Box::new(crate::apis::nodes_api_client::NodesClient::new(rc.clone())),
            pools_api: Box::new(crate::apis::pools_api_client::PoolsClient::new(rc.clone())),
            replicas_api: Box::new(crate::apis::replicas_api_client::ReplicasClient::new(
                rc.clone(),
            )),
            specs_api: Box::new(crate::apis::specs_api_client::SpecsClient::new(rc.clone())),
            volumes_api: Box::new(crate::apis::volumes_api_client::VolumesClient::new(
                rc.clone(),
            )),
            watches_api: Box::new(crate::apis::watches_api_client::WatchesClient::new(rc)),
        }
    }

    pub fn block_devices_api(&self) -> &dyn crate::apis::block_devices_api_client::BlockDevices {
        self.block_devices_api.as_ref()
    }
    pub fn children_api(&self) -> &dyn crate::apis::children_api_client::Children {
        self.children_api.as_ref()
    }
    pub fn json_grpc_api(&self) -> &dyn crate::apis::json_grpc_api_client::JsonGrpc {
        self.json_grpc_api.as_ref()
    }
    pub fn nexuses_api(&self) -> &dyn crate::apis::nexuses_api_client::Nexuses {
        self.nexuses_api.as_ref()
    }
    pub fn nodes_api(&self) -> &dyn crate::apis::nodes_api_client::Nodes {
        self.nodes_api.as_ref()
    }
    pub fn pools_api(&self) -> &dyn crate::apis::pools_api_client::Pools {
        self.pools_api.as_ref()
    }
    pub fn replicas_api(&self) -> &dyn crate::apis::replicas_api_client::Replicas {
        self.replicas_api.as_ref()
    }
    pub fn specs_api(&self) -> &dyn crate::apis::specs_api_client::Specs {
        self.specs_api.as_ref()
    }
    pub fn volumes_api(&self) -> &dyn crate::apis::volumes_api_client::Volumes {
        self.volumes_api.as_ref()
    }
    pub fn watches_api(&self) -> &dyn crate::apis::watches_api_client::Watches {
        self.watches_api.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct ResponseContent<T> {
    pub status: awc::http::StatusCode,
    pub error: T,
}

#[derive(Debug, Clone)]
pub struct ResponseContentUnexpected {
    pub status: awc::http::StatusCode,
    pub text: String,
}

#[derive(Debug)]
pub enum Error<T> {
    Request(awc::error::SendRequestError),
    Serde(serde_json::Error),
    SerdeEncoded(serde_urlencoded::ser::Error),
    PayloadError(awc::error::JsonPayloadError),
    Io(std::io::Error),
    ResponseError(ResponseContent<T>),
    ResponseUnexpected(ResponseContentUnexpected),
}

impl<T: fmt::Debug> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (module, e) = match self {
            Error::Request(e) => ("request", e.to_string()),
            Error::Serde(e) => ("serde", e.to_string()),
            Error::SerdeEncoded(e) => ("serde", e.to_string()),
            Error::PayloadError(e) => ("payload", e.to_string()),
            Error::Io(e) => ("IO", e.to_string()),
            Error::ResponseError(e) => (
                "response",
                format!("status code '{}', content: '{:?}'", e.status, e.error),
            ),
            Error::ResponseUnexpected(e) => (
                "response",
                format!("status code '{}', text '{}'", e.status, e.text),
            ),
        };
        write!(f, "error in {}: {}", module, e)
    }
}

impl<T: fmt::Debug> error::Error for Error<T> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(match self {
            Error::Request(e) => e,
            Error::Serde(e) => e,
            Error::SerdeEncoded(e) => e,
            Error::PayloadError(e) => e,
            Error::Io(e) => e,
            Error::ResponseError(_) => return None,
            Error::ResponseUnexpected(_) => return None,
        })
    }
}

impl<T> From<awc::error::SendRequestError> for Error<T> {
    fn from(e: awc::error::SendRequestError) -> Self {
        Error::Request(e)
    }
}

impl<T> From<awc::error::JsonPayloadError> for Error<T> {
    fn from(e: awc::error::JsonPayloadError) -> Self {
        Error::PayloadError(e)
    }
}

impl<T> From<serde_json::Error> for Error<T> {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(e)
    }
}

impl<T> From<serde_urlencoded::ser::Error> for Error<T> {
    fn from(e: serde_urlencoded::ser::Error) -> Self {
        Error::SerdeEncoded(e)
    }
}

impl<T> From<std::io::Error> for Error<T> {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub fn urlencode<T: AsRef<str>>(s: T) -> String {
    ::url::form_urlencoded::byte_serialize(s.as_ref().as_bytes()).collect()
}
