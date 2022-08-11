use crate::{collect::utils::write_to_log_file, log};
use chrono::Utc;
use hyper::body::Buf;
use serde::{Deserialize, Serialize};
use std::{io::Write, path::PathBuf};
use tower::{util::BoxService, Service, ServiceExt};

/// Loki endpoint to query for logs
const ENDPOINT: &str = "/loki/api/v1/query_range";

const SERVICE_NAME: &str = "loki";

/// Possible errors can occur while interacting with Loki service
#[derive(Debug)]
pub(crate) enum LokiError {
    Request(http::Error),
    Response(String),
    Tower(tower::BoxError),
    Serde(serde_json::Error),
    Hyper(hyper::Error),
    IOError(std::io::Error),
}

impl From<http::Error> for LokiError {
    fn from(e: http::Error) -> LokiError {
        LokiError::Request(e)
    }
}
impl From<tower::BoxError> for LokiError {
    fn from(e: tower::BoxError) -> LokiError {
        LokiError::Tower(e)
    }
}
impl From<serde_json::Error> for LokiError {
    fn from(e: serde_json::Error) -> LokiError {
        LokiError::Serde(e)
    }
}
impl From<hyper::Error> for LokiError {
    fn from(e: hyper::Error) -> LokiError {
        LokiError::Hyper(e)
    }
}
impl From<std::io::Error> for LokiError {
    fn from(e: std::io::Error) -> LokiError {
        LokiError::IOError(e)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct StreamMetaData {
    #[serde(rename = "hostname")]
    host_name: String,
    #[serde(rename = "pod")]
    pod_name: String,
    #[serde(rename = "container")]
    container_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct StreamContent {
    #[serde(rename = "stream")]
    stream_metadata: StreamMetaData,
    values: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Data {
    result: Vec<StreamContent>,
}

// Response structure obtained from Loki after making http request
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LokiResponse {
    status: String,
    data: Data,
}

type SinceTime = u128;

impl LokiResponse {
    // fetch last stream log epoch timestamp in nanoseconds
    fn get_last_stream_unix_time(&self) -> SinceTime {
        let unix_time = match self.data.result.last() {
            Some(last_stream) => last_stream
                .values
                .last()
                .unwrap_or(&vec![])
                .get(0)
                .unwrap_or(&"0".to_string())
                .parse::<SinceTime>()
                .unwrap_or(0),
            None => {
                return 0;
            }
        };
        unix_time
    }
}

// Determines the sort order of logs
#[derive(Debug, Clone)]
enum LogDirection {
    Forward,
}

impl LogDirection {
    fn as_string(&self) -> String {
        match self {
            LogDirection::Forward => "forward".to_string(),
        }
    }
}

/// Http client to interact with Loki (a log management system)
/// to fetch historical log information
#[derive(Debug)]
pub(crate) struct LokiClient {
    /// Address of Loki service
    uri: String,
    /// Loki client
    inner_client: kube_proxy::LokiClient,
    /// Endpoint of Loki logs service
    logs_endpoint: String,
    /// Defines period from which logs needs to collect
    since: SinceTime,
    /// Determines the sort order of logs. Supported values are "forward" or "backward".
    /// Defaults to forward
    direction: LogDirection,
    /// maximum number of entries to return on one http call
    limit: u64,
}

impl LokiClient {
    /// Instantiate new instance of Http Loki client
    pub(crate) async fn new(
        uri: Option<String>,
        kube_config_path: Option<std::path::PathBuf>,
        namespace: String,
        since: humantime::Duration,
        timeout: humantime::Duration,
    ) -> Option<Self> {
        let (uri, client) = match uri {
            None => {
                let (uri, svc) = match kube_proxy::ConfigBuilder::default_loki()
                    .with_kube_config(kube_config_path)
                    .with_target_mod(|t| t.with_namespace(namespace))
                    .build()
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        log(format!(
                            "Failed to create loki client ({:?}). Continuing...",
                            error
                        ));
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

        Some(LokiClient {
            uri,
            inner_client: client,
            since: get_epoch_unix_time(since),
            logs_endpoint: ENDPOINT.to_string(),
            direction: LogDirection::Forward,
            limit: 3000,
        })
    }

    /// fetch_and_dump_logs will do the following steps:
    /// 1. Creates poller to interact with Loki service based on provided arguments
    ///     1.1. Use poller to fetch all available logs
    ///     1.2. Write fetched logs into file
    ///     Continue above steps till extraction all logs
    pub(crate) async fn fetch_and_dump_logs(
        &mut self,
        label_selector: String,
        container_name: String,
        host_name: Option<String>,
        service_dir: PathBuf,
    ) -> Result<(), LokiError> {
        // Build query params: Convert label selector into Loki supported query field
        // Below snippet convert app=mayastor,openebs.io/storage=mayastor into
        //  app="mayastor",openebs_io_storage="mayastor"(Loki supported values)
        let mut label_filters: String = label_selector
            .split(',')
            .into_iter()
            .map(|key_value_pair| {
                let pairs = key_value_pair.split('=').collect::<Vec<&str>>();
                format!("{}=\"{}\",", pairs[0], pairs[1])
                    .replace('.', "_")
                    .replace('/', "_")
            })
            .collect::<String>();
        if !label_filters.is_empty() {
            label_filters.pop();
        }
        let (file_name, new_query_field) = match host_name {
            Some(host_name) => {
                let file_name = format!("{}-{}-{}.log", host_name, SERVICE_NAME, container_name);
                let new_query_field = format!(
                    "{{{},container=\"{}\",hostname=~\"{}.*\"}}",
                    label_filters, container_name, host_name
                );
                (file_name, new_query_field)
            }
            None => {
                let file_name = format!("{}-{}.log", SERVICE_NAME, container_name);
                let new_query_field =
                    format!("{{{},container=\"{}\"}}", label_filters, container_name);
                (file_name, new_query_field)
            }
        };
        let encoded_query = urlencoding::encode(&new_query_field);
        let query_params = format!(
            "?query={}&limit={}&direction={}",
            encoded_query,
            self.limit,
            self.direction.as_string()
        );

        let mut poller = LokiPoll {
            uri: self.uri.clone(),
            endpoint: self.logs_endpoint.clone(),
            since: self.since,
            query_params,
            next_start_epoch_timestamp: 0,
            client: self,
        };
        let mut is_written = false;
        let file_path = service_dir.join(file_name.clone());
        let mut log_file: std::fs::File = std::fs::File::create(file_path.clone())?;

        loop {
            let result = match poller.poll_next().await {
                Ok(value) => match value {
                    Some(v) => v,
                    None => {
                        break;
                    }
                },
                Err(e) => {
                    if !is_written {
                        if let Err(e) = std::fs::remove_file(file_path) {
                            log(format!(
                                "[Warning] Failed to remove empty historic log file {}",
                                e
                            ));
                        }
                    }
                    write_to_log_file(format!("[Warning] While fetching logs from Loki {:?}", e))?;
                    return Err(e);
                }
            };
            is_written = true;
            for msg in result.iter() {
                write!(log_file, "{}", msg)?;
            }
        }
        Ok(())
    }
}

fn get_epoch_unix_time(since: humantime::Duration) -> SinceTime {
    Utc::now().timestamp_nanos() as SinceTime - since.as_nanos()
}

struct LokiPoll<'a> {
    client: &'a mut LokiClient,
    uri: String,
    endpoint: String,
    since: SinceTime,
    query_params: String,
    next_start_epoch_timestamp: SinceTime,
}

impl<'a> LokiPoll<'a> {
    // poll_next will extract response from Loki service and perform following actions:
    // 1. Get last log epoch timestamp
    // 2. Extract logs from response
    async fn poll_next(&mut self) -> Result<Option<Vec<String>>, LokiError> {
        let mut start_time = self.since;
        if self.next_start_epoch_timestamp != 0 {
            start_time = self.since;
        }
        let request_str = format!(
            "{}{}{}&start={}",
            self.uri, self.endpoint, self.query_params, start_time
        );

        // TODO: Test timeouts when Loki service is dropped unexpectedly
        let request = http::Request::builder()
            .method("GET")
            .uri(&request_str)
            .body(hyper::body::Body::empty())?;

        let response = self.client().ready().await?.call(request).await?;
        if !response.status().is_success() {
            let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
            let text = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();
            return Err(LokiError::Response(text));
        }

        let body = hyper::body::aggregate(response.into_body()).await?;
        let loki_response: LokiResponse = serde_json::from_reader(body.reader())?;

        if loki_response.status == "success" && loki_response.data.result.is_empty() {
            return Ok(None);
        }
        let last_unix_time = loki_response.get_last_stream_unix_time();
        if last_unix_time == 0 {
            return Ok(None);
        }
        // Next time when poll_next is invoked it will continue to fetch logs after last timestamp
        // TODO: Do we need to just add 1 nanosecond instead of 1 mill second?
        self.since = last_unix_time + (1000000);
        let logs = loki_response
            .data
            .result
            .iter()
            .flat_map(|stream| -> Vec<String> {
                stream
                    .values
                    .iter()
                    .map(|value| value.get(1).unwrap_or(&"".to_string()).to_owned())
                    .filter(|val| !val.is_empty())
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<String>>();
        Ok(Some(logs))
    }
    fn client(&mut self) -> &mut kube_proxy::LokiClient {
        &mut self.client.inner_client
    }
}
