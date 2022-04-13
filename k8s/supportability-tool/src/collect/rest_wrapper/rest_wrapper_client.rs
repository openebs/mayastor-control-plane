use once_cell::sync::OnceCell;
use openapi::clients::{
    self,
    tower::{self, Url},
};
use std::{
    env,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum RestClientError {
    EnvError(env::VarError),
    UrlNotExist,
    YamlEmitError(yaml_rust::emitter::EmitError),
    ConfigError(tower::configuration::Error),
    IOError(std::io::Error),
    CustomError(String),
    KubeError(kube::Error),
}

impl From<env::VarError> for RestClientError {
    fn from(e: env::VarError) -> Self {
        RestClientError::EnvError(e)
    }
}

impl From<yaml_rust::emitter::EmitError> for RestClientError {
    fn from(e: yaml_rust::emitter::EmitError) -> Self {
        RestClientError::YamlEmitError(e)
    }
}

impl From<tower::configuration::Error> for RestClientError {
    fn from(e: tower::configuration::Error) -> Self {
        RestClientError::ConfigError(e)
    }
}

impl From<std::io::Error> for RestClientError {
    fn from(e: std::io::Error) -> Self {
        RestClientError::IOError(e)
    }
}

impl From<kube::Error> for RestClientError {
    fn from(e: kube::Error) -> Self {
        RestClientError::KubeError(e)
    }
}

#[derive(Debug, Clone)]
pub struct RestClient {
    pub client: clients::tower::ApiClient,
}

static REST_CLIENT: OnceCell<RestClient> = OnceCell::new();

impl RestClient {
    pub async fn new(
        opt_url: Option<Url>,
        kubeconfig_file: Option<PathBuf>,
        time_duration: std::time::Duration,
    ) -> Result<&'static Self, RestClientError> {
        let mut url = match opt_url {
            Some(url) => url,
            None => Self::get_rest_url(kubeconfig_file).await?,
        };
        if url.port().is_none() {
            url.set_port(Some(30011)).map_err(|e| {
                RestClientError::CustomError(format!(
                    "Failed to set REST client port error: {:?}",
                    e
                ))
            })?;
        }
        let cfg = clients::tower::Configuration::new(url, time_duration, None, None, true)?;
        REST_CLIENT.get_or_init(|| RestClient {
            client: clients::tower::ApiClient::new(cfg),
        });
        Ok(Self::get_rest_client())
    }

    pub fn get_rest_client() -> &'static Self {
        REST_CLIENT.get().unwrap()
    }

    async fn get_rest_url(kubeconfig_file: Option<PathBuf>) -> Result<Url, RestClientError> {
        let file = match kubeconfig_file {
            Some(config_path) => config_path,
            None => {
                let file_path = match env::var("KUBECONFIG") {
                    Ok(value) => Some(value),
                    Err(_) => {
                        let default_path = format!("{}/.kube/config", env::var("HOME")?);
                        match Path::new(&default_path).exists() {
                            true => Some(default_path),
                            false => None,
                        }
                    }
                };
                if file_path.is_none() {
                    return Err(RestClientError::UrlNotExist);
                }
                let mut path = PathBuf::new();
                path.push(file_path.unwrap_or_default());
                path
            }
        };

        // NOTE: Kubeconfig file may hold multiple contexts to communicate
        //       with different kubernetes clusters. We have to pick master
        //       address of current-context config
        let kube_config = kube::config::Kubeconfig::read_from(&file)?;
        let config = kube::Config::from_custom_kubeconfig(kube_config, &Default::default()).await?;
        let server_url = config.cluster_url.host().ok_or_else(|| {
            RestClientError::CustomError("Failed to get master address".to_string())
        })?;
        let mut url = Url::parse(("http://".to_owned() + server_url).as_str()).map_err(|e| {
            RestClientError::CustomError(format!("Failed to parse URL, error: {}", e))
        })?;
        url.set_port(None)
            .map_err(|e| RestClientError::CustomError(format!("Failed to unset port {:?}", e)))?;
        url.set_scheme("http").map_err(|e| {
            RestClientError::CustomError(format!(
                "Failed to set REST client scheme, Error: {:?}",
                e
            ))
        })?;
        Ok(url)
    }
}
