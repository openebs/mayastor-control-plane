use once_cell::sync::OnceCell;
use openapi::clients::{
    self,
    tower::{self, Url},
};
use std::{env, path::Path};
use yaml_rust::YamlLoader;

#[derive(Debug)]
pub enum RestClientError {
    EnvError(env::VarError),
    UrlNotExist,
    YamlEmitError(yaml_rust::emitter::EmitError),
    ConfigError(tower::configuration::Error),
    IOError(std::io::Error),
    CustomError(String),
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

#[derive(Debug, Clone)]
pub struct RestClient {
    pub client: clients::tower::ApiClient,
}

static REST_CLIENT: OnceCell<RestClient> = OnceCell::new();

impl RestClient {
    pub fn new(
        opt_url: Option<Url>,
        time_duration: std::time::Duration,
    ) -> Result<&'static Self, RestClientError> {
        let mut url = match opt_url {
            Some(url) => url,
            None => Self::get_rest_url()?,
        };
        if url.port().is_none() {
            url.set_port(Some(30011))
                .expect("Failed to set REST client port");
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

    fn get_rest_url() -> Result<Url, RestClientError> {
        // TODO: Remove
        println!("Fetch URL from Kube configuration file");
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

        match file_path {
            Some(file) => {
                let cfg_str = std::fs::read_to_string(file)?;
                let cfg_yaml = &YamlLoader::load_from_str(&cfg_str).unwrap()[0];
                let master_ip = cfg_yaml["clusters"][0]["cluster"]["server"]
                    .as_str()
                    .ok_or_else(|| {
                        RestClientError::CustomError(
                            "Failed to convert IP of master node to string".to_string(),
                        )
                    })?;
                let mut url = Url::parse(master_ip).expect("Failed to parse URL");
                url.set_port(None).map_err(|e| {
                    RestClientError::CustomError(format!("Failed to unset port {:?}", e))
                })?;
                url.set_scheme("http").map_err(|e| {
                    RestClientError::CustomError(format!(
                        "Failed to set REST client scheme, Error: {:?}",
                        e
                    ))
                })?;
                Ok(url)
            }
            None => Err(RestClientError::UrlNotExist),
        }
    }
}
