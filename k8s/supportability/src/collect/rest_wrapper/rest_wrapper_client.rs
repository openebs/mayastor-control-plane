use once_cell::sync::OnceCell;
use openapi::clients::{
    self,
    tower::{self},
};
use std::ops::Deref;

#[derive(Debug)]
pub enum RestClientError {
    ConfigError(tower::configuration::Error),
}

impl From<tower::configuration::Error> for RestClientError {
    fn from(e: tower::configuration::Error) -> Self {
        RestClientError::ConfigError(e)
    }
}

#[derive(Debug, Clone)]
pub struct RestClient {
    client: clients::tower::ApiClient,
}

impl Deref for RestClient {
    type Target = clients::tower::ApiClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

static REST_CLIENT: OnceCell<RestClient> = OnceCell::new();

impl RestClient {
    pub fn new(time_duration: std::time::Duration) -> Result<&'static Self, RestClientError> {
        let url = plugin::rest_wrapper::RestClient::get_or_panic().url();
        let cfg = clients::tower::Configuration::new(url.clone(), time_duration, None, None, true)?;
        REST_CLIENT.get_or_init(|| RestClient {
            client: clients::tower::ApiClient::new(cfg),
        });
        Ok(Self::get_rest_client())
    }

    pub fn get_rest_client() -> &'static Self {
        REST_CLIENT.get().unwrap()
    }
}
