use openapi::clients::tower::{self, Configuration};
use std::ops::Deref;

/// New-Type for a RestClient over the tower openapi client.
#[derive(Debug, Clone)]
pub struct RestClient {
    client: tower::ApiClient,
}

impl Deref for RestClient {
    type Target = tower::ApiClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl RestClient {
    /// Create new Rest Client from the given `Configuration`.
    pub fn new_with_config(config: Configuration) -> RestClient {
        Self {
            client: tower::ApiClient::new(config),
        }
    }
}
