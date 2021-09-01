use once_cell::sync::OnceCell;
use reqwest::{Client, Error, Response, Url};

static REST_CLIENT: OnceCell<RestClient> = OnceCell::new();

/// REST client
pub struct RestClient {
    /// Address of the rest server. This acts as the base url for all requests.
    base: Url,
    /// Client used to make REST requests.
    client: Client,
}

impl RestClient {
    /// Initialise the REST client.
    pub fn init(endpoint: &Url) {
        if !Self::valid_endpoint(endpoint) {
            return;
        }

        let base = endpoint.join("/v0/").expect("Failed to create base URL.");
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("Failed to build REST client");
        REST_CLIENT.get_or_init(|| Self { base, client });
    }

    /// Returns the base url for all requests.
    pub fn http_base_url() -> Url {
        Self::rest_client().base.clone()
    }

    /// Issue the 'GET' REST request, returning the result of the operation.
    pub async fn get_request(url: &Url) -> Result<Response, Error> {
        Self::rest_client().client.get(url.clone()).send().await
    }

    /// Issue the 'PUT' REST request, returning the result of the operation.
    pub async fn put_request(url: &Url) -> Result<Response, Error> {
        Self::rest_client().client.put(url.clone()).send().await
    }

    /// Helper function to get the rest client.
    fn rest_client() -> &'static RestClient {
        REST_CLIENT
            .get()
            .expect("REST client should be initialised before use.")
    }

    /// Validate that the endpoint is in the correct form.
    fn valid_endpoint(endpoint: &Url) -> bool {
        // TODO: Add support for HTTPS.
        if endpoint.scheme() != "http" {
            println!(
                "{} unsupported. Only HTTP is currently supported.",
                endpoint.scheme()
            );
            return false;
        } else if endpoint.port().is_none() {
            println!("A port number must be supplied for the REST endpoint.");
            return false;
        }
        true
    }
}
