use crate::apis::{
    client::{Error, ResponseContent, ResponseContentUnexpected},
    configuration,
};
use actix_web_opentelemetry::ClientExt;
use std::rc::Rc;

#[derive(Clone)]
pub struct WatchesClient {
    configuration: Rc<configuration::Configuration>,
}

impl WatchesClient {
    pub fn new(configuration: Rc<configuration::Configuration>) -> Self {
        Self { configuration }
    }
}

#[async_trait::async_trait(?Send)]
#[dyn_clonable::clonable]
pub trait Watches: Clone {
    async fn del_watch_volume(
        &self,
        volume_id: &str,
        callback: &str,
    ) -> Result<(), Error<crate::models::RestJsonError>>;
    async fn get_watch_volume(
        &self,
        volume_id: &str,
    ) -> Result<Vec<crate::models::RestWatch>, Error<crate::models::RestJsonError>>;
    async fn put_watch_volume(
        &self,
        volume_id: &str,
        callback: &str,
    ) -> Result<(), Error<crate::models::RestJsonError>>;
}

#[async_trait::async_trait(?Send)]
impl Watches for WatchesClient {
    async fn del_watch_volume(
        &self,
        volume_id: &str,
        callback: &str,
    ) -> Result<(), Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/watches/volumes/{volume_id}",
            configuration.base_path,
            volume_id = volume_id.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::DELETE, local_var_uri_str.as_str());

        local_var_req_builder =
            local_var_req_builder.query(&[("callback", &callback.to_string())])?;
        if let Some(ref local_var_user_agent) = configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .insert_header((awc::http::header::USER_AGENT, local_var_user_agent.clone()));
        }
        if let Some(ref local_var_token) = configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        let mut local_var_resp = if configuration.trace_requests {
            local_var_req_builder.trace_request().send().await
        } else {
            local_var_req_builder.send().await
        }?;

        let local_var_status = local_var_resp.status();

        if local_var_status.is_success() {
            Ok(())
        } else {
            match local_var_resp.json::<crate::models::RestJsonError>().await {
                Ok(error) => Err(Error::ResponseError(ResponseContent {
                    status: local_var_status,
                    error,
                })),
                Err(_) => Err(Error::ResponseUnexpected(ResponseContentUnexpected {
                    status: local_var_status,
                    text: local_var_resp.json().await?,
                })),
            }
        }
    }
    async fn get_watch_volume(
        &self,
        volume_id: &str,
    ) -> Result<Vec<crate::models::RestWatch>, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/watches/volumes/{volume_id}",
            configuration.base_path,
            volume_id = volume_id.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::GET, local_var_uri_str.as_str());

        if let Some(ref local_var_user_agent) = configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .insert_header((awc::http::header::USER_AGENT, local_var_user_agent.clone()));
        }
        if let Some(ref local_var_token) = configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        let mut local_var_resp = if configuration.trace_requests {
            local_var_req_builder.trace_request().send().await
        } else {
            local_var_req_builder.send().await
        }?;

        let local_var_status = local_var_resp.status();

        if local_var_status.is_success() {
            let local_var_content = local_var_resp
                .json::<Vec<crate::models::RestWatch>>()
                .await?;
            Ok(local_var_content)
        } else {
            match local_var_resp.json::<crate::models::RestJsonError>().await {
                Ok(error) => Err(Error::ResponseError(ResponseContent {
                    status: local_var_status,
                    error,
                })),
                Err(_) => Err(Error::ResponseUnexpected(ResponseContentUnexpected {
                    status: local_var_status,
                    text: local_var_resp.json().await?,
                })),
            }
        }
    }
    async fn put_watch_volume(
        &self,
        volume_id: &str,
        callback: &str,
    ) -> Result<(), Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/watches/volumes/{volume_id}",
            configuration.base_path,
            volume_id = volume_id.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::PUT, local_var_uri_str.as_str());

        local_var_req_builder =
            local_var_req_builder.query(&[("callback", &callback.to_string())])?;
        if let Some(ref local_var_user_agent) = configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .insert_header((awc::http::header::USER_AGENT, local_var_user_agent.clone()));
        }
        if let Some(ref local_var_token) = configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        let mut local_var_resp = if configuration.trace_requests {
            local_var_req_builder.trace_request().send().await
        } else {
            local_var_req_builder.send().await
        }?;

        let local_var_status = local_var_resp.status();

        if local_var_status.is_success() {
            Ok(())
        } else {
            match local_var_resp.json::<crate::models::RestJsonError>().await {
                Ok(error) => Err(Error::ResponseError(ResponseContent {
                    status: local_var_status,
                    error,
                })),
                Err(_) => Err(Error::ResponseUnexpected(ResponseContentUnexpected {
                    status: local_var_status,
                    text: local_var_resp.json().await?,
                })),
            }
        }
    }
}
