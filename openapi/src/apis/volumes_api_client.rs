use crate::apis::{
    client::{Error, ResponseContent, ResponseContentUnexpected},
    configuration,
};
use actix_web_opentelemetry::ClientExt;
use std::rc::Rc;

#[derive(Clone)]
pub struct VolumesClient {
    configuration: Rc<configuration::Configuration>,
}

impl VolumesClient {
    pub fn new(configuration: Rc<configuration::Configuration>) -> Self {
        Self { configuration }
    }
}

#[async_trait::async_trait(?Send)]
#[dyn_clonable::clonable]
pub trait Volumes: Clone {
    async fn del_share(&self, volume_id: &str) -> Result<(), Error<crate::models::RestJsonError>>;
    async fn del_volume(&self, volume_id: &str) -> Result<(), Error<crate::models::RestJsonError>>;
    async fn get_node_volume(
        &self,
        node_id: &str,
        volume_id: &str,
    ) -> Result<crate::models::Volume, Error<crate::models::RestJsonError>>;
    async fn get_node_volumes(
        &self,
        node_id: &str,
    ) -> Result<Vec<crate::models::Volume>, Error<crate::models::RestJsonError>>;
    async fn get_volume(
        &self,
        volume_id: &str,
    ) -> Result<crate::models::Volume, Error<crate::models::RestJsonError>>;
    async fn get_volumes(
        &self,
    ) -> Result<Vec<crate::models::Volume>, Error<crate::models::RestJsonError>>;
    async fn put_volume(
        &self,
        volume_id: &str,
        create_volume_body: crate::models::CreateVolumeBody,
    ) -> Result<crate::models::Volume, Error<crate::models::RestJsonError>>;
    async fn put_volume_share(
        &self,
        volume_id: &str,
        protocol: crate::models::VolumeShareProtocol,
    ) -> Result<String, Error<crate::models::RestJsonError>>;
}

#[async_trait::async_trait(?Send)]
impl Volumes for VolumesClient {
    async fn del_share(&self, volume_id: &str) -> Result<(), Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/volumes{volume_id}/share",
            configuration.base_path,
            volume_id = volume_id.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::DELETE, local_var_uri_str.as_str());

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
    async fn del_volume(&self, volume_id: &str) -> Result<(), Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/volumes/{volume_id}",
            configuration.base_path,
            volume_id = volume_id.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::DELETE, local_var_uri_str.as_str());

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
    async fn get_node_volume(
        &self,
        node_id: &str,
        volume_id: &str,
    ) -> Result<crate::models::Volume, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/nodes/{node_id}/volumes/{volume_id}",
            configuration.base_path,
            node_id = crate::apis::client::urlencode(node_id),
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
            let local_var_content = local_var_resp.json::<crate::models::Volume>().await?;
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
    async fn get_node_volumes(
        &self,
        node_id: &str,
    ) -> Result<Vec<crate::models::Volume>, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/nodes/{node_id}/volumes",
            configuration.base_path,
            node_id = crate::apis::client::urlencode(node_id)
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
            let local_var_content = local_var_resp.json::<Vec<crate::models::Volume>>().await?;
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
    async fn get_volume(
        &self,
        volume_id: &str,
    ) -> Result<crate::models::Volume, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/volumes/{volume_id}",
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
            let local_var_content = local_var_resp.json::<crate::models::Volume>().await?;
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
    async fn get_volumes(
        &self,
    ) -> Result<Vec<crate::models::Volume>, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!("{}/volumes", configuration.base_path);
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
            let local_var_content = local_var_resp.json::<Vec<crate::models::Volume>>().await?;
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
    async fn put_volume(
        &self,
        volume_id: &str,
        create_volume_body: crate::models::CreateVolumeBody,
    ) -> Result<crate::models::Volume, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/volumes/{volume_id}",
            configuration.base_path,
            volume_id = volume_id.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::PUT, local_var_uri_str.as_str());

        if let Some(ref local_var_user_agent) = configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .insert_header((awc::http::header::USER_AGENT, local_var_user_agent.clone()));
        }
        if let Some(ref local_var_token) = configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        let mut local_var_resp = if configuration.trace_requests {
            local_var_req_builder.send_json(&create_volume_body).await
        } else {
            local_var_req_builder
                .trace_request()
                .send_json(&create_volume_body)
                .await
        }?;

        let local_var_status = local_var_resp.status();

        if local_var_status.is_success() {
            let local_var_content = local_var_resp.json::<crate::models::Volume>().await?;
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
    async fn put_volume_share(
        &self,
        volume_id: &str,
        protocol: crate::models::VolumeShareProtocol,
    ) -> Result<String, Error<crate::models::RestJsonError>> {
        let configuration = &self.configuration;
        let local_var_client = &configuration.client;

        let local_var_uri_str = format!(
            "{}/volumes/{volume_id}/share/{protocol}",
            configuration.base_path,
            volume_id = volume_id.to_string(),
            protocol = protocol.to_string()
        );
        let mut local_var_req_builder =
            local_var_client.request(awc::http::Method::PUT, local_var_uri_str.as_str());

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
            let local_var_content = local_var_resp.json::<String>().await?;
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
}
