#![allow(clippy::field_reassign_with_default)]
//! Version 0 of the URI's
//! Ex: /v0/nodes

pub mod block_devices;
pub mod children;
pub mod jsongrpc;
pub mod nexuses;
pub mod nodes;
pub mod pools;
pub mod replicas;
pub mod specs;
pub mod states;
pub mod swagger_ui;
pub mod volumes;
pub mod watches;

use actix_service::ServiceFactory;
use actix_web::{
    dev::{MessageBody, ServiceRequest, ServiceResponse},
    web::{self, Json},
    FromRequest, HttpRequest,
};
use futures::future::Ready;
use serde::Deserialize;

use crate::authentication::authenticate;
pub use common_lib::types::v0::openapi::{apis::RestError, models::RestJsonError};
use mbus_api::{ReplyError, ReplyErrorKind, ResourceKind};
use rest_client::versions::v0::*;

fn version() -> String {
    "v0".into()
}
fn spec_uri() -> String {
    format!("/{}/api/spec", version())
}

pub(crate) struct RestApi {}

fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    apis::configure::<RestApi, BearerToken>(cfg);
    // todo: remove when the /states is added to the spec
    states::configure(cfg);
}

fn json_error(err: impl std::fmt::Display, _req: &actix_web::HttpRequest) -> actix_web::Error {
    RestError::from(ReplyError {
        kind: ReplyErrorKind::DeserializeReq,
        resource: ResourceKind::Unknown,
        source: "".to_string(),
        extra: err.to_string(),
    })
    .into()
}

pub(super) fn configure_api<T, B>(api: actix_web::App<T, B>) -> actix_web::App<T, B>
where
    B: MessageBody,
    T: ServiceFactory<
        Config = (),
        Request = ServiceRequest,
        Response = ServiceResponse<B>,
        Error = actix_web::Error,
        InitError = (),
    >,
{
    api.configure(swagger_ui::configure).service(
        // any /v0 services must either live within this scope or be
        // declared beforehand
        web::scope("/v0")
            .app_data(web::PathConfig::default().error_handler(|e, r| json_error(e, r)))
            .app_data(web::JsonConfig::default().error_handler(|e, r| json_error(e, r)))
            .app_data(web::QueryConfig::default().error_handler(|e, r| json_error(e, r)))
            .configure(configure),
    )
}

#[derive(Deserialize)]
pub struct BearerToken;

impl FromRequest for BearerToken {
    type Error = RestError<RestJsonError>;
    type Future = Ready<Result<Self, Self::Error>>;
    type Config = ();

    fn from_request(req: &HttpRequest, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        futures::future::ready(authenticate(req).map(|_| Self {}).map_err(|auth_error| {
            RestError::from(ReplyError {
                kind: ReplyErrorKind::Unauthorized,
                resource: ResourceKind::Unknown,
                source: req.uri().to_string(),
                extra: auth_error.to_string(),
            })
        }))
    }
}
