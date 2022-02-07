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

use crate::authentication::authenticate;
use actix_service::ServiceFactory;
use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    web, FromRequest, HttpRequest,
};
pub use common_lib::{
    types::v0::openapi::{
        apis::actix_server::{Body, Path, Query, RestError},
        models::RestJsonError,
    },
    IntoVec,
};
use futures::future::Ready;
use grpc::client::CoreClient;
use mbus_api::{ReplyError, ReplyErrorKind, ResourceKind};
use once_cell::sync::OnceCell;
use rest_client::versions::v0::*;
use serde::Deserialize;

/// once cell static variable to store the grpc client and initialise
/// once at startup
pub static CORE_CLIENT: OnceCell<CoreClient> = OnceCell::new();

fn version() -> String {
    "v0".into()
}
fn spec_uri() -> String {
    format!("/{}/api/spec", version())
}

pub(crate) struct RestApi {}

fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    apis::actix_server::configure::<RestApi, BearerToken>(cfg);
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

pub(super) fn configure_api<T, B>(api: actix_web::App<T>) -> actix_web::App<T>
where
    B: MessageBody,
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
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
