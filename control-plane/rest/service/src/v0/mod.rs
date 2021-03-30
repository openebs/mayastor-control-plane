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
pub mod swagger_ui;
pub mod volumes;
pub mod watches;

use rest_client::{versions::v0::*, JsonGeneric, JsonUnit};

use crate::authentication::authenticate;
use actix_service::ServiceFactory;
use actix_web::{
    dev::{MessageBody, ServiceRequest, ServiceResponse},
    web::{self, Json},
    FromRequest,
    HttpRequest,
};
use futures::future::Ready;
use macros::actix::{delete, get, put};
use paperclip::actix::OpenApiExt;
use std::io::Write;
use structopt::StructOpt;
use tracing::info;

use paperclip::actix::Apiv2Security;
use serde::Deserialize;

fn version() -> String {
    "v0".into()
}
fn base_path() -> String {
    format!("/{}", version())
}
fn spec_uri() -> String {
    format!("/{}/api/spec", version())
}
fn get_api() -> paperclip::v2::models::DefaultApiRaw {
    let mut api = paperclip::v2::models::DefaultApiRaw::default();
    api.info.version = version();
    api.info.title = "Mayastor RESTful API".into();
    api.base_path = Some(base_path());
    api
}

fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    nodes::configure(cfg);
    pools::configure(cfg);
    replicas::configure(cfg);
    nexuses::configure(cfg);
    children::configure(cfg);
    volumes::configure(cfg);
    jsongrpc::configure(cfg);
    block_devices::configure(cfg);
    watches::configure(cfg);
}

fn json_error(
    err: impl std::fmt::Display,
    _req: &actix_web::HttpRequest,
) -> actix_web::Error {
    RestError::from(ReplyError {
        kind: ReplyErrorKind::DeserializeReq,
        resource: ResourceKind::Unknown,
        source: "".to_string(),
        extra: err.to_string(),
    })
    .into()
}

pub(super) fn configure_api<T, B>(
    api: actix_web::App<T, B>,
) -> actix_web::App<T, B>
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
    api.configure(swagger_ui::configure)
        .wrap_api_with_spec(get_api())
        .with_json_spec_at(&spec_uri())
        .service(
            // any /v0 services must either live within this scope or be
            // declared beforehand
            paperclip::actix::web::scope("/v0")
                .app_data(
                    actix_web::web::PathConfig::default()
                        .error_handler(|e, r| json_error(e, r)),
                )
                .app_data(
                    actix_web::web::JsonConfig::default()
                        .error_handler(|e, r| json_error(e, r)),
                )
                .app_data(
                    actix_web::web::QueryConfig::default()
                        .error_handler(|e, r| json_error(e, r)),
                )
                .configure(configure),
        )
        .trim_base_path()
        .with_raw_json_spec(|app, spec| {
            if let Some(dir) = super::CliArgs::from_args().output_specs {
                let file = dir.join(&format!("{}_api_spec.json", version()));
                info!("Writing {} to {}", spec_uri(), file.to_string_lossy());
                let mut file = std::fs::File::create(file)
                    .expect("Should create the spec file");
                file.write_all(spec.to_string().as_ref())
                    .expect("Should write the spec to file");
            }
            app
        })
        .build()
}

#[derive(Apiv2Security, Deserialize)]
#[openapi(
    apiKey,
    alias = "JWT",
    in = "header",
    name = "Authorization",
    description = "Use format 'Bearer TOKEN'"
)]
pub struct BearerToken;

impl FromRequest for BearerToken {
    type Error = RestError;
    type Future = Ready<Result<Self, Self::Error>>;
    type Config = ();

    fn from_request(
        req: &HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        futures::future::ready(authenticate(req).map(|_| Self {}).map_err(
            |auth_error| {
                RestError::from(ReplyError {
                    kind: ReplyErrorKind::Unauthorized,
                    resource: ResourceKind::Unknown,
                    source: req.uri().to_string(),
                    extra: auth_error.to_string(),
                })
            },
        ))
    }
}
