use crate::resources::{GetResources, ListResources, ScaleResources};
use anyhow::Result;
use async_trait::async_trait;
use reqwest::Response;
use structopt::StructOpt;

/// The types of operations that are supported.
#[derive(StructOpt, Debug)]
pub(crate) enum Operations {
    /// 'List' operation.
    List(ListResources),
    /// 'Get' operation.
    Get(GetResources),
    /// 'Scale' operation.
    Scale(ScaleResources),
}

/// List trait.
/// To be implemented by resources which support the 'list' operation.
#[async_trait]
pub trait List {
    async fn list(&self) -> Result<Response>;
}

/// Get trait.
/// To be implemented by resources which support the 'get' operation.
#[async_trait]
pub trait Get {
    async fn get(&self) -> Result<Response>;
}

/// Scale trait.
/// To be implemented by resources which support the 'scale' operation.
#[async_trait]
pub trait Scale {
    async fn scale(&self) -> Result<Response>;
}
