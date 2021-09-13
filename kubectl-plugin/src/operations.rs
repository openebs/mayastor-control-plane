use crate::resources::{GetResources, ScaleResources, OutputFormat};
use async_trait::async_trait;
use structopt::StructOpt;

/// The types of operations that are supported.
#[derive(StructOpt, Debug)]
pub(crate) enum Operations {
    /// 'Get' resources.
    Get(GetResources),
    /// 'Scale' resources.
    Scale(ScaleResources),
}

/// List trait.
/// To be implemented by resources which support the 'list' operation.
#[async_trait(?Send)]
pub trait List {
    async fn list(output: &OutputFormat);
}

/// Get trait.
/// To be implemented by resources which support the 'get' operation.
#[async_trait(?Send)]
pub trait Get {
    type ID;
    async fn get(id: &Self::ID, output: &OutputFormat);
}

/// Scale trait.
/// To be implemented by resources which support the 'scale' operation.
#[async_trait(?Send)]
pub trait Scale {
    type ID;
    async fn scale(id: &Self::ID, replica_count: u8, output: &OutputFormat);
}
