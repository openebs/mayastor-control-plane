use crate::resources::{utils, GetResources, ScaleResources};
use async_trait::async_trait;
use structopt::StructOpt;

/// The types of operations that are supported.
#[derive(StructOpt, Debug)]
pub enum Operations {
    /// 'Get' resources.
    Get(GetResources),
    /// 'Scale' resources.
    Scale(ScaleResources),
}

/// List trait.
/// To be implemented by resources which support the 'list' operation.
#[async_trait(?Send)]
pub trait List {
    async fn list(output: &utils::OutputFormat);
}

/// Get trait.
/// To be implemented by resources which support the 'get' operation.
#[async_trait(?Send)]
pub trait Get {
    type ID;
    async fn get(id: &Self::ID, output: &utils::OutputFormat);
}

/// Scale trait.
/// To be implemented by resources which support the 'scale' operation.
#[async_trait(?Send)]
pub trait Scale {
    type ID;
    async fn scale(id: &Self::ID, replica_count: u8, output: &utils::OutputFormat);
}

/// Replica topology trait.
/// To be implemented by resources which support the 'replica-topology' operation
#[async_trait(?Send)]
pub trait ReplicaTopology {
    type ID;
    async fn topology(id: &Self::ID, output: &utils::OutputFormat);
}
