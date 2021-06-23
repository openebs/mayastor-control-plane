mod registry;
pub mod service;
pub mod specs;

use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use async_trait::async_trait;
use common::{errors::SvcError, handler::*, Service};

// Pool Operations
use types::v0::message_bus::mbus::{CreatePool, DestroyPool, GetPools};
// Replica Operations
use types::v0::message_bus::mbus::{
    CreateReplica, DestroyReplica, GetReplicas, ShareReplica, UnshareReplica,
};

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Pool)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetPools))
        .with_subscription(handler!(CreatePool))
        .with_subscription(handler!(DestroyPool))
        .with_subscription(handler!(GetReplicas))
        .with_subscription(handler!(CreateReplica))
        .with_subscription(handler!(DestroyReplica))
        .with_subscription(handler!(ShareReplica))
        .with_subscription(handler!(UnshareReplica))
}

/// Pool Agent's Tests
#[cfg(test)]
mod tests;
