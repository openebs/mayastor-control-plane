pub(crate) mod registry;
pub(crate) mod scheduling;
mod service;
pub mod specs;

use async_trait::async_trait;
use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use common::{errors::SvcError, handler::*};

// Nexus Operations
use common_lib::types::v0::message_bus::{
    CreateNexus, DestroyNexus, GetNexuses, ShareNexus, UnshareNexus,
};
// Nexus Child Operations
use common_lib::types::v0::message_bus::{AddNexusChild, RemoveNexusChild};

pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Nexus)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetNexuses))
        .with_subscription(handler!(CreateNexus))
        .with_subscription(handler!(DestroyNexus))
        .with_subscription(handler!(ShareNexus))
        .with_subscription(handler!(UnshareNexus))
        .with_subscription(handler!(AddNexusChild))
        .with_subscription(handler!(RemoveNexusChild))
}

/// Nexus Agent's Tests
#[cfg(test)]
mod tests;
