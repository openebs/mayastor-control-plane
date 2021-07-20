use async_trait::async_trait;
use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use common::{errors::SvcError, handler::*};
use common_lib::types::v0::message_bus::{
    CreateVolume, DestroyVolume, GetVolumes, PublishVolume, SetVolumeReplica, ShareVolume,
    UnpublishVolume, UnshareVolume,
};

mod registry;
mod scheduling;
mod service;
pub mod specs;

pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Volume)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetVolumes))
        .with_subscription(handler!(CreateVolume))
        .with_subscription(handler!(DestroyVolume))
        .with_subscription(handler!(ShareVolume))
        .with_subscription(handler!(UnshareVolume))
        .with_subscription(handler!(PublishVolume))
        .with_subscription(handler!(UnpublishVolume))
        .with_subscription(handler!(SetVolumeReplica))
}

/// Volume Agent's Tests
#[cfg(test)]
mod tests;
