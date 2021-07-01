use crate::types::v0::message_bus::ChannelVs;
use std::str::FromStr;

pub mod v0;

/// Available Message Bus channels
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Channel {
    /// Version 0 of the Channels
    v0(ChannelVs),
}

impl FromStr for Channel {
    type Err = strum::ParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source.split('/').next() {
            Some(version) => {
                let c: ChannelVs = source[version.len() + 1 ..].parse()?;
                Ok(Self::v0(c))
            }
            _ => Err(strum::ParseError::VariantNotFound),
        }
    }
}

impl ToString for Channel {
    fn to_string(&self) -> String {
        match self {
            Self::v0(channel) => format!("v0/{}", channel.to_string()),
        }
    }
}

impl Default for Channel {
    fn default() -> Self {
        Channel::v0(ChannelVs::Default)
    }
}
