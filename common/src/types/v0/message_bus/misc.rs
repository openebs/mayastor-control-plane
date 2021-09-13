use super::*;

use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt::Debug};

use std::str::FromStr;
use strum_macros::{EnumString, ToString};

/// Filter Objects based on one of the following criteria
/// # Example:
/// // Get all nexuses from the node `node_id`
/// let nexuses =
///     MessageBus::get_nexuses(Filter::Node(node_id)).await.unwrap();
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::ToString)] // likely this ToString does not do the right thing...
pub enum Filter {
    /// All objects
    None,
    /// Filter by Node id
    Node(NodeId),
    /// Pool filters
    ///
    /// Filter by Pool id
    Pool(PoolId),
    /// Filter by Node and Pool id
    NodePool(NodeId, PoolId),
    /// Filter by Node and Replica id
    NodeReplica(NodeId, ReplicaId),
    /// Filter by Node, Pool and Replica id
    NodePoolReplica(NodeId, PoolId, ReplicaId),
    /// Filter by Pool and Replica id
    PoolReplica(PoolId, ReplicaId),
    /// Filter by Replica id
    Replica(ReplicaId),
    /// Volume filters
    ///
    /// Filter by Node and Nexus
    NodeNexus(NodeId, NexusId),
    /// Filter by Nexus
    Nexus(NexusId),
    /// Filter by Volume
    Volume(VolumeId),
}
impl Default for Filter {
    fn default() -> Self {
        Self::None
    }
}

#[macro_export]
macro_rules! bus_impl_string_id_inner {
    ($Name:ident, $Doc:literal) => {
        #[doc = $Doc]
        #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
        pub struct $Name(String);

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl $Name {
            /// Build Self from a string trait id
            pub fn as_str<'a>(&'a self) -> &'a str {
                self.0.as_str()
            }
        }

        impl From<&str> for $Name {
            fn from(id: &str) -> Self {
                $Name::from(id)
            }
        }
        impl From<String> for $Name {
            fn from(id: String) -> Self {
                $Name::from(id.as_str())
            }
        }

        impl From<&$Name> for $Name {
            fn from(id: &$Name) -> $Name {
                id.clone()
            }
        }

        impl From<$Name> for String {
            fn from(id: $Name) -> String {
                id.to_string()
            }
        }
        impl From<&$Name> for String {
            fn from(id: &$Name) -> String {
                id.to_string()
            }
        }
    };
}

#[macro_export]
macro_rules! bus_impl_string_id {
    ($Name:ident, $Doc:literal) => {
        bus_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            /// Generates new blank identifier
            fn default() -> Self {
                $Name(uuid::Uuid::default().to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id
            pub fn from<T: Into<String>>(id: T) -> Self {
                $Name(id.into())
            }
            /// Generates new random identifier
            pub fn new() -> Self {
                $Name(uuid::Uuid::new_v4().to_string())
            }
        }
    };
}

#[macro_export]
macro_rules! bus_impl_string_uuid {
    ($Name:ident, $Doc:literal) => {
        bus_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            /// Generates new blank identifier
            fn default() -> Self {
                $Name(uuid::Uuid::default().to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id
            pub fn from<T: Into<String>>(id: T) -> Self {
                $Name(id.into())
            }
            /// Generates new random identifier
            pub fn new() -> Self {
                $Name(uuid::Uuid::new_v4().to_string())
            }
        }
        impl std::convert::TryFrom<&$Name> for uuid::Uuid {
            type Error = uuid::Error;
            fn try_from(value: &$Name) -> Result<Self, Self::Error> {
                value.as_str().parse()
            }
        }
        impl std::convert::TryFrom<$Name> for uuid::Uuid {
            type Error = uuid::Error;
            fn try_from(value: $Name) -> Result<Self, Self::Error> {
                std::convert::TryFrom::try_from(&value)
            }
        }
    };
}

#[macro_export]
macro_rules! bus_impl_string_id_percent_decoding {
    ($Name:ident, $Doc:literal) => {
        bus_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            fn default() -> Self {
                $Name("".to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id
            pub fn from<T: Into<String>>(id: T) -> Self {
                let src: String = id.into();
                let decoded_src = percent_decode_str(src.clone().as_str())
                    .decode_utf8()
                    .unwrap_or(src.into())
                    .to_string();
                $Name(decoded_src)
            }
        }
    };
}

/// Indicates what protocol the bdev is shared as
#[derive(Serialize, Deserialize, Debug, Copy, Clone, EnumString, ToString, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum Protocol {
    /// not shared by any of the variants
    None = 0,
    /// shared as NVMe-oF TCP
    Nvmf = 1,
    /// shared as iSCSI
    Iscsi = 2,
    /// shared as NBD
    Nbd = 3,
}

impl Protocol {
    /// Is the protocol set to be shared
    pub fn shared(&self) -> bool {
        self != &Self::None
    }
}
impl Default for Protocol {
    fn default() -> Self {
        Self::None
    }
}
impl From<i32> for Protocol {
    fn from(src: i32) -> Self {
        match src {
            0 => Self::None,
            1 => Self::Nvmf,
            2 => Self::Iscsi,
            _ => Self::None,
        }
    }
}

/// Convert a device URI to a share Protocol
/// Uses the URI scheme to determine the protocol
/// Temporary WA until the share is added to the mayastor RPC
impl TryFrom<&str> for Protocol {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(if value.is_empty() {
            Protocol::None
        } else {
            match url::Url::from_str(value) {
                Ok(url) => match url.scheme() {
                    "nvmf" => Self::Nvmf,
                    "iscsi" => Self::Iscsi,
                    "nbd" => Self::Nbd,
                    other => return Err(format!("Invalid nexus protocol: {}", other)),
                },
                Err(error) => {
                    tracing::error!("error parsing uri's ({}) protocol: {}", value, error);
                    return Err(error.to_string());
                }
            }
        })
    }
}

impl From<Protocol> for models::Protocol {
    fn from(src: Protocol) -> Self {
        match src {
            Protocol::None => Self::None,
            Protocol::Nvmf => Self::Nvmf,
            Protocol::Iscsi => Self::Iscsi,
            Protocol::Nbd => Self::Nbd,
        }
    }
}
impl From<models::Protocol> for Protocol {
    fn from(src: models::Protocol) -> Self {
        match src {
            models::Protocol::None => Self::None,
            models::Protocol::Nvmf => Self::Nvmf,
            models::Protocol::Iscsi => Self::Iscsi,
            models::Protocol::Nbd => Self::Nbd,
        }
    }
}

/// Liveness Probe
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Liveness {}
