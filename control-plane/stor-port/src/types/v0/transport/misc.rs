use super::*;

use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    fmt::{Debug, Write},
};

use std::str::FromStr;
use strum_macros::Display;

/// Filter Objects based on one of the following criteria.
/// # Example:
/// // Get all nexuses from the node `node_id`
/// let nexuses =
///     client.get_nexuses(Filter::Node(node_id)).await.unwrap();
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::Display)] // likely this ToString does not do the right thing...
pub enum Filter {
    /// All objects.
    None,
    /// Filter by Node id.
    Node(NodeId),
    /// Pool filters.
    ///
    /// Filter by Pool id.
    Pool(PoolId),
    /// Filter by Node and Pool id.
    NodePool(NodeId, PoolId),
    /// Filter by Node and Replica id.
    NodeReplica(NodeId, ReplicaId),
    /// Filter by Node, Pool and Replica id.
    NodePoolReplica(NodeId, PoolId, ReplicaId),
    /// Filter by Pool and Replica id.
    PoolReplica(PoolId, ReplicaId),
    /// Filter by Replica id.
    Replica(ReplicaId),
    /// Volume filters.
    ///
    /// Filter by Node and Nexus.
    NodeNexus(NodeId, NexusId),
    /// Filter by Nexus.
    Nexus(NexusId),
    /// Filter by Volume.
    Volume(VolumeId),
}
impl Default for Filter {
    fn default() -> Self {
        Self::None
    }
}

#[macro_export]
macro_rules! rpc_impl_string_id_inner {
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
            /// Build Self from a string trait id.
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

        impl From<$Name> for Option<String> {
            fn from(id: $Name) -> Option<String> {
                Some(id.to_string())
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
macro_rules! rpc_impl_string_id {
    ($Name:ident, $Doc:literal) => {
        rpc_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            /// Generates new blank identifier.
            fn default() -> Self {
                $Name(uuid::Uuid::default().to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id.
            pub fn from<T: Into<String>>(id: T) -> Self {
                $Name(id.into())
            }
            /// Generates new random identifier.
            pub fn new() -> Self {
                $Name(uuid::Uuid::new_v4().to_string())
            }
        }
    };
}

#[macro_export]
macro_rules! rpc_impl_string_uuid_inner {
    ($Name:ident, $Doc:literal) => {
        #[doc = $Doc]
        #[derive(Debug, Clone, Eq, PartialEq, Hash)]
        pub struct $Name(uuid::Uuid, String);

        impl Serialize for $Name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $Name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let uuid = uuid::Uuid::deserialize(deserializer)?;
                Ok($Name(uuid, uuid.to_string()))
            }
        }

        impl std::ops::Deref for $Name {
            type Target = uuid::Uuid;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl $Name {
            /// Build Self from a string trait id.
            pub fn as_str<'a>(&'a self) -> &'a str {
                self.1.as_str()
            }
            /// Get a reference to a `uuid::Uuid` container.
            pub fn uuid(&self) -> &uuid::Uuid {
                &self.0
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
        impl From<&uuid::Uuid> for $Name {
            fn from(uuid: &uuid::Uuid) -> $Name {
                $Name(uuid.clone(), uuid.to_string())
            }
        }
        impl From<uuid::Uuid> for $Name {
            fn from(uuid: uuid::Uuid) -> $Name {
                $Name::from(&uuid)
            }
        }
        impl From<$Name> for uuid::Uuid {
            fn from(src: $Name) -> uuid::Uuid {
                src.0
            }
        }
        impl From<&$Name> for uuid::Uuid {
            fn from(src: &$Name) -> uuid::Uuid {
                src.0.clone()
            }
        }
        impl std::convert::TryFrom<&str> for $Name {
            type Error = uuid::Error;
            fn try_from(value: &str) -> Result<Self, Self::Error> {
                let uuid: uuid::Uuid = std::str::FromStr::from_str(value)?;
                Ok($Name::from(uuid))
            }
        }
        impl std::convert::TryFrom<String> for $Name {
            type Error = uuid::Error;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                let uuid: uuid::Uuid = std::str::FromStr::from_str(&value)?;
                Ok($Name::from(uuid))
            }
        }
    };
}

#[macro_export]
macro_rules! rpc_impl_string_uuid {
    ($Name:ident, $Doc:literal) => {
        rpc_impl_string_uuid_inner!($Name, $Doc);
        impl Default for $Name {
            /// Generates new blank identifier.
            fn default() -> Self {
                let uuid = uuid::Uuid::default();
                $Name(uuid.clone(), uuid.to_string())
            }
        }
        impl $Name {
            /// Generates new random identifier.
            pub fn new() -> Self {
                let uuid = uuid::Uuid::new_v4();
                $Name(uuid.clone(), uuid.to_string())
            }
        }
    };
}

#[macro_export]
macro_rules! rpc_impl_string_id_percent_decoding {
    ($Name:ident, $Doc:literal) => {
        rpc_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            fn default() -> Self {
                $Name("".to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id.
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

/// Indicates what protocol the bdev is shared as.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Display, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum Protocol {
    /// Not shared by any of the variants.
    None = 0,
    /// Shared as NVMe-oF TCP.
    Nvmf = 1,
    /// Shared as iSCSI.
    Iscsi = 2,
    /// Shared as NBD.
    Nbd = 3,
}

impl Protocol {
    /// Is the protocol set to be shared.
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

/// Convert a device URI to a share Protocol.
/// Uses the URI scheme to determine the protocol.
/// Temporary WA until the share is added to the io-engine RPC.
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
                    other => return Err(format!("Invalid nexus protocol: {other}")),
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

/// Liveness Probe.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Liveness {}

/// Add query parameter to a Uri.
pub fn add_query(mut base: String, name: &str, value: &str) -> String {
    if base.contains('?') {
        let _ = write!(base, "&{name}={value}");
    } else {
        let _ = write!(base, "?{name}={value}");
    }
    base
}

/// Update the Uri with the given allowed_host's nqn's.
/// # Warning: Should be called only once.
pub fn uri_with_hostnqn(base_uri: &str, hostnqns: &[HostNqn]) -> String {
    hostnqns.iter().fold(base_uri.to_string(), |acc, nqn| {
        add_query(acc, "hostnqn", &nqn.to_string())
    })
}

/// Strip all query parameters from the given `String`.
pub fn strip_queries(base: String, name: &str) -> String {
    let mut url = url::Url::from_str(&base).unwrap();
    let qp = url
        .query_pairs()
        .filter(|(n, _)| n != name)
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect::<Vec<_>>();
    url.set_query(None);
    for (n, v) in qp {
        url.query_pairs_mut().append_pair(&n, &v);
    }
    url.to_string()
}
