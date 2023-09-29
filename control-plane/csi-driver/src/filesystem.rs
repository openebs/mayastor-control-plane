//! This module consists of the filesystem type definition shared between controller and node.
use strum_macros::EnumString;

/// A type to enumerate used filesystems.
#[derive(EnumString, Clone, Debug, Eq, PartialEq)]
#[strum(serialize_all = "lowercase")]
pub enum FileSystem {
    Ext4,
    Xfs,
    Btrfs,
    DevTmpFs,
    Unsupported(String),
}

// Implement as ref for the FileSystem.
impl AsRef<str> for FileSystem {
    fn as_ref(&self) -> &str {
        match self {
            FileSystem::Ext4 => "ext4",
            FileSystem::Xfs => "xfs",
            FileSystem::Btrfs => "btrfs",
            FileSystem::DevTmpFs => "devtmpfs",
            FileSystem::Unsupported(inner) => inner,
        }
    }
}

// Implement Display for the filesystem
impl std::fmt::Display for FileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
