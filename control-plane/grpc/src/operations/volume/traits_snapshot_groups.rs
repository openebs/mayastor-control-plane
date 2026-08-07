use crate::{common, misc::traits::ValidateRequestTypes, volume};

use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::{
        store::{snapshots::group::VolumeSnapshotGroupQuiesce, SpecStatus},
        transport::{SnapshotGroupId, SnapshotId, VolumeId},
    },
};

use super::traits_snapshots::{TryIntoId, VolumeSnapshot};
use std::{collections::HashMap, convert::TryFrom};

/// Snapshot group creation information.
pub trait CreateSnapshotGroupInfo: Send + Sync + std::fmt::Debug {
    /// Group creation information.
    fn info(&self) -> CreateSnapshotGroup;
}

/// Snapshot group deletion information.
pub trait DestroySnapshotGroupInfo: Send + Sync + std::fmt::Debug {
    /// Group deletion information.
    fn info(&self) -> DestroySnapshotGroup;
}

/// Validated create snapshot group parameters.
#[derive(Debug, Clone)]
pub struct CreateSnapshotGroup {
    group_id: SnapshotGroupId,
    members: HashMap<VolumeId, SnapshotId>,
    quiesce: VolumeSnapshotGroupQuiesce,
}
impl CreateSnapshotGroup {
    /// Create a new `Self` from the given parameters.
    pub fn new(
        group_id: SnapshotGroupId,
        members: HashMap<VolumeId, SnapshotId>,
        quiesce: VolumeSnapshotGroupQuiesce,
    ) -> Self {
        Self {
            group_id,
            members,
            quiesce,
        }
    }
    /// Get the group id.
    pub fn group_id(&self) -> &SnapshotGroupId {
        &self.group_id
    }
    /// Get the group members as a map of volume to snapshot id.
    pub fn members(&self) -> &HashMap<VolumeId, SnapshotId> {
        &self.members
    }
    /// Get the quiesce mode of the group.
    pub fn quiesce(&self) -> VolumeSnapshotGroupQuiesce {
        self.quiesce
    }
}
impl CreateSnapshotGroupInfo for CreateSnapshotGroup {
    fn info(&self) -> CreateSnapshotGroup {
        self.clone()
    }
}

/// Validated destroy snapshot group parameters.
#[derive(Debug, Clone)]
pub struct DestroySnapshotGroup {
    group_id: SnapshotGroupId,
}
impl DestroySnapshotGroup {
    /// Create a new `Self` from the given group id.
    pub fn new(group_id: SnapshotGroupId) -> Self {
        Self { group_id }
    }
    /// Get the group id.
    pub fn group_id(&self) -> &SnapshotGroupId {
        &self.group_id
    }
}
impl DestroySnapshotGroupInfo for DestroySnapshotGroup {
    fn info(&self) -> DestroySnapshotGroup {
        self.clone()
    }
}

/// A snapshot group along with its member volume snapshots.
#[derive(Debug)]
pub struct SnapshotGroup {
    group_id: SnapshotGroupId,
    members: HashMap<VolumeId, SnapshotId>,
    status: SpecStatus<()>,
    timestamp: Option<prost_types::Timestamp>,
    quiesce: VolumeSnapshotGroupQuiesce,
    snapshots: Vec<VolumeSnapshot>,
}
impl SnapshotGroup {
    /// Create a new `Self` from the given parameters.
    pub fn new(
        group_id: SnapshotGroupId,
        members: HashMap<VolumeId, SnapshotId>,
        status: SpecStatus<()>,
        timestamp: Option<prost_types::Timestamp>,
        quiesce: VolumeSnapshotGroupQuiesce,
        snapshots: Vec<VolumeSnapshot>,
    ) -> Self {
        Self {
            group_id,
            members,
            status,
            timestamp,
            quiesce,
            snapshots,
        }
    }
    /// Get the group id.
    pub fn group_id(&self) -> &SnapshotGroupId {
        &self.group_id
    }
    /// Get the group members as a map of volume to snapshot id.
    pub fn members(&self) -> &HashMap<VolumeId, SnapshotId> {
        &self.members
    }
    /// Get the group status.
    pub fn status(&self) -> &SpecStatus<()> {
        &self.status
    }
    /// Get the group creation timestamp (set after creation time).
    pub fn timestamp(&self) -> Option<&prost_types::Timestamp> {
        self.timestamp.as_ref()
    }
    /// Get the quiesce mode of the group.
    pub fn quiesce(&self) -> VolumeSnapshotGroupQuiesce {
        self.quiesce
    }
    /// Get the member volume snapshots.
    pub fn snapshots(&self) -> &Vec<VolumeSnapshot> {
        &self.snapshots
    }
}

impl From<&stor_port::types::v0::store::snapshots::group::VolumeSnapshotGroup> for SnapshotGroup {
    fn from(value: &stor_port::types::v0::store::snapshots::group::VolumeSnapshotGroup) -> Self {
        Self {
            group_id: value.uuid().clone(),
            members: value.spec().members().clone(),
            status: value.status().clone(),
            timestamp: value
                .metadata()
                .timestamp()
                .map(|t| std::time::SystemTime::from(t).into()),
            quiesce: value.spec().quiesce(),
            snapshots: vec![],
        }
    }
}

/// A list of snapshot groups.
#[derive(Debug, Default)]
pub struct SnapshotGroups {
    entries: Vec<SnapshotGroup>,
}
impl SnapshotGroups {
    /// Create a new `Self` from the given entries.
    pub fn new(entries: Vec<SnapshotGroup>) -> Self {
        Self { entries }
    }
    /// Get the snapshot group entries.
    pub fn entries(&self) -> &Vec<SnapshotGroup> {
        &self.entries
    }
    /// Move `Self` into the snapshot group entries.
    pub fn into_entries(self) -> Vec<SnapshotGroup> {
        self.entries
    }
}

impl From<VolumeSnapshotGroupQuiesce> for volume::SnapshotGroupQuiesce {
    fn from(value: VolumeSnapshotGroupQuiesce) -> Self {
        match value {
            VolumeSnapshotGroupQuiesce::Freeze => Self::Freeze,
            VolumeSnapshotGroupQuiesce::None => Self::NoQuiesce,
        }
    }
}
impl From<volume::SnapshotGroupQuiesce> for VolumeSnapshotGroupQuiesce {
    fn from(value: volume::SnapshotGroupQuiesce) -> Self {
        match value {
            volume::SnapshotGroupQuiesce::Freeze => Self::Freeze,
            volume::SnapshotGroupQuiesce::NoQuiesce => Self::None,
        }
    }
}

impl ValidateRequestTypes for volume::CreateSnapshotGroupRequest {
    type Validated = CreateSnapshotGroup;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(CreateSnapshotGroup {
            group_id: self
                .group_id
                .try_into_id(ResourceKind::VolumeSnapshotGroup, "group_id")?,
            members: validated_members(self.members)?,
            quiesce: volume::SnapshotGroupQuiesce::try_from(self.quiesce)
                .map_err(|error| {
                    ReplyError::invalid_argument(
                        ResourceKind::VolumeSnapshotGroup,
                        "quiesce",
                        error,
                    )
                })?
                .into(),
        })
    }
}
impl ValidateRequestTypes for volume::DestroySnapshotGroupRequest {
    type Validated = DestroySnapshotGroup;
    fn validated(self) -> Result<Self::Validated, ReplyError> {
        Ok(DestroySnapshotGroup {
            group_id: self
                .group_id
                .try_into_id(ResourceKind::VolumeSnapshotGroup, "group_id")?,
        })
    }
}

/// Validate a raw member map into typed volume and snapshot ids.
fn validated_members(
    members: HashMap<String, String>,
) -> Result<HashMap<VolumeId, SnapshotId>, ReplyError> {
    members
        .into_iter()
        .map(|(volume_id, snapshot_id)| {
            Ok((
                volume_id.try_into_id(ResourceKind::VolumeSnapshotGroup, "members.volume_id")?,
                snapshot_id
                    .try_into_id(ResourceKind::VolumeSnapshotGroup, "members.snapshot_id")?,
            ))
        })
        .collect()
}

impl From<&dyn CreateSnapshotGroupInfo> for volume::CreateSnapshotGroupRequest {
    fn from(value: &dyn CreateSnapshotGroupInfo) -> Self {
        let info = value.info();
        Self {
            group_id: info.group_id.to_string(),
            members: info
                .members
                .iter()
                .map(|(volume_id, snapshot_id)| (volume_id.to_string(), snapshot_id.to_string()))
                .collect(),
            quiesce: volume::SnapshotGroupQuiesce::from(info.quiesce) as i32,
        }
    }
}
impl From<&dyn DestroySnapshotGroupInfo> for volume::DestroySnapshotGroupRequest {
    fn from(value: &dyn DestroySnapshotGroupInfo) -> Self {
        let info = value.info();
        Self {
            group_id: info.group_id.to_string(),
        }
    }
}

impl TryFrom<volume::SnapshotGroup> for SnapshotGroup {
    type Error = ReplyError;
    fn try_from(value: volume::SnapshotGroup) -> Result<Self, Self::Error> {
        Ok(Self {
            group_id: value
                .group_id
                .try_into_id(ResourceKind::VolumeSnapshotGroup, "group_id")?,
            members: validated_members(value.members)?,
            status: common::SpecStatus::try_from(value.spec_status)
                .unwrap_or_default()
                .into(),
            timestamp: value.timestamp,
            quiesce: volume::SnapshotGroupQuiesce::try_from(value.quiesce)
                .unwrap_or_default()
                .into(),
            snapshots: value
                .snapshots
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<VolumeSnapshot>, ReplyError>>()?,
        })
    }
}
impl TryFrom<SnapshotGroup> for volume::SnapshotGroup {
    type Error = ReplyError;
    fn try_from(value: SnapshotGroup) -> Result<Self, Self::Error> {
        Ok(Self {
            group_id: value.group_id.to_string(),
            members: value
                .members
                .iter()
                .map(|(volume_id, snapshot_id)| (volume_id.to_string(), snapshot_id.to_string()))
                .collect(),
            spec_status: common::SpecStatus::from(&value.status) as i32,
            timestamp: value.timestamp,
            quiesce: volume::SnapshotGroupQuiesce::from(value.quiesce) as i32,
            snapshots: value
                .snapshots
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<volume::VolumeSnapshot>, ReplyError>>()?,
        })
    }
}

impl TryFrom<volume::SnapshotGroups> for SnapshotGroups {
    type Error = ReplyError;
    fn try_from(value: volume::SnapshotGroups) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .groups
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<SnapshotGroup>, ReplyError>>()?,
        })
    }
}
impl TryFrom<SnapshotGroups> for volume::SnapshotGroups {
    type Error = ReplyError;
    fn try_from(value: SnapshotGroups) -> Result<Self, Self::Error> {
        Ok(Self {
            groups: value
                .entries
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<volume::SnapshotGroup>, ReplyError>>()?,
        })
    }
}
