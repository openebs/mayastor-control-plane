//! Definition of replica types that can be saved to the persistent store.

use crate::{
    types::v0::{
        openapi::models,
        store::{
            definitions::{ObjectKey, StorableObject, StorableObjectType},
            AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction,
        },
        transport::{
            self, CreateReplica, NodeId, PoolId, PoolUuid, Protocol, ReplicaId, ReplicaName,
            ReplicaOwners, ReplicaShareProtocol,
        },
    },
    IntoOption,
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Replica information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Replica {
    /// Current state of the replica.
    pub state: Option<ReplicaState>,
    /// Desired replica specification.
    pub spec: ReplicaSpec,
}

/// Runtime state of a replica.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct ReplicaState {
    /// Replica information.
    pub replica: transport::Replica,
}

impl From<transport::Replica> for ReplicaState {
    fn from(replica: transport::Replica) -> Self {
        Self { replica }
    }
}

/// Key used by the store to uniquely identify a ReplicaState structure.
pub struct ReplicaStateKey(ReplicaId);

impl ObjectKey for ReplicaStateKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ReplicaState
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for ReplicaState {
    type Key = ReplicaStateKey;

    fn key(&self) -> Self::Key {
        ReplicaStateKey(self.replica.uuid.clone())
    }
}

/// User specification of a replica.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSpec {
    /// name of the replica
    pub name: ReplicaName,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// The size that the replica should be.
    pub size: u64,
    /// Reference of a pool that the replica should live on.
    pub pool: PoolRef,
    /// Protocol used for exposing the replica.
    pub share: Protocol,
    /// Thin provisioning.
    pub thin: bool,
    /// The status that the replica should eventually achieve.
    pub status: ReplicaSpecStatus,
    /// Managed by our control plane
    pub managed: bool,
    /// Owner Resource
    pub owners: ReplicaOwners,
    /// Update in progress
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Record of the operation in progress
    pub operation: Option<ReplicaOperationState>,
}

/// Reference of a pool.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum PoolRef {
    /// Name referenced pool.
    Named(PoolId),
    /// Uuid and name referenced pool.
    Uuid(PoolId, PoolUuid),
}

impl Default for PoolRef {
    fn default() -> Self {
        Self::Named("".into())
    }
}

impl PoolRef {
    /// Get the pool name.
    pub fn pool_name(&self) -> &PoolId {
        match &self {
            PoolRef::Named(name) => name,
            PoolRef::Uuid(name, _) => name,
        }
    }

    /// Get the pool uuid.
    pub fn pool_uuid(&self) -> Option<PoolUuid> {
        match &self {
            PoolRef::Named(_) => None,
            PoolRef::Uuid(_, uuid) => Some(uuid.clone()),
        }
    }
}

#[cfg(test)]
mod tests_deserializer {
    use super::*;
    use crate::types::v0::transport::{ReplicaStatus, VolumeId};

    #[test]
    fn test_replica_spec_deserializer() {
        struct Test<'a> {
            input: &'a str,
            expected: ReplicaSpec,
        }
        let tests: Vec<Test> = vec![
            Test {
                input: r#"{"name":"30b1a62e-4301-445b-88af-125eca1dcc6d","uuid":"30b1a62e-4301-445b-88af-125eca1dcc6d","size":10485761,"pool":"pool-1","share":"none","thin":false,"status":{"Created":"online"},"managed":true,"owners":{"volume":"ec4e66fd-3b33-4439-b504-d49aba53da26","disown_all":false},"operation":null}"#,
                expected: ReplicaSpec {
                    name: "30b1a62e-4301-445b-88af-125eca1dcc6d".into(),
                    uuid: ReplicaId::try_from("30b1a62e-4301-445b-88af-125eca1dcc6d").unwrap(),
                    size: 10485761,
                    pool: PoolRef::Named("pool-1".into()),
                    share: Protocol::None,
                    thin: false,
                    status: ReplicaSpecStatus::Created(ReplicaStatus::Online),
                    managed: true,
                    owners: ReplicaOwners::new(
                        Some(VolumeId::try_from("ec4e66fd-3b33-4439-b504-d49aba53da26").unwrap()),
                        vec![],
                    ),
                    sequencer: Default::default(),
                    operation: None,
                },
            },
            Test {
                input: r#"{"name":"30b1a62e-4301-445b-88af-125eca1dcc6d","uuid":"30b1a62e-4301-445b-88af-125eca1dcc6d","size":10485761,"pool":["pool-1","22ca10d3-4f2b-4b95-9814-9181c025cc1a"],"share":"none","thin":false,"status":{"Created":"online"},"managed":true,"owners":{"volume":"ec4e66fd-3b33-4439-b504-d49aba53da26","disown_all":false},"operation":null}"#,
                expected: ReplicaSpec {
                    name: "30b1a62e-4301-445b-88af-125eca1dcc6d".into(),
                    uuid: ReplicaId::try_from("30b1a62e-4301-445b-88af-125eca1dcc6d").unwrap(),
                    size: 10485761,
                    pool: PoolRef::Uuid(
                        "pool-1".into(),
                        PoolUuid::try_from("22ca10d3-4f2b-4b95-9814-9181c025cc1a").unwrap(),
                    ),
                    share: Protocol::None,
                    thin: false,
                    status: ReplicaSpecStatus::Created(ReplicaStatus::Online),
                    managed: true,
                    owners: ReplicaOwners::new(
                        Some(VolumeId::try_from("ec4e66fd-3b33-4439-b504-d49aba53da26").unwrap()),
                        vec![],
                    ),
                    sequencer: Default::default(),
                    operation: None,
                },
            },
        ];

        for test in &tests {
            let replica_spec: ReplicaSpec = serde_json::from_str(test.input).unwrap();
            assert_eq!(test.expected, replica_spec);
        }
    }
}

impl AsOperationSequencer for ReplicaSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl From<ReplicaSpec> for models::ReplicaSpec {
    fn from(src: ReplicaSpec) -> Self {
        Self::new_all(
            src.managed,
            None,
            src.owners,
            src.pool.pool_name(),
            src.pool.pool_uuid().into_opt(),
            src.share,
            src.size,
            src.status,
            src.thin,
            openapi::apis::Uuid::try_from(src.uuid).unwrap(),
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicaOperationState {
    /// Record of the operation
    pub operation: ReplicaOperation,
    /// Result of the operation
    pub result: Option<bool>,
}

impl SpecTransaction<ReplicaOperation> for ReplicaSpec {
    fn pending_op(&self) -> bool {
        self.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.operation.clone() {
            match op.operation {
                ReplicaOperation::Create => {
                    self.status = SpecStatus::Created(transport::ReplicaStatus::Online);
                }
                ReplicaOperation::Destroy => {
                    self.status = SpecStatus::Deleted;
                }
                ReplicaOperation::Share(share) => {
                    self.share = share.into();
                }
                ReplicaOperation::Unshare => {
                    self.share = Protocol::None;
                }
                ReplicaOperation::OwnerUpdate(owners) => {
                    self.owners = owners;
                }
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        self.operation = None;
    }

    fn start_op(&mut self, operation: ReplicaOperation) {
        self.operation = Some(ReplicaOperationState {
            operation,
            result: None,
        })
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.operation {
            op.result = Some(result);
        }
    }
}

/// Available Replica Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ReplicaOperation {
    Create,
    Destroy,
    Share(ReplicaShareProtocol),
    Unshare,
    OwnerUpdate(ReplicaOwners),
}

/// Key used by the store to uniquely identify a ReplicaSpec structure.
pub struct ReplicaSpecKey(ReplicaId);

impl ObjectKey for ReplicaSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ReplicaSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl From<&ReplicaId> for ReplicaSpecKey {
    fn from(id: &ReplicaId) -> Self {
        ReplicaSpecKey(id.clone())
    }
}

impl StorableObject for ReplicaSpec {
    type Key = ReplicaSpecKey;

    fn key(&self) -> Self::Key {
        ReplicaSpecKey(self.uuid.clone())
    }
}

impl From<&ReplicaSpec> for transport::Replica {
    fn from(replica: &ReplicaSpec) -> Self {
        Self {
            node: NodeId::default(),
            name: replica.name.clone(),
            uuid: replica.uuid.clone(),
            pool_id: replica.pool.pool_name().clone(),
            pool_uuid: replica.pool.pool_uuid(),
            thin: replica.thin,
            size: replica.size,
            share: replica.share,
            uri: "".to_string(),
            status: transport::ReplicaStatus::Unknown,
        }
    }
}

/// State of the Replica Spec
pub type ReplicaSpecStatus = SpecStatus<transport::ReplicaStatus>;

impl From<&CreateReplica> for ReplicaSpec {
    fn from(request: &CreateReplica) -> Self {
        Self {
            name: ReplicaName::from_opt_uuid(request.name.as_ref(), &request.uuid),
            uuid: request.uuid.clone(),
            size: request.size,
            pool: match request.pool_uuid.clone() {
                Some(uuid) => PoolRef::Uuid(request.pool_id.clone(), uuid),
                None => PoolRef::Named(request.pool_id.clone()),
            },
            share: request.share,
            thin: request.thin,
            status: ReplicaSpecStatus::Creating,
            managed: request.managed,
            owners: request.owners.clone(),
            sequencer: OperationSequence::new(request.uuid.clone()),
            operation: None,
        }
    }
}
impl PartialEq<CreateReplica> for ReplicaSpec {
    fn eq(&self, other: &CreateReplica) -> bool {
        let mut other = ReplicaSpec::from(other);
        other.status = self.status.clone();
        other.sequencer = self.sequencer.clone();
        &other == self
    }
}
impl PartialEq<transport::Replica> for ReplicaSpec {
    fn eq(&self, other: &transport::Replica) -> bool {
        let pool = match other.pool_uuid.clone() {
            Some(uuid) => PoolRef::Uuid(other.pool_id.clone(), uuid),
            None => PoolRef::Named(other.pool_id.clone()),
        };
        self.share == other.share && self.pool == pool
    }
}
