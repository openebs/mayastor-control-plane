//! Definition of replica types that can be saved to the persistent store.

use crate::types::v0::{
    openapi::models,
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        AsOperationSequencer, OperationSequence, ResourceUuid, SpecStatus, SpecTransaction,
    },
    transport::{
        self, CreateReplica, NodeId, PoolId, PoolRef, PoolUuid, Protocol, Replica as MbusReplica,
        ReplicaId, ReplicaName, ReplicaOwners, ReplicaShareProtocol, VolumeId,
    },
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
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

impl From<MbusReplica> for ReplicaState {
    fn from(replica: MbusReplica) -> Self {
        Self { replica }
    }
}

impl ResourceUuid for ReplicaState {
    type Id = ReplicaId;
    fn uuid(&self) -> Self::Id {
        self.replica.uuid.clone()
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
#[derive(Serialize, Debug, Clone, PartialEq, Default)]
pub struct ReplicaSpec {
    /// name of the replica
    pub name: ReplicaName,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// The size that the replica should be.
    pub size: u64,
    /// The pool that the replica should live on.
    pub pool: PoolId,
    /// The pool that the replica should live on.
    pub pool_ref: PoolRef,
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

impl TryFrom<&Value> for ReplicaName {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some(name) => Ok(name.into()),
            None => Err(format!("Invalid replica name: {}", value)),
        }
    }
}

impl TryFrom<&Value> for ReplicaId {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some(uuid) => match ReplicaId::try_from(uuid) {
                Ok(id) => Ok(id),
                Err(e) => Err(format!("Invalid replica id: {}", e)),
            },
            None => Err(format!("Invalid replica id: {}", value)),
        }
    }
}

impl TryFrom<&Value> for Protocol {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some(protocol) => Ok(match protocol {
                "nvmf" => Self::Nvmf,
                "iscsi" => Self::Iscsi,
                "nbd" => Self::Nbd,
                "none" => Self::None,
                _ => return Err(format!("Invalid Protocol: {}", protocol)),
            }),
            None => Err(format!("Invalid Protocol: {}", value)),
        }
    }
}

impl TryFrom<&Value> for ReplicaSpecStatus {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_object() {
            Some(status) => match status.get(&"Created".to_string()) {
                Some(status_val) => Ok(ReplicaSpecStatus::Created(
                    transport::ReplicaStatus::try_from(status_val)?,
                )),
                None => Err(format!("Invalid status: {:?}", status)),
            },
            None => match value.as_str() {
                Some(status) => Ok(match status {
                    "Creating" => ReplicaSpecStatus::Creating,
                    "Deleting" => ReplicaSpecStatus::Deleting,
                    "Deleted" => ReplicaSpecStatus::Deleted,
                    _ => return Err(format!("Invalid status: {}", status)),
                }),
                None => Err(format!("Invalid status: {}", value)),
            },
        }
    }
}

impl TryFrom<&Value> for transport::ReplicaStatus {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some(status) => Ok(match status {
                "unknown" => transport::ReplicaStatus::Unknown,
                "online" => transport::ReplicaStatus::Online,
                "degraded" => transport::ReplicaStatus::Degraded,
                "faulted" => transport::ReplicaStatus::Faulted,
                _ => return Err(format!("Invalid status: {}", status)),
            }),
            None => Err(format!("Invalid status: {}", value)),
        }
    }
}

impl TryFrom<&Value> for ReplicaOwners {
    type Error = String;

    fn try_from(owners: &Value) -> Result<Self, Self::Error> {
        match owners.get("volume") {
            Some(volume) => match volume.as_str() {
                Some(val) => {
                    let volume_id = match VolumeId::try_from(val) {
                        Ok(id) => id,
                        Err(e) => return Err(format!("Invalid owner volume: {}", e)),
                    };
                    Ok(ReplicaOwners::new(Some(volume_id), vec![]))
                }
                None => match volume.as_null() {
                    Some(()) => Ok(ReplicaOwners::new(None, vec![])),
                    None => Err(format!("Invalid owner volume: {}", volume)),
                },
            },
            None => Err(format!("Invalid owners: {}", owners)),
        }
    }
}

impl TryFrom<&Value> for ReplicaOperationState {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let result = match value.get("result") {
            Some(result) => match result.as_null() {
                Some(()) => None,
                None => match result.as_bool() {
                    Some(result) => Some(result),
                    None => return Err(format!("Invalid operation result: {}", result)),
                },
            },
            None => return Err(format!("Invalid operation result: {}", value)),
        };

        match value.get("operation") {
            Some(operation) => match operation.as_object() {
                Some(op) => match op.get(&"Share".to_string()) {
                    Some(share) => Ok(ReplicaOperationState {
                        operation: ReplicaOperation::Share(ReplicaShareProtocol::try_from(share)?),
                        result,
                    }),
                    None => Err(format!("Invalid status: {:?}", op)),
                },
                None => Ok(ReplicaOperationState {
                    operation: ReplicaOperation::try_from(operation)?,
                    result,
                }),
            },
            None => Err(format!("Invalid operation: {}", value)),
        }
    }
}

impl TryFrom<&Value> for ReplicaShareProtocol {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some(share) => match share {
                "nvmf" => Ok(ReplicaShareProtocol::Nvmf),
                _ => Err(format!("Invalid replica share protocol: {}", share)),
            },
            None => Err(format!("Invalid replica share protocol: {}", value)),
        }
    }
}

impl TryFrom<&Value> for ReplicaOperation {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some(replica_operation) => Ok(match replica_operation {
                "Create" => ReplicaOperation::Create,
                "Destroy" => ReplicaOperation::Destroy,
                "Unshare" => ReplicaOperation::Unshare,
                _ => return Err(format!("Invalid replica operation {}", replica_operation)),
            }),
            None => Err(format!("Invalid replica operation {}", value)),
        }
    }
}

#[cfg(test)]
mod tests_de {
    use super::*;

    #[test]
    fn test_custom_deserializer() {
        let inputs: Vec<&str> = vec![
            r#"{"name":"30b1a62e-4301-445b-88af-125eca1dcc6d","uuid":"30b1a62e-4301-445b-88af-125eca1dcc6d","size":80241024,"pool_ref":{"poolName":"pool-1"},"share":"none","thin":false,"status":{"Created":"online"},"managed":false,"owners":{"volume":null},"operation":null}"#,
            r#"{"name":"30b1a62e-4301-445b-88af-125eca1dcc6d","uuid":"30b1a62e-4301-445b-88af-125eca1dcc6d","size":10485761,"pool":"pool-1","share":"none","thin":false,"status":{"Created":"online"},"managed":true,"owners":{"volume":"ec4e66fd-3b33-4439-b504-d49aba53da26"},"operation":null}"#,
        ];

        for input in &inputs {
            let replica_spec: ReplicaSpec = serde_json::from_str(input).unwrap();
            println!("{:?}", replica_spec);
        }
    }
}

impl<'de> Deserialize<'de> for ReplicaSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json: Value = Value::deserialize(deserializer)?;
        let name = match json.get("name") {
            Some(name) => match ReplicaName::try_from(name) {
                Ok(name) => name,
                Err(e) => return Err(serde::de::Error::custom(e)),
            },
            None => return Err(serde::de::Error::custom("Missing replica name".to_string())),
        };

        let uuid = match json.get("uuid") {
            Some(uuid) => match ReplicaId::try_from(uuid) {
                Ok(uuid) => uuid,
                Err(e) => return Err(serde::de::Error::custom(e)),
            },
            None => return Err(serde::de::Error::custom("Missing replica uuid".to_string())),
        };

        let size = match json.get("size") {
            Some(size) => match size.as_u64() {
                Some(size) => size,
                None => {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid replica size: {}",
                        size
                    )))
                }
            },
            None => return Err(serde::de::Error::custom("Missing replica size".to_string())),
        };

        let share = match json.get("share") {
            Some(share) => match Protocol::try_from(share) {
                Ok(share) => share,
                Err(e) => return Err(serde::de::Error::custom(e)),
            },
            None => {
                return Err(serde::de::Error::custom(
                    "Missing replica share protocol".to_string(),
                ))
            }
        };

        let thin = match json.get("thin") {
            Some(thin) => match thin.as_bool() {
                Some(thin) => thin,
                None => {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid thin flag: {}",
                        thin
                    )))
                }
            },
            None => return Err(serde::de::Error::custom("Missing thin flag".to_string())),
        };

        let managed = match json.get("managed") {
            Some(managed) => match managed.as_bool() {
                Some(managed) => managed,
                None => {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid managed flag: {}",
                        managed
                    )))
                }
            },
            None => return Err(serde::de::Error::custom("Missing managed flag".to_string())),
        };

        let status = match json.get("status") {
            Some(status) => match ReplicaSpecStatus::try_from(status) {
                Ok(status) => status,
                Err(e) => return Err(serde::de::Error::custom(e)),
            },
            None => return Err(serde::de::Error::custom("Missing status flag".to_string())),
        };

        let owners = match json.get("owners") {
            Some(owners) => match ReplicaOwners::try_from(owners) {
                Ok(owners) => owners,
                Err(e) => return Err(serde::de::Error::custom(e)),
            },
            None => return Err(serde::de::Error::custom("Missing owners flag".to_string())),
        };

        let operation = match json.get("operation") {
            Some(operation) => match operation.as_null() {
                Some(()) => None,
                None => match ReplicaOperationState::try_from(operation) {
                    Ok(operation) => Some(operation),
                    Err(e) => return Err(serde::de::Error::custom(e)),
                },
            },
            None => {
                return Err(serde::de::Error::custom(
                    "Missing replica operation".to_string(),
                ))
            }
        };

        let pool = json.get("pool");
        let pool_ref = match pool {
            Some(pool) => PoolRef::PoolName(match pool.as_str() {
                Some(pool) => pool.into(),
                None => {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid pool id: {}",
                        pool
                    )))
                }
            }),
            None => match json.get("pool_ref") {
                Some(pool_ref) => match pool_ref.get("poolName") {
                    Some(pool_id) => match pool_id.as_str() {
                        Some(id) => PoolRef::PoolName(id.into()),
                        None => {
                            return Err(serde::de::Error::custom(format!(
                                "Invalid Pool id: {}",
                                pool_id
                            )))
                        }
                    },
                    None => match pool_ref.get("poolUuid") {
                        Some(pool_uuid) => match pool_uuid.as_str() {
                            Some(uuid) => PoolRef::PoolUuid(match PoolUuid::try_from(uuid) {
                                Ok(uuid) => uuid,
                                Err(e) => return Err(serde::de::Error::custom(e)),
                            }),
                            None => {
                                return Err(serde::de::Error::custom(format!(
                                    "Invalid Pool uuid: {}",
                                    pool_uuid
                                )))
                            }
                        },
                        None => {
                            return Err(serde::de::Error::custom(
                                "Invalid Pool reference".to_string(),
                            ))
                        }
                    },
                },
                None => {
                    return Err(serde::de::Error::custom(
                        "Missing Pool reference: {}".to_string(),
                    ))
                }
            },
        };

        Ok(ReplicaSpec {
            name,
            uuid,
            size,
            pool: Default::default(),
            pool_ref,
            share,
            thin,
            status,
            managed,
            owners,
            sequencer: Default::default(),
            operation,
        })
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

impl ResourceUuid for ReplicaSpec {
    type Id = ReplicaId;
    fn uuid(&self) -> Self::Id {
        self.uuid.clone()
    }
}

impl From<ReplicaSpec> for models::ReplicaSpec {
    fn from(src: ReplicaSpec) -> Self {
        Self::new(
            src.managed,
            src.owners,
            src.pool,
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
            pool: replica.pool.clone(),
            pool_ref: replica.pool_ref.clone(),
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
            pool: request.pool.clone(),
            pool_ref: request.pool_ref.clone(),
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
        self.share == other.share && self.pool == other.pool
    }
}
