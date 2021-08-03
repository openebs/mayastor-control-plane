pub mod child;
pub mod definitions;
pub mod nexus;
pub mod nexus_child;
pub mod nexus_persistence;
pub mod node;
pub mod pool;
pub mod registry;
pub mod replica;
pub mod volume;
pub mod watch;

use crate::types::v0::openapi::models;
use serde::{Deserialize, Serialize};
use strum_macros::ToString;

/// Enum defining the various states that a resource spec can be in.
#[derive(Serialize, Deserialize, Debug, Clone, ToString, PartialEq)]
pub enum SpecStatus<T> {
    Creating,
    Created(T),
    Deleting,
    Deleted,
}

impl<T> Default for SpecStatus<T> {
    fn default() -> Self {
        Self::Creating
    }
}

// todo: change openapi spec to support enum variants
impl<T> From<SpecStatus<T>> for models::SpecState {
    fn from(src: SpecStatus<T>) -> Self {
        match src {
            SpecStatus::Creating => Self::Creating,
            SpecStatus::Created(_) => Self::Created,
            SpecStatus::Deleting => Self::Deleting,
            SpecStatus::Deleted => Self::Deleted,
        }
    }
}

impl<T: std::cmp::PartialEq> SpecStatus<T> {
    pub fn creating(&self) -> bool {
        self == &Self::Creating
    }
    pub fn created(&self) -> bool {
        matches!(self, &Self::Created(_))
    }
    pub fn deleting(&self) -> bool {
        self == &Self::Deleting
    }
    pub fn deleted(&self) -> bool {
        self == &Self::Deleted
    }
}

/// Transaction Operations for a Spec
pub trait SpecTransaction<Operation> {
    /// Check for a pending operation
    fn pending_op(&self) -> bool;
    /// Commit the operation to the spec and clear it
    fn commit_op(&mut self);
    /// Clear the operation
    fn clear_op(&mut self);
    /// Add a new pending operation
    fn start_op(&mut self, operation: Operation);
    /// Sets the result of the operation
    fn set_op_result(&mut self, result: bool);
}

/// Trait which allows a UUID to be returned as a string.
pub trait UuidString {
    fn uuid_as_string(&self) -> String;
}
