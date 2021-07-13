pub mod child;
pub mod definitions;
pub mod nexus;
pub mod node;
pub mod pool;
pub mod replica;
pub mod volume;
pub mod watch;

use crate::types::v0::openapi::models;
use serde::{Deserialize, Serialize};

/// Enum defining the various states that a resource spec can be in.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SpecState<T> {
    Creating,
    Created(T),
    Deleting,
    Deleted,
}

impl<T> Default for SpecState<T> {
    fn default() -> Self {
        Self::Creating
    }
}

// todo: change openapi spec to support enum variants
impl<T> From<SpecState<T>> for models::SpecState {
    fn from(src: SpecState<T>) -> Self {
        match src {
            SpecState::Creating => Self::Creating,
            SpecState::Created(_) => Self::Created,
            SpecState::Deleting => Self::Deleting,
            SpecState::Deleted => Self::Deleted,
        }
    }
}

impl<T: std::cmp::PartialEq> SpecState<T> {
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
