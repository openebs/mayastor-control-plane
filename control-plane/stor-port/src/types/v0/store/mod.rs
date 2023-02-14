pub mod child;
pub mod definitions;
pub mod nexus;
pub mod nexus_child;
pub mod nexus_persistence;
pub mod node;
pub mod pool;
pub mod registry;
pub mod replica;
pub mod switchover;
pub mod volume;
pub mod watch;

use crate::types::v0::openapi::models;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use strum_macros::Display;

/// Enum defining the various states that a resource spec can be in.
#[derive(Serialize, Deserialize, Debug, Clone, Display, PartialEq)]
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
impl<T> From<SpecStatus<T>> for models::SpecStatus {
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
    /// Check if resource is being created.
    pub fn creating(&self) -> bool {
        self == &Self::Creating
    }
    /// Check if resource is created.
    pub fn created(&self) -> bool {
        matches!(self, &Self::Created(_))
    }
    /// Check if resource is being deleted.
    pub fn deleting(&self) -> bool {
        self == &Self::Deleting
    }
    /// Check if resource is deleted.
    pub fn deleted(&self) -> bool {
        self == &Self::Deleted
    }
    /// Check if resource is being deleted or is deleted.
    pub fn deleting_or_deleted(&self) -> bool {
        self.deleting() || self.deleted()
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
    /// Allow this operation while deleting a spec.
    fn allow_op_deleting(&mut self, _operation: &Operation) -> bool {
        false
    }
}

/// Sequence operations for a resource without locking it
/// Allows for multiple reconciliation operation steps to be executed in sequence whilst
/// blocking access from front-end operations (rest)
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct OperationSequence {
    uuid: String,
    state: OperationSequenceState,
    log_error: bool,
}
impl OperationSequence {
    /// Create new `Self` with a uuid for observability
    pub fn new(uuid: impl Into<String>) -> Self {
        Self {
            uuid: uuid.into(),
            state: Default::default(),
            log_error: true,
        }
    }
}

/// Sequence operations
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub enum OperationSequenceState {
    /// None in progress
    Idle,
    /// An single exclusive operation (openapi driven)
    Exclusive,
    /// Compound Operations as part of a reconcile algorithm
    /// todo: If we have multiple concurrent reconcile loops, then we'll need an ID to
    /// distinguish between them and avoid concurrent updates
    Reconcile { active: bool },
}
impl Default for OperationSequenceState {
    fn default() -> Self {
        Self::Idle
    }
}

/// Operations are locked
pub trait AsOperationSequencer {
    fn as_ref(&self) -> &OperationSequence;
    fn as_mut(&mut self) -> &mut OperationSequence;
}

pub trait OperationSequencer: Debug + Clone {
    /// Check if the transition is valid.
    fn valid(&self, next: OperationSequenceState) -> bool;
    /// Try to transition from current to next state.
    fn transition(&self, next: OperationSequenceState) -> Result<OperationSequenceState, bool>;
    /// Sequence an operation using the provided `OperationMode`.
    /// It returns the state which must be used to revert this operation.
    fn sequence(&self, mode: OperationMode) -> Result<OperationSequenceState, bool>;
    /// Complete the operation sequenced using the provided `OperationMode`.
    fn complete(&self, revert: OperationSequenceState);
}

/// Exclusive operations must be performed one at a time.
/// A reconcile compound operation can be comprised of multiple steps
/// A reconcile start operation must first be issued, followed by 1-N Single Step Operations
#[derive(Debug, Copy, Clone)]
pub enum OperationMode {
    /// Start Exclusive operation
    Exclusive,
    /// Start Reconcile Step operation that follows a ReconcileStart
    ReconcileStep,
    /// Start Reconcile Compound operation
    ReconcileStart,
}

impl OperationMode {
    /// Transform this operation into a sequence to transition to
    pub fn apply(&self) -> OperationSequenceState {
        match self {
            OperationMode::Exclusive => OperationSequenceState::Exclusive,
            OperationMode::ReconcileStep => OperationSequenceState::Reconcile { active: true },
            OperationMode::ReconcileStart => OperationSequenceState::Reconcile { active: false },
        }
    }
}

impl OperationSequence {
    /// Check if the transition is valid
    pub fn valid(&self, next: OperationSequenceState) -> bool {
        match self.state {
            OperationSequenceState::Idle => {
                matches!(
                    next,
                    OperationSequenceState::Exclusive
                        | OperationSequenceState::Reconcile { active: false }
                        | OperationSequenceState::Reconcile { active: true }
                )
            }
            OperationSequenceState::Exclusive => {
                matches!(next, OperationSequenceState::Idle)
            }
            OperationSequenceState::Reconcile { active: true } => {
                matches!(
                    next,
                    OperationSequenceState::Idle
                        | OperationSequenceState::Reconcile { active: false }
                )
            }
            OperationSequenceState::Reconcile { active: false } => {
                matches!(
                    next,
                    OperationSequenceState::Idle
                        | OperationSequenceState::Reconcile { active: true }
                )
            }
        }
    }
    /// Try to transition from current to next state.
    pub fn transition(
        &mut self,
        next: OperationSequenceState,
    ) -> Result<OperationSequenceState, bool> {
        if self.valid(next) {
            let previous = self.state;
            self.state = next;
            self.log_error = true;
            Ok(previous)
        } else {
            let first_error = self.log_error;
            self.log_error = false;
            Err(first_error)
        }
    }
    /// Sequence an operation using the provided `OperationMode`.
    /// It returns the state which must be used to revert this operation.
    pub fn sequence(&mut self, mode: OperationMode) -> Result<OperationSequenceState, bool> {
        self.transition(mode.apply())
    }
    /// Complete the operation sequenced using the provided `OperationMode`.
    pub fn complete(&mut self, revert: OperationSequenceState) {
        if self.transition(revert).is_err() {
            debug_assert!(false, "Invalid revert from '{self:?}' to '{revert:?}'");
            self.state = OperationSequenceState::Idle;
        }
    }
}
