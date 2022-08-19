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
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
};
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

/// Trait which allows a UUID to be returned as the associated type Id.
pub trait ResourceUuid {
    type Id;
    fn uuid(&self) -> Self::Id;
}

/// Sequence operations for a resource without locking it
/// Allows for multiple reconciliation operation steps to be executed in sequence whilst
/// blocking access from front-end operations (rest)
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct OperationSequence {
    uuid: String,
    state: OperationSequenceState,
}
impl OperationSequence {
    /// Create new `Self` with a uuid for observability
    pub fn new(uuid: impl Into<String>) -> Self {
        Self {
            uuid: uuid.into(),
            state: Default::default(),
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

pub trait OperationSequencer: std::fmt::Debug + Clone {
    /// Check if the transition is valid.
    fn valid(&self, next: OperationSequenceState) -> bool;
    /// Try to transition from current to next state.
    fn transition(&self, next: OperationSequenceState) -> Option<OperationSequenceState>;
    /// Sequence an operation using the provided `OperationMode`.
    /// It returns the state which must be used to revert this operation.
    fn sequence(&self, mode: OperationMode) -> Option<OperationSequenceState>;
    /// Complete the operation sequenced using the provided `OperationMode`.
    fn complete(&self, revert: OperationSequenceState);
}

impl<T: AsOperationSequencer + std::fmt::Debug + Clone> OperationSequencer for ResourceMutex<T> {
    fn valid(&self, next: OperationSequenceState) -> bool {
        self.lock().as_mut().valid(next)
    }
    fn transition(&self, next: OperationSequenceState) -> Option<OperationSequenceState> {
        self.lock().as_mut().transition(next)
    }
    fn sequence(&self, mode: OperationMode) -> Option<OperationSequenceState> {
        self.lock().as_mut().sequence(mode)
    }
    fn complete(&self, revert: OperationSequenceState) {
        self.lock().as_mut().complete(revert);
    }
}

/// Operation Guard for a ResourceMutex<T> type.
pub type OperationGuardArc<T> = OperationGuard<ResourceMutex<T>, T>;
/// Ref-counted resource wrapped with a mutex.
#[derive(Debug, Clone)]
pub struct ResourceMutex<T> {
    inner: Arc<ResourceMutexInner<T>>,
}
/// Inner Resource which holds the mutex and an immutable value for peeking
/// into immutable fields such as identification fields.
#[derive(Debug)]
pub struct ResourceMutexInner<T> {
    resource: Mutex<T>,
    immutable_peek: T,
}
impl<T: Clone> From<T> for ResourceMutex<T> {
    fn from(resource: T) -> Self {
        let immutable_peek = resource.clone();
        let resource = Mutex::new(resource);
        Self {
            inner: Arc::new(ResourceMutexInner {
                resource,
                immutable_peek,
            }),
        }
    }
}
impl<T> Deref for ResourceMutex<T> {
    type Target = Mutex<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner.resource
    }
}
impl<T: Clone> ResourceMutex<T> {
    /// Peek the initial resource value without locking.
    /// # Note:
    /// This is only useful for immutable fields, such as the resource identifier.
    pub fn immutable_peek(&self) -> &T {
        &self.inner.immutable_peek
    }
}

impl<T: OperationSequencer, R> Deref for OperationGuard<T, R> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl<T: OperationSequencer, R> DerefMut for OperationGuard<T, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// It unlocks the sequence lock on drop.
#[derive(Debug)]
pub struct OperationGuard<T: OperationSequencer, R> {
    inner: T,
    inner_value: R,
    mode: OperationMode,
    locked: Option<OperationSequenceState>,
}
impl<T: OperationSequencer + Sized, R> OperationGuard<T, R> {
    /// Get a copy of the `OperationMode` constrained by this Guard.
    pub fn mode(&self) -> OperationMode {
        self.mode
    }
    fn unlock(&mut self) {
        if let Some(revert) = self.locked.take() {
            self.inner.complete(revert);
        }
    }
    /// Peek at the resource without locking.
    /// Note, this value may be outdated *During* an operation, and so must not be used to
    /// inspect fields which are being mutated.
    /// To inspect fields being mutated, please use the locked resource itself.
    pub fn peek(&self) -> &R {
        &self.inner_value
    }
    /// Create operation Guard for the resource with the operation mode
    pub fn try_sequence(
        resource: &T,
        value: fn(&T) -> R,
        mode: OperationMode,
    ) -> Result<Self, String> {
        // use result variable to make sure the mutex's temporary guard is dropped
        match resource.sequence(mode) {
            Some(revert) => Ok(Self {
                inner: resource.clone(),
                inner_value: value(resource),
                mode,
                locked: Some(revert),
            }),
            None => Err(format!(
                "Cannot transition from '{:?}' to '{:?}'",
                resource,
                mode.apply()
            )),
        }
    }
}

pub trait UpdateInnerValue {
    fn update(&mut self);
}
impl<R: Clone + Debug + AsOperationSequencer> UpdateInnerValue
    for OperationGuard<ResourceMutex<R>, R>
{
    fn update(&mut self) {
        self.inner_value = self.inner.lock().clone();
    }
}

impl<T: OperationSequencer + Sized, R> Drop for OperationGuard<T, R> {
    fn drop(&mut self) {
        self.unlock();
    }
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
    fn apply(&self) -> OperationSequenceState {
        match self {
            OperationMode::Exclusive => OperationSequenceState::Exclusive,
            OperationMode::ReconcileStep => OperationSequenceState::Reconcile { active: true },
            OperationMode::ReconcileStart => OperationSequenceState::Reconcile { active: false },
        }
    }
}

impl OperationSequence {
    /// Check if the transition is valid
    fn valid(&self, next: OperationSequenceState) -> bool {
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
    fn transition(&mut self, next: OperationSequenceState) -> Option<OperationSequenceState> {
        if self.valid(next) {
            let previous = self.state;
            self.state = next;
            Some(previous)
        } else {
            None
        }
    }
    /// Sequence an operation using the provided `OperationMode`.
    /// It returns the state which must be used to revert this operation.
    fn sequence(&mut self, mode: OperationMode) -> Option<OperationSequenceState> {
        self.transition(mode.apply())
    }
    /// Complete the operation sequenced using the provided `OperationMode`.
    fn complete(&mut self, revert: OperationSequenceState) {
        if self.transition(revert).is_none() {
            debug_assert!(false, "Invalid revert from '{:?}' to '{:?}'", self, revert);
            self.state = OperationSequenceState::Idle;
        }
    }
}

/// Tracing simple string messages with resource specific information
/// eg, volume.uuid for volumes and replica.uuid for replicas
pub trait TraceStrLog {
    fn error(&self, message: &str);
    fn warn(&self, message: &str);
    fn info(&self, message: &str);
    fn debug(&self, message: &str);
    fn trace(&self, message: &str);
}

/// Execute code within a resource specific span which contains resource specific information, such
/// as volume.uuid for volumes and replica.uuid for replicas
/// # Example:
/// let volume = VolumeSpec::default();
/// volume.warn_span(|| tracing::warn!("This volume is not online"));
pub trait TraceSpan {
    fn error_span<F: FnOnce()>(&self, f: F);
    fn warn_span<F: FnOnce()>(&self, f: F);
    fn info_span<F: FnOnce()>(&self, f: F);
    fn debug_span<F: FnOnce()>(&self, f: F);
    fn trace_span<F: FnOnce()>(&self, f: F);
}

/// Implements `TraceStrLog` for the given $type
/// $log_macro is the logging fn, provided as a macro so we can statically specify the log level
/// $log_macro: ($Self:tt, $Level:expr, $Message:tt)
#[macro_export]
macro_rules! impl_trace_str_log {
    ($log_macro:tt, $type:tt) => {
        impl crate::types::v0::store::TraceStrLog for $type {
            fn error(&self, message: &str) {
                $log_macro!(self, tracing::Level::ERROR, message);
            }
            fn warn(&self, message: &str) {
                $log_macro!(self, tracing::Level::WARN, message);
            }
            fn info(&self, message: &str) {
                $log_macro!(self, tracing::Level::INFO, message);
            }
            fn debug(&self, message: &str) {
                $log_macro!(self, tracing::Level::DEBUG, message);
            }
            fn trace(&self, message: &str) {
                $log_macro!(self, tracing::Level::TRACE, message);
            }
        }
        impl crate::types::v0::store::TraceStrLog for OperationGuardArc<$type> {
            fn error(&self, message: &str) {
                let peek = self.peek();
                $log_macro!(peek, tracing::Level::ERROR, message);
            }
            fn warn(&self, message: &str) {
                let peek = self.peek();
                $log_macro!(peek, tracing::Level::WARN, message);
            }
            fn info(&self, message: &str) {
                let peek = self.peek();
                $log_macro!(peek, tracing::Level::INFO, message);
            }
            fn debug(&self, message: &str) {
                let peek = self.peek();
                $log_macro!(peek, tracing::Level::DEBUG, message);
            }
            fn trace(&self, message: &str) {
                let peek = self.peek();
                $log_macro!(peek, tracing::Level::TRACE, message);
            }
        }
    };
}

/// Implements `TraceSpan` for the given $type
/// span_macro is the resource specific fn, provided as a macro so we can statically specify
/// the log level span_macro: ($Self:tt, $Level:expr, $func:expr)
#[macro_export]
macro_rules! impl_trace_span {
    ($span_macro:tt, $type:tt) => {
        impl crate::types::v0::store::TraceSpan for $type {
            fn error_span<F: FnOnce()>(&self, f: F) {
                $span_macro!(self, tracing::Level::ERROR, f);
            }
            fn warn_span<F: FnOnce()>(&self, f: F) {
                $span_macro!(self, tracing::Level::WARN, f);
            }
            fn info_span<F: FnOnce()>(&self, f: F) {
                $span_macro!(self, tracing::Level::INFO, f);
            }
            fn debug_span<F: FnOnce()>(&self, f: F) {
                $span_macro!(self, tracing::Level::DEBUG, f);
            }
            fn trace_span<F: FnOnce()>(&self, f: F) {
                $span_macro!(self, tracing::Level::TRACE, f);
            }
        }
        impl crate::types::v0::store::TraceSpan for OperationGuardArc<$type> {
            fn error_span<F: FnOnce()>(&self, f: F) {
                let peek = self.peek();
                $span_macro!(peek, tracing::Level::ERROR, f);
            }
            fn warn_span<F: FnOnce()>(&self, f: F) {
                let peek = self.peek();
                $span_macro!(peek, tracing::Level::WARN, f);
            }
            fn info_span<F: FnOnce()>(&self, f: F) {
                let peek = self.peek();
                $span_macro!(peek, tracing::Level::INFO, f);
            }
            fn debug_span<F: FnOnce()>(&self, f: F) {
                let peek = self.peek();
                $span_macro!(peek, tracing::Level::DEBUG, f);
            }
            fn trace_span<F: FnOnce()>(&self, f: F) {
                let peek = self.peek();
                $span_macro!(peek, tracing::Level::TRACE, f);
            }
        }
    };
}
