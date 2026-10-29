use parking_lot::{Mutex, MutexGuard};
use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
    sync::Arc,
};
use stor_port::types::v0::store::{
    OperationMode, OperationSequence, OperationSequenceState, OperationSequencer,
};

mod app_node;
mod migration;
mod nexus;
mod node;
mod pool;
mod replica;
mod volume;

/// The internal operations interface for all resources.
pub(crate) mod operations;
/// Generic interface implemented for all resources.
pub(crate) mod operations_helper;
/// Generic resources map.
pub(crate) mod resource_map;

pub(crate) use volume::VolumeHealthWatcher;

impl<T: OperationSequencer + Sized, R> Drop for OperationGuard<T, R> {
    fn drop(&mut self) {
        self.unlock();
    }
}
impl<T: std::fmt::Debug + Clone> OperationSequencer for ResourceMutex<T> {
    fn valid(&self, next: OperationSequenceState) -> bool {
        self.inner.resource.lock().sequence.valid(next)
    }
    fn transition(&self, next: OperationSequenceState) -> Result<OperationSequenceState, bool> {
        self.inner.resource.lock().sequence.transition(next)
    }
    fn sequence(&self, mode: OperationMode) -> Result<OperationSequenceState, bool> {
        self.inner.resource.lock().sequence.sequence(mode)
    }
    fn complete(&self, revert: OperationSequenceState) {
        let mut data = self.inner.resource.lock();
        data.sequence.complete(revert);
        // The operation sequence just freed up: wake the next live waiter (if any) so it can
        // attempt to acquire, instead of waiting out its poll interval. Dropped receivers are
        // skipped. This happens under the same lock as the sequence (and the spec value), so
        // there's no window for a waiter to register and miss this wake-up (no lost wake-up).
        while let Some(tx) = data.waiters.pop_front() {
            if tx.send(()).is_ok() {
                break;
            }
        }
    }
}

/// Operation Guard for a ResourceMutex<T> type.
pub(crate) type OperationGuardArc<T> = OperationGuard<ResourceMutex<T>, T>;
/// Ref-counted resource wrapped with a mutex.
#[derive(Debug, Clone)]
pub(crate) struct ResourceMutex<T> {
    inner: Arc<ResourceMutexInner<T>>,
}
impl<T: Clone + ResourceUid> ResourceUid for ResourceMutex<T> {
    type Uid = T::Uid;

    fn uid(&self) -> &Self::Uid {
        self.immutable_ref().uid()
    }
}
impl<T: Clone + ResourceUid> ResourceUid for Resource<T> {
    type Uid = T::Uid;

    fn uid(&self) -> &Self::Uid {
        self.inner.uid()
    }
}
/// The mutable resource value together with its operation sequence state and FIFO waiters,
/// all behind a single lock.
///
/// Colocating these lets a single `resource` lock acquisition both sequence an operation and
/// snapshot the spec value (what every acquirer and reconciler pays, even when there's no work
/// to do), instead of taking one lock to sequence and a second to snapshot. It also keeps
/// "acquire-or-enqueue" (`try_guard`/`try_guard_or_wait`) atomic with "complete-and-wake"
/// (`OperationSequencer::complete`), closing the lost-wake-up race that a separate waiter lock
/// would open. FIFO ordering is best-effort: a non-waiting acquirer (`operation_guard_mode`)
/// may still barge past queued waiters since it doesn't consult the queue.
#[derive(Debug)]
struct ResourceData<T> {
    value: T,
    sequence: OperationSequence,
    waiters: std::collections::VecDeque<tokio::sync::oneshot::Sender<()>>,
}
impl<T> ResourceData<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            sequence: OperationSequence::default(),
            waiters: std::collections::VecDeque::new(),
        }
    }
}

/// Inner Resource which holds the mutex and an immutable value for peeking
/// into immutable fields such as identification fields.
#[derive(Debug)]
pub(crate) struct ResourceMutexInner<T> {
    resource: Mutex<ResourceData<T>>,
    immutable_peek: Arc<T>,
}
impl<T: Clone> From<T> for ResourceMutex<T> {
    fn from(resource: T) -> Self {
        let immutable_peek = Arc::new(resource.clone());
        let resource = Mutex::new(ResourceData::new(resource));
        Self {
            inner: Arc::new(ResourceMutexInner {
                resource,
                immutable_peek,
            }),
        }
    }
}
/// A locked guard over a [`ResourceMutex`]'s spec value.
///
/// Dereferences to the spec `T`, so it's a drop-in for the previous `Mutex<T>` guard. The
/// operation sequence state and waiter queue colocated under the same lock are intentionally
/// not exposed here; they're driven through `OperationSequencer`/`try_guard*`.
pub(crate) struct ResourceGuard<'a, T> {
    inner: MutexGuard<'a, ResourceData<T>>,
}
impl<T> Deref for ResourceGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner.value
    }
}
impl<T> DerefMut for ResourceGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner.value
    }
}
impl<T> ResourceMutex<T> {
    /// Lock the resource and return a guard over its spec value.
    pub(crate) fn lock(&self) -> ResourceGuard<'_, T> {
        ResourceGuard {
            inner: self.inner.resource.lock(),
        }
    }
}
impl<T: Clone> ResourceMutex<T> {
    /// Peek the initial resource value without locking.
    /// # Note:
    /// This is only useful for immutable fields, such as the resource identifier.
    pub(crate) fn immutable_ref(&self) -> &Arc<T> {
        &self.inner.immutable_peek
    }
    /// Peek the initial resource value without locking.
    /// # Note:
    /// This is only useful for immutable fields, such as the resource identifier.
    /// Useful over `as_ref` as it returns the `Arc` directly.
    pub(crate) fn immutable_arc(&self) -> Arc<T> {
        self.inner.immutable_peek.clone()
    }
}
impl<T: Clone + Debug> ResourceMutex<T> {
    /// Attempt to acquire the sequenced operation guard for `mode`, without enqueuing.
    ///
    /// On success the operation is sequenced and the spec value snapshotted under a single
    /// lock acquisition.
    ///
    /// On failure returns whether this conflict should be logged (only the first conflict
    /// in a run is logged). This is the non-waiting fast path taken by every operation and
    /// reconciler, including when there's no work to do.
    pub(crate) fn try_guard(&self, mode: OperationMode) -> Result<OperationGuardArc<T>, bool> {
        let mut data = self.inner.resource.lock();
        match data.sequence.sequence(mode) {
            Ok(revert) => {
                let inner_value = data.value.clone();
                Ok(OperationGuard {
                    inner: self.clone(),
                    inner_value,
                    mode,
                    locked: Some(revert),
                })
            }
            Err(log) => Err(log),
        }
    }
    /// Attempt to acquire the sequenced operation guard for `mode`, enqueuing on failure.
    ///
    /// On success returns the guard (sequence + spec snapshot under a single lock).
    ///
    /// On failure the caller is atomically enqueued as a FIFO waiter and the returned receiver
    /// resolves when a guard is next dropped.
    pub(crate) fn try_guard_or_register(
        &self,
        mode: OperationMode,
    ) -> Result<OperationGuardArc<T>, tokio::sync::oneshot::Receiver<()>> {
        let mut data = self.inner.resource.lock();
        match data.sequence.sequence(mode) {
            Ok(revert) => {
                let inner_value = data.value.clone();
                Ok(OperationGuard {
                    inner: self.clone(),
                    inner_value,
                    mode,
                    locked: Some(revert),
                })
            }
            Err(_) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                data.waiters.push_back(tx);
                Err(rx)
            }
        }
    }
}

/// Ref-counted resource.
#[derive(Debug, Clone)]
pub(crate) struct Resource<T> {
    inner: Arc<T>,
}
impl<T> Deref for Resource<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl<T: Clone> Resource<T> {
    pub(crate) fn inner(&self) -> &T {
        self.deref()
    }
}
impl<T: Clone> From<T> for Resource<T> {
    fn from(resource: T) -> Self {
        Self {
            inner: Arc::new(resource),
        }
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

impl<T: OperationSequencer + Sized, R> AsRef<R> for OperationGuard<T, R> {
    fn as_ref(&self) -> &R {
        self.peek()
    }
}
/// It unlocks the sequence lock on drop.
#[derive(Debug)]
pub(crate) struct OperationGuard<T: OperationSequencer, R> {
    inner: T,
    inner_value: R,
    #[allow(dead_code)]
    mode: OperationMode,
    locked: Option<OperationSequenceState>,
}
impl<T: OperationSequencer + Sized, R> OperationGuard<T, R> {
    #[allow(dead_code)]
    /// Get a copy of the `OperationMode` constrained by this Guard.
    pub(crate) fn mode(&self) -> OperationMode {
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
    fn peek(&self) -> &R {
        &self.inner_value
    }
}

impl<T: Clone + Debug, R> OperationGuard<ResourceMutex<T>, R> {
    /// Get the ResourceMutex for the concerned resource.
    pub fn resource(&self) -> &ResourceMutex<T> {
        &self.inner
    }
}

/// Tracing simple string messages with resource specific information
/// eg, volume.uuid for volumes and replica.uuid for replicas
pub(crate) trait TraceStrLog {
    /// Logs an error message.
    fn error(&self, message: &str);
    /// Logs a wan message.
    fn warn(&self, message: &str);
    /// Logs an info message.
    fn info(&self, message: &str);
    /// Logs a debug message.
    fn debug(&self, message: &str);
    /// Logs a trace message.
    fn trace(&self, message: &str);
}

/// Execute code within a resource specific span which contains resource specific information, such
/// as volume.uuid for volumes and replica.uuid for replicas
/// # Example:
/// let volume = VolumeSpec::default();
/// volume.warn_span(|| tracing::warn!("This volume is not online"));
pub(crate) trait TraceSpan {
    /// Run closure within an error span.
    fn error_span<F: FnOnce()>(&self, f: F);
    /// Run closure within a warn span.
    fn warn_span<F: FnOnce()>(&self, f: F);
    /// Run closure within an info span.
    fn info_span<F: FnOnce()>(&self, f: F);
    /// Run closure within a debug span.
    fn debug_span<F: FnOnce()>(&self, f: F);
    /// Run closure within a trace span.
    fn trace_span<F: FnOnce()>(&self, f: F);
}

/// Implements `TraceStrLog` for the given $type
/// $log_macro is the logging fn, provided as a macro so we can statically specify the log level
/// $log_macro: ($Self:tt, $Level:expr, $Message:tt)
#[macro_export]
macro_rules! impl_trace_str_log {
    ($log_macro:tt, $type:tt) => {
        impl $crate::controller::resources::TraceStrLog for $type {
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
        impl $crate::controller::resources::TraceStrLog
            for $crate::controller::resources::OperationGuardArc<$type>
        {
            fn error(&self, message: &str) {
                let peek = self.as_ref();
                $log_macro!(peek, tracing::Level::ERROR, message);
            }
            fn warn(&self, message: &str) {
                let peek = self.as_ref();
                $log_macro!(peek, tracing::Level::WARN, message);
            }
            fn info(&self, message: &str) {
                let peek = self.as_ref();
                $log_macro!(peek, tracing::Level::INFO, message);
            }
            fn debug(&self, message: &str) {
                let peek = self.as_ref();
                $log_macro!(peek, tracing::Level::DEBUG, message);
            }
            fn trace(&self, message: &str) {
                let peek = self.as_ref();
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
        impl $crate::controller::resources::TraceSpan for $type {
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
        impl $crate::controller::resources::TraceSpan
            for $crate::controller::resources::OperationGuardArc<$type>
        {
            fn error_span<F: FnOnce()>(&self, f: F) {
                let peek = self.as_ref();
                $span_macro!(peek, tracing::Level::ERROR, f);
            }
            fn warn_span<F: FnOnce()>(&self, f: F) {
                let peek = self.as_ref();
                $span_macro!(peek, tracing::Level::WARN, f);
            }
            fn info_span<F: FnOnce()>(&self, f: F) {
                let peek = self.as_ref();
                $span_macro!(peek, tracing::Level::INFO, f);
            }
            fn debug_span<F: FnOnce()>(&self, f: F) {
                let peek = self.as_ref();
                $span_macro!(peek, tracing::Level::DEBUG, f);
            }
            fn trace_span<F: FnOnce()>(&self, f: F) {
                let peek = self.as_ref();
                $span_macro!(peek, tracing::Level::TRACE, f);
            }
        }
    };
}

/// Update inner value interface.
pub(crate) trait UpdateInnerValue {
    /// Update the inner value.
    fn update(&mut self);
}
impl<R: Clone + std::fmt::Debug> UpdateInnerValue for OperationGuard<ResourceMutex<R>, R> {
    fn update(&mut self) {
        self.inner_value = self.inner.lock().clone();
    }
}

/// Trait which exposes a resource Uid for various types of resources.
pub(crate) trait ResourceUid {
    /// The associated type for Id.
    type Uid: Clone + Display;
    /// The id of the resource.
    fn uid(&self) -> &Self::Uid;
    fn uid_str(&self) -> String {
        self.uid().to_string()
    }
}
