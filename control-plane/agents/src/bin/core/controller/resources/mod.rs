use parking_lot::Mutex;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use stor_port::types::v0::store::{
    AsOperationSequencer, OperationMode, OperationSequenceState, OperationSequencer,
};

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

impl<T: OperationSequencer + Sized, R> Drop for OperationGuard<T, R> {
    fn drop(&mut self) {
        self.unlock();
    }
}
impl<T: AsOperationSequencer + std::fmt::Debug + Clone> OperationSequencer for ResourceMutex<T> {
    fn valid(&self, next: OperationSequenceState) -> bool {
        self.lock().as_mut().valid(next)
    }
    fn transition(&self, next: OperationSequenceState) -> Result<OperationSequenceState, bool> {
        self.lock().as_mut().transition(next)
    }
    fn sequence(&self, mode: OperationMode) -> Result<OperationSequenceState, bool> {
        self.lock().as_mut().sequence(mode)
    }
    fn complete(&self, revert: OperationSequenceState) {
        self.lock().as_mut().complete(revert);
    }
}

/// Operation Guard for a ResourceMutex<T> type.
pub(crate) type OperationGuardArc<T> = OperationGuard<ResourceMutex<T>, T>;
/// Ref-counted resource wrapped with a mutex.
#[derive(Debug, Clone)]
pub(crate) struct ResourceMutex<T> {
    inner: Arc<ResourceMutexInner<T>>,
}
/// Inner Resource which holds the mutex and an immutable value for peeking
/// into immutable fields such as identification fields.
#[derive(Debug)]
pub(crate) struct ResourceMutexInner<T> {
    resource: Mutex<T>,
    immutable_peek: Arc<T>,
}
impl<T: Clone> From<T> for ResourceMutex<T> {
    fn from(resource: T) -> Self {
        let immutable_peek = Arc::new(resource.clone());
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
    /// Create operation Guard for the resource with the operation mode
    pub(crate) fn try_sequence(
        resource: &T,
        value: fn(&T) -> R,
        mode: OperationMode,
    ) -> Result<Self, (String, bool)> {
        // use result variable to make sure the mutex's temporary guard is dropped
        match resource.sequence(mode) {
            Ok(revert) => Ok(Self {
                inner: resource.clone(),
                inner_value: value(resource),
                mode,
                locked: Some(revert),
            }),
            Err(log) => Err((
                format!(
                    "Cannot transition from '{:?}' to '{:?}'",
                    resource,
                    mode.apply()
                ),
                log,
            )),
        }
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
impl<R: Clone + std::fmt::Debug + AsOperationSequencer> UpdateInnerValue
    for OperationGuard<ResourceMutex<R>, R>
{
    fn update(&mut self) {
        self.inner_value = self.inner.lock().clone();
    }
}

/// Trait which exposes a resource Uid for various types of resources.
pub(crate) trait ResourceUid {
    /// The associated type for Id.
    type Uid: Clone;
    /// The id of the resource.
    fn uid(&self) -> &Self::Uid;
}
