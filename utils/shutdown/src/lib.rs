use lazy_static::lazy_static;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{signal::unix::SignalKind, sync::oneshot};

type ShutdownSync = Arc<Mutex<Shutdown>>;

/// Shutdown Event handler.
pub struct Shutdown {
    /// Listeners awaiting for the shutdown.
    listeners: Vec<oneshot::Sender<SignalKind>>,
    /// Whether we've already received the shutdown signal and which signal triggered it.
    shutdown: Option<SignalKind>,
}
impl Shutdown {
    /// Get a sync wrapper of `Self`.
    /// The internal task to listen on the shutdown event is scheduled.
    fn new_sync(event: impl ShutdownEvent + 'static) -> ShutdownSync {
        let this = Arc::new(Mutex::new(Self::new()));
        let this_clone = this.clone();
        tokio::spawn(async move { Self::run(this_clone, event).await });
        this
    }
    fn new() -> Self {
        Shutdown {
            listeners: Vec::new(),
            shutdown: None,
        }
    }
    /// Get a shutdown channel to await on or None if the shutdown event has already been received.
    fn shutdown_chan(&mut self) -> Result<oneshot::Receiver<SignalKind>, SignalKind> {
        if let Some(event) = self.shutdown {
            Err(event)
        } else {
            let (send, receive) = oneshot::channel();
            self.listeners.push(send);
            Ok(receive)
        }
    }
    /// Run the main waiting loop that waits for the reception of SIGINT or SIGTERM.
    /// When any of them is received the listeners are notified.
    async fn run(this: ShutdownSync, event: impl ShutdownEvent) {
        let kind = event.wait().await;

        let mut this = this.lock().expect("not poisoned");
        this.shutdown = Some(kind);

        for sender in std::mem::take(&mut this.listeners) {
            // It's ok if the receiver has already been dropped.
            sender.send(kind).ok();
        }
    }
    /// Returns a future that completes when a shutdown event has been received.
    /// The output of the future is signal which triggered the shutdown.
    /// None is returned if we failed to receive the event from the internal task.
    /// Shutdown events: INT|TERM.
    pub fn wait_sig() -> impl Future<Output = Option<SignalKind>> {
        Self::wait_int_term()
    }
    /// Helper async fn over `Self::wait` with no return.
    pub async fn wait() {
        let _ = Self::wait_sig().await;
    }
    fn wait_int_term() -> impl Future<Output = Option<SignalKind>> {
        lazy_static! {
            static ref TERM: ShutdownSync = Shutdown::new_sync(IntTermEvent {});
        }
        let chan = TERM.lock().expect("not poisoned").shutdown_chan();
        async move {
            match chan {
                Ok(wait) => wait.await.ok(),
                Err(signal) => Some(signal),
            }
        }
    }
}

/// Internal Shutdown Event which returns which signal triggered it.
#[async_trait::async_trait]
trait ShutdownEvent: Send + Sync {
    async fn wait(&self) -> SignalKind;
}

/// Shutdown Event when INT | TERM are received.
struct IntTermEvent {}
#[async_trait::async_trait]
impl ShutdownEvent for IntTermEvent {
    async fn wait(&self) -> SignalKind {
        let mut sig_int =
            tokio::signal::unix::signal(SignalKind::interrupt()).expect("to register SIGINT");
        let mut sig_term =
            tokio::signal::unix::signal(SignalKind::terminate()).expect("to register SIGTERM");

        let kind = tokio::select! {
            _ = sig_int.recv() => {
                tracing::warn!(signal = ?SignalKind::interrupt(), "Signalled");
                SignalKind::interrupt()
            },
            _ = sig_term.recv() => {
                tracing::warn!(signal = ?SignalKind::terminate(), "Signalled");
                SignalKind::terminate()
            },
        };

        kind
    }
}

#[cfg(test)]
mod tests {
    use crate::{Shutdown, ShutdownEvent, ShutdownSync};
    use lazy_static::lazy_static;
    use std::future::Future;
    use tokio::signal::unix::SignalKind;

    impl Shutdown {
        fn wait_test() -> impl Future<Output = Option<SignalKind>> {
            lazy_static! {
                static ref TERM: ShutdownSync = Shutdown::new_sync(TestEvent {});
            }
            let chan = TERM.lock().expect("not poisoned").shutdown_chan();
            async move {
                match chan {
                    Ok(wait) => wait.await.ok(),
                    Err(signal) => Some(signal),
                }
            }
        }
    }

    struct TestEvent {}
    #[async_trait::async_trait]
    impl ShutdownEvent for TestEvent {
        async fn wait(&self) -> SignalKind {
            SignalKind::alarm()
        }
    }

    #[tokio::test]
    async fn shutdown() {
        assert_eq!(
            format!("{:?}", Shutdown::wait_test().await),
            format!("{:?}", Some(SignalKind::alarm()))
        );
    }
}
