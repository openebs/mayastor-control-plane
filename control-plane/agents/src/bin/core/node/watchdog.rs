use crate::node::service::Service;
use common_lib::types::v0::transport::NodeId;

/// Watchdog which must be pet within the deadline, otherwise
/// it triggers the `on_timeout` callback from the node `Service`.
#[derive(Debug, Clone)]
pub(crate) struct Watchdog {
    node_id: NodeId,
    deadline: std::time::Duration,
    timestamp: std::time::Instant,
    pet_chan: Option<tokio::sync::mpsc::Sender<()>>,
    service: Option<Service>,
}

impl Watchdog {
    /// New empty watchdog with a deadline timeout for node `node_id`.
    pub(crate) fn new(node_id: &NodeId, deadline: std::time::Duration) -> Self {
        Self {
            deadline,
            node_id: node_id.clone(),
            timestamp: std::time::Instant::now(),
            pet_chan: None,
            service: None,
        }
    }

    /// Get the deadline.
    pub(crate) fn deadline(&self) -> std::time::Duration {
        self.deadline
    }

    /// Get the last time the node was seen.
    pub(crate) fn timestamp(&self) -> std::time::Instant {
        self.timestamp
    }

    /// Arm watchdog with self timeout and execute error callback if the deadline is not met.
    pub(crate) fn arm(&mut self, service: Service) {
        tracing::debug!(node.id = %self.node_id, "Arming the node's watchdog");
        let (s, mut r) = tokio::sync::mpsc::channel(1);
        self.pet_chan = Some(s);
        self.service = Some(service.clone());
        let deadline = self.deadline;
        let id = self.node_id.clone();
        tokio::spawn(async move {
            loop {
                let result = tokio::time::timeout(deadline, r.recv()).await;
                match result {
                    Err(_) => Service::on_timeout(&service, &id).await,
                    Ok(None) => {
                        tracing::warn!(node.id = %id, "Stopping Watchdog for node");
                        break;
                    }
                    _ => (),
                }
            }
        });
    }

    /// Meet the deadline.
    pub(crate) async fn pet(&mut self) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
        self.timestamp = std::time::Instant::now();
        if let Some(chan) = &mut self.pet_chan {
            chan.send(()).await
        } else {
            // if the watchdog was stopped, then rearm it
            if let Some(service) = self.service.clone() {
                self.arm(service);
            }
            Ok(())
        }
    }
    /// Stop the watchdog.
    pub(crate) fn disarm(&mut self) {
        tracing::debug!(node.id = %self.node_id, "Disarming the node's watchdog");
        let _ = self.pet_chan.take();
    }
}
