//! Prometheus metrics endpoint for Mayastor control plane
//!
//! This module provides metrics about Mayastor node status, including:
//! - Node online/offline/unknown status
//! - Node cordoned state
//! - Node drain state

use super::*;
use actix_web::{HttpResponse, Responder};
use grpc::operations::node::traits::NodeOperations;
use prometheus::{
    core::{Collector, Desc},
    Encoder, GaugeVec, Opts, TextEncoder,
};
use std::sync::Mutex;
use stor_port::types::v0::{
    store::node::CordonDrainState,
    transport::{Filter, Node, NodeStatus},
};

/// Static registry for Prometheus collectors
static REGISTRY: once_cell::sync::Lazy<prometheus::Registry> =
    once_cell::sync::Lazy::new(prometheus::Registry::new);

/// Static collector for node metrics
static NODE_COLLECTOR: once_cell::sync::Lazy<Mutex<NodeMetricsCollector>> =
    once_cell::sync::Lazy::new(|| Mutex::new(NodeMetricsCollector::new()));

fn client() -> impl NodeOperations {
    core_grpc().node()
}

/// Node metrics collector that implements the Prometheus Collector trait
#[derive(Clone)]
pub(crate) struct NodeMetricsCollector {
    node_status: GaugeVec,
    node_cordoned: GaugeVec,
    node_drain_state: GaugeVec,
    descs: Vec<Desc>,
}

impl NodeMetricsCollector {
    /// Create a new NodeMetricsCollector with all metrics
    pub fn new() -> Self {
        let mut descs = Vec::new();

        // Node status metric: 0 = Unknown, 1 = Online, 2 = Offline
        let node_status_opts = Opts::new(
            "mayastor_node_status",
            "Status of mayastor node (0=Unknown, 1=Online, 2=Offline)",
        )
        .variable_labels(vec!["node".to_string()]);

        let node_status = GaugeVec::new(node_status_opts, &["node"])
            .expect("Failed to create node_status metric");
        descs.extend(node_status.desc().into_iter().cloned());

        // Node cordoned metric: 0 = Not Cordoned, 1 = Cordoned
        let node_cordoned_opts = Opts::new(
            "mayastor_node_cordoned",
            "Whether mayastor node is cordoned (0=No, 1=Yes)",
        )
        .variable_labels(vec!["node".to_string()]);

        let node_cordoned = GaugeVec::new(node_cordoned_opts, &["node"])
            .expect("Failed to create node_cordoned metric");
        descs.extend(node_cordoned.desc().into_iter().cloned());

        // Node drain state metric: 0 = None, 1 = Cordoned, 2 = Draining, 3 = Drained
        let node_drain_state_opts = Opts::new(
            "mayastor_node_drain_state",
            "Drain state of mayastor node (0=None, 1=Cordoned, 2=Draining, 3=Drained)",
        )
        .variable_labels(vec!["node".to_string()]);

        let node_drain_state = GaugeVec::new(node_drain_state_opts, &["node"])
            .expect("Failed to create node_drain_state metric");
        descs.extend(node_drain_state.desc().into_iter().cloned());

        Self {
            node_status,
            node_cordoned,
            node_drain_state,
            descs,
        }
    }

    /// Convert NodeStatus to metric value
    fn node_status_to_metric(status: &NodeStatus) -> f64 {
        match status {
            NodeStatus::Unknown => 0.0,
            NodeStatus::Online => 1.0,
            NodeStatus::Offline => 2.0,
        }
    }

    /// Check if node is cordoned (has any cordon/drain state)
    fn is_node_cordoned(node: &Node) -> f64 {
        if let Some(spec) = node.spec() {
            if spec.cordon_drain_state().is_some() {
                return 1.0;
            }
        }
        0.0
    }

    /// Convert CordonDrainState to metric value
    fn drain_state_to_metric(node: &Node) -> f64 {
        if let Some(spec) = node.spec() {
            if let Some(cordon_drain_state) = spec.cordon_drain_state() {
                return match cordon_drain_state {
                    CordonDrainState::Cordoned(_) => 1.0,
                    CordonDrainState::Draining(_) => 2.0,
                    CordonDrainState::Drained(_) => 3.0,
                };
            }
        }
        0.0
    }

    /// Update metrics with current node data
    async fn update_metrics(&self) -> Result<(), String> {
        // Fetch all nodes from the control plane
        let nodes = client()
            .get(Filter::None, false, None)
            .await
            .map_err(|e| format!("Failed to fetch nodes: {e}"))?
            .into_inner();

        // Update metrics for each node
        for node in nodes {
            let node_id = node.id().to_string();

            // Set node status metric
            if let Some(state) = node.state() {
                let status_value = Self::node_status_to_metric(state.status());
                self.node_status
                    .with_label_values(&[&node_id])
                    .set(status_value);
            } else {
                // If no state, mark as Unknown
                self.node_status.with_label_values(&[&node_id]).set(0.0);
            }

            // Set node cordoned metric
            let cordoned_value = Self::is_node_cordoned(&node);
            self.node_cordoned
                .with_label_values(&[&node_id])
                .set(cordoned_value);

            // Set node drain state metric
            let drain_state_value = Self::drain_state_to_metric(&node);
            self.node_drain_state
                .with_label_values(&[&node_id])
                .set(drain_state_value);
        }

        Ok(())
    }
}

impl Collector for NodeMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut metrics = Vec::new();
        metrics.extend(self.node_status.collect());
        metrics.extend(self.node_cordoned.collect());
        metrics.extend(self.node_drain_state.collect());
        metrics
    }
}

/// Initialize the metrics collector and register it with the Prometheus registry
pub(crate) fn init_metrics() {
    let collector = NODE_COLLECTOR.lock().unwrap().clone();
    REGISTRY.register(Box::new(collector)).unwrap_or_else(|e| {
        tracing::warn!("Failed to register node metrics collector: {}", e);
    });
}

/// Handler for the /metrics endpoint
///
/// This endpoint returns metrics in Prometheus text format
pub(crate) async fn metrics_handler() -> impl Responder {
    // Update metrics with latest node data
    let collector = NODE_COLLECTOR.lock().unwrap().clone();
    if let Err(e) = collector.update_metrics().await {
        tracing::error!("Failed to update node metrics: {}", e);
        return HttpResponse::InternalServerError().body(format!("Error updating metrics: {e}"));
    }

    // Gather metrics from the registry
    let metric_families = REGISTRY.gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => HttpResponse::Ok()
            .content_type("text/plain; version=0.0.4")
            .body(buffer),
        Err(e) => {
            tracing::error!("Failed to encode metrics: {}", e);
            HttpResponse::InternalServerError().body(format!("Error encoding metrics: {e}"))
        }
    }
}

/// Configure the metrics route with our custom handler
///
/// This overrides the auto-generated route from OpenAPI
pub(crate) fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.route("/metrics", actix_web::web::get().to(metrics_handler));
}

/// Implementation of the OpenAPI-generated Metrics trait for RestApi
///
/// Note: The OpenAPI code generator doesn't properly handle text/plain responses,
/// so it returns Result<(), RestError> instead of Result<String, RestError>.
/// This trait method is registered by the auto-generated handler first, but then
/// we override it in the configure() function above with metrics_handler().
#[async_trait::async_trait]
impl apis::actix_server::Metrics for super::RestApi {
    async fn get_metrics() -> Result<(), RestError<RestJsonError>> {
        // This stub implementation satisfies the compiler requirement that RestApi
        // implements the Metrics trait. The actual endpoint is handled by the
        // metrics_handler() function registered in configure() above, which
        // overrides this auto-generated handler.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_status_to_metric() {
        assert_eq!(
            NodeMetricsCollector::node_status_to_metric(&NodeStatus::Unknown),
            0.0
        );
        assert_eq!(
            NodeMetricsCollector::node_status_to_metric(&NodeStatus::Online),
            1.0
        );
        assert_eq!(
            NodeMetricsCollector::node_status_to_metric(&NodeStatus::Offline),
            2.0
        );
    }

    #[test]
    fn test_collector_creation() {
        let collector = NodeMetricsCollector::new();
        assert_eq!(collector.descs.len(), 3); // Should have 3 metric descriptors
    }
}
