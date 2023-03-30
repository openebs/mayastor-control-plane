use stor_port::types::v0::{store::node::NodeSpec, transport::VolumeId};

use crate::controller::resources::OperationGuardArc;

use std::{collections::HashSet, time::SystemTime};

impl OperationGuardArc<NodeSpec> {
    /// Get the draining timestamp on this node.
    pub(crate) async fn node_draining_timestamp(&self) -> Option<SystemTime> {
        let locked_node = self.lock();
        locked_node.draining_timestamp()
    }

    /// Get the number of draining volumes on this node.
    pub(crate) async fn node_draining_volume_count(&self) -> usize {
        let locked_node = self.lock();
        locked_node.draining_volume_count()
    }

    /// Get the draining volumes on this node.
    pub(crate) async fn node_draining_volumes(&self) -> HashSet<VolumeId> {
        let locked_node = self.lock();
        locked_node.draining_volumes()
    }

    /// Set the draining timestamp on this node.
    pub(crate) async fn set_draining_timestamp_if_none(&self) {
        let mut locked_node = self.lock();
        locked_node.set_draining_timestamp_if_none();
    }
}
