use crate::controller::resources::ResourceUid;
use stor_port::types::v0::{store::app_node::AppNodeSpec, transport::AppNodeId};

impl ResourceUid for AppNodeSpec {
    type Uid = AppNodeId;
    fn uid(&self) -> &Self::Uid {
        &self.id
    }
    fn rkind(&self) -> stor_port::transport_api::ResourceKind {
        stor_port::transport_api::ResourceKind::AppNode
    }
}
