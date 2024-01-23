/// Type definitions.
pub mod types;

/// Pstor-proxy v1 types
pub mod v1 {
    mod all {
        tonic::include_proto!("pstor_proxy.v1");
    }

    /// Pstor-proxy frontend node registration grpc module.
    pub mod registration {
        pub use super::all::{
            registration_server::{Registration, RegistrationServer},
            DeregisterRequest, RegisterRequest,
        };
    }

    /// Pstor-proxy frontend node grpc module.
    pub mod frontend_node {
        pub use super::all::{
            frontend_node_grpc_server::{FrontendNodeGrpc, FrontendNodeGrpcServer},
            get_frontend_nodes_reply, get_frontend_nodes_request, FrontendNode, FrontendNodeSpec,
            FrontendNodeState, FrontendNodes, GetFrontendNodesReply, GetFrontendNodesRequest,
        };
    }

    /// Pstor-proxy commmon grpc module.
    pub mod common {
        pub use super::all::{
            FrontendNodeFilter, Pagination, ReplyError, ReplyErrorKind, ResourceKind,
            StringMapValue,
        };
    }
}
