use rpc::{
    v1::registration::{DeregisterRequest, RegisterRequest},
    v1_alpha::registration::{
        DeregisterRequest as V1AlphaDeregisterRequest, RegisterRequest as V1AlphaRegisterRequest,
    },
};
use std::str::FromStr;
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::{
        store::app_node::TransportCaps,
        transport::{
            node, Deregister, HostNqn, NexusVersion, NodeBugFixes, NodeFeatures, NodeId,
            NvmfTargetInfo, Register,
        },
    },
};

/// New type to wrap grpc ApiVersion type.
pub struct ApiVersion(pub rpc::v1::registration::ApiVersion);

/// Operations to be supported by the Registration Service.
#[tonic::async_trait]
pub trait RegistrationOperations: Send + Sync {
    /// Register a dataplane node to controlplane.
    async fn register(&self, req: &dyn RegisterInfo) -> Result<(), ReplyError>;
    /// Deregister a dataplane node to controlplane.
    async fn deregister(&self, req: &dyn DeregisterInfo) -> Result<(), ReplyError>;
}

/// Trait to be implemented for Register operation.
pub trait RegisterInfo: Send + Sync {
    /// Node Id of the IoEngine instance.
    fn node_id(&self) -> NodeId;
    /// Grpc endpoint of the IoEngine instance.
    fn grpc_endpoint(&self) -> String;
    /// Api-version supported by the dataplane.
    fn api_version(&self) -> Option<Vec<node::ApiVersion>>;
    /// Used to identify dataplane process restarts.
    fn instance_uuid(&self) -> Option<uuid::Uuid>;
    /// Used to identify dataplane nvme hostnqn.
    fn node_nqn(&self) -> Option<HostNqn>;
    /// Features of the io-engine.
    fn features(&self) -> Option<NodeFeatures>;
    /// Bugs fixed in the io-engine.
    fn bugfixes(&self) -> Option<NodeBugFixes>;
    /// Version of the io-engine.
    fn io_version(&self) -> Option<String>;
    /// Transport capabilities of the host the io-engine runs on.
    fn transport_caps(&self) -> Option<TransportCaps>;
    /// State of the io-engine's nvmf target.
    fn nvmf_target(&self) -> Option<NvmfTargetInfo>;
}

/// Trait to be implemented for Register operation.
pub trait DeregisterInfo: Send + Sync {
    /// Node Id of the IoEngine instance.
    fn node_id(&self) -> NodeId;
}

impl RegisterInfo for Register {
    fn node_id(&self) -> NodeId {
        self.id.clone()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.to_string()
    }

    fn api_version(&self) -> Option<Vec<node::ApiVersion>> {
        self.api_versions.clone()
    }

    fn instance_uuid(&self) -> Option<uuid::Uuid> {
        self.instance_uuid
    }

    fn node_nqn(&self) -> Option<HostNqn> {
        self.node_nqn.clone()
    }

    fn features(&self) -> Option<NodeFeatures> {
        self.features.clone()
    }

    fn bugfixes(&self) -> Option<NodeBugFixes> {
        self.bugfixes.clone()
    }

    fn io_version(&self) -> Option<String> {
        self.version.clone()
    }

    fn transport_caps(&self) -> Option<TransportCaps> {
        self.transport_caps.clone()
    }

    fn nvmf_target(&self) -> Option<NvmfTargetInfo> {
        self.nvmf_target.clone()
    }
}

impl RegisterInfo for RegisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.clone()
    }

    fn api_version(&self) -> Option<Vec<node::ApiVersion>> {
        Some(
            self.api_version
                .clone()
                .into_iter()
                .map(|v| {
                    ApiVersion(rpc::v1::registration::ApiVersion::try_from(v).unwrap_or_default())
                        .into()
                })
                .collect(),
        )
    }

    fn instance_uuid(&self) -> Option<uuid::Uuid> {
        self.instance_uuid
            .as_ref()
            .and_then(|u| uuid::Uuid::parse_str(u).ok())
    }

    fn node_nqn(&self) -> Option<HostNqn> {
        self.hostnqn.as_ref().and_then(|h| h.try_into().ok())
    }

    fn features(&self) -> Option<NodeFeatures> {
        self.features.as_ref().map(|features| NodeFeatures {
            asymmetric_namespace_access: Some(features.asymmetric_namespace_access),
            logical_volume_manager: features.logical_volume_manager,
            snapshot_rebuild: features.snapshot_rebuild,
            rdma_capable_io_engine: features.rdma_capable_io_engine,
            diskpool_encryption: features.diskpool_encryption,
            nexus_label_version: match features.nexus_label_version {
                0 | 1 => NexusVersion::V1,
                2 => NexusVersion::V2,
                // if it's greater, then assume it's the latest version we support, which is V2.
                _ => NexusVersion::V2,
            },
            grpc_tls: features.grpc_tls,
            fips: features.fips,
        })
    }

    fn bugfixes(&self) -> Option<NodeBugFixes> {
        self.bugfixes.as_ref().map(|bugfixes| NodeBugFixes {
            nexus_rebuild_replica_ancestry: bugfixes.nexus_rebuild_replica_ancestry,
        })
    }

    fn io_version(&self) -> Option<String> {
        self.version.clone()
    }

    fn transport_caps(&self) -> Option<TransportCaps> {
        self.transport_caps.as_ref().map(|caps| TransportCaps {
            rdma_hca_present: caps.rdma_hca_present,
            nvme_rdma_module_loaded: caps.nvme_rdma_module_loaded,
            // The io-engine reports its node's ANA capability through the features, not the
            // transport caps, so derive it from there.
            ana_capable: self
                .features
                .as_ref()
                .map(|f| f.asymmetric_namespace_access)
                .unwrap_or_default(),
        })
    }

    fn nvmf_target(&self) -> Option<NvmfTargetInfo> {
        self.nvmf_target.as_ref().map(|target| NvmfTargetInfo {
            interface: target.interface.clone(),
            address: target.address.clone(),
            tcp: target.tcp,
            rdma: target.rdma,
        })
    }
}

impl RegisterInfo for V1AlphaRegisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }

    fn grpc_endpoint(&self) -> String {
        self.grpc_endpoint.clone()
    }

    fn api_version(&self) -> Option<Vec<node::ApiVersion>> {
        // Older versions support only V0
        Some(vec![node::ApiVersion::V0])
    }

    fn instance_uuid(&self) -> Option<uuid::Uuid> {
        None
    }

    fn node_nqn(&self) -> Option<HostNqn> {
        None
    }

    fn features(&self) -> Option<NodeFeatures> {
        None
    }

    fn bugfixes(&self) -> Option<NodeBugFixes> {
        None
    }

    fn io_version(&self) -> Option<String> {
        None
    }

    fn transport_caps(&self) -> Option<TransportCaps> {
        None
    }

    fn nvmf_target(&self) -> Option<NvmfTargetInfo> {
        None
    }
}

impl DeregisterInfo for Deregister {
    fn node_id(&self) -> NodeId {
        self.id.clone()
    }
}

impl DeregisterInfo for DeregisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }
}

impl DeregisterInfo for V1AlphaDeregisterRequest {
    fn node_id(&self) -> NodeId {
        self.id.clone().into()
    }
}

impl TryFrom<&dyn RegisterInfo> for Register {
    type Error = ReplyError;
    fn try_from(register: &dyn RegisterInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            id: register.node_id(),
            grpc_endpoint: std::net::SocketAddr::from_str(&register.grpc_endpoint()).map_err(
                |error| {
                    Self::Error::invalid_argument(
                        ResourceKind::Node,
                        "register.grpc_endpoint",
                        error.to_string(),
                    )
                },
            )?,
            api_versions: register.api_version(),
            instance_uuid: register.instance_uuid(),
            node_nqn: register.node_nqn(),
            features: register.features(),
            bugfixes: register.bugfixes(),
            version: register.io_version(),
            transport_caps: register.transport_caps(),
            nvmf_target: register.nvmf_target(),
        })
    }
}

impl From<&dyn DeregisterInfo> for Deregister {
    fn from(register: &dyn DeregisterInfo) -> Self {
        Self {
            id: register.node_id(),
        }
    }
}

impl From<node::ApiVersion> for ApiVersion {
    fn from(v: node::ApiVersion) -> Self {
        match v {
            node::ApiVersion::V0 => Self(rpc::v1::registration::ApiVersion::V0),
            node::ApiVersion::V1 => Self(rpc::v1::registration::ApiVersion::V1),
        }
    }
}

impl From<ApiVersion> for node::ApiVersion {
    fn from(v: ApiVersion) -> Self {
        match v {
            ApiVersion(rpc::v1::registration::ApiVersion::V0) => Self::V0,
            ApiVersion(rpc::v1::registration::ApiVersion::V1) => Self::V1,
        }
    }
}
