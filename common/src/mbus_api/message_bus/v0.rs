// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

pub use crate::mbus_api::{v0::*, Message};
use crate::{
    mbus_api::{ReplyError, ReplyErrorKind, ResourceKind},
    types::v0::message_bus::{
        AddNexusChild, AddVolumeNexus, Child, CreateNexus, CreatePool, CreateReplica, CreateVolume,
        DestroyNexus, DestroyPool, DestroyReplica, DestroyVolume, Filter, GetBlockDevices,
        GetNexuses, GetNodes, GetPools, GetReplicas, GetSpecs, GetStates, GetVolumes,
        JsonGrpcRequest, Nexus, Node, NodeId, Pool, PublishVolume, RemoveNexusChild,
        RemoveVolumeNexus, Replica, SetVolumeReplica, ShareNexus, ShareReplica, ShareVolume, Specs,
        States, UnpublishVolume, UnshareNexus, UnshareReplica, UnshareVolume, Volume, VolumeId,
        VolumeShareProtocol,
    },
};
use async_trait::async_trait;

/// Error sending/receiving
/// Common error type for send/receive
pub type BusError = ReplyError;

/// Result for sending/receiving
pub type BusResult<T> = Result<T, BusError>;

macro_rules! only_one {
    ($list:ident, $resource:expr) => {
        if let Some(obj) = $list.first() {
            if $list.len() > 1 {
                Err(ReplyError {
                    kind: ReplyErrorKind::FailedPrecondition,
                    resource: $resource,
                    source: "".to_string(),
                    extra: "".to_string(),
                })
            } else {
                Ok(obj.clone())
            }
        } else {
            Err(ReplyError {
                kind: ReplyErrorKind::NotFound,
                resource: $resource,
                source: "".to_string(),
                extra: "".to_string(),
            })
        }
    };
}

/// Interface used by the rest service to interact with the control plane
/// services via the message bus
#[async_trait]
pub trait MessageBusTrait: Sized {
    /// Get all known nodes from the registry
    #[tracing::instrument(level = "debug", err)]
    async fn get_nodes() -> BusResult<Vec<Node>> {
        Ok(GetNodes::from(None).request().await?.into_inner())
    }

    /// Get node with `id`
    #[tracing::instrument(level = "debug", err)]
    async fn get_node(id: &NodeId) -> BusResult<Node> {
        let nodes = GetNodes::from(id.clone()).request().await?.into_inner();
        only_one!(nodes, ResourceKind::Node)
    }

    /// Get pool with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_pool(filter: Filter) -> BusResult<Pool> {
        let pools = Self::get_pools(filter.clone()).await?;
        only_one!(pools, ResourceKind::Pool)
    }

    /// Get pools with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_pools(filter: Filter) -> BusResult<Vec<Pool>> {
        let pools = GetPools { filter }.request().await?;
        Ok(pools.into_inner())
    }

    /// create pool
    #[tracing::instrument(level = "debug", err)]
    async fn create_pool(request: CreatePool) -> BusResult<Pool> {
        Ok(request.request().await?)
    }

    /// destroy pool
    #[tracing::instrument(level = "debug", err)]
    async fn destroy_pool(request: DestroyPool) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// Get replica with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_replica(filter: Filter) -> BusResult<Replica> {
        let replicas = Self::get_replicas(filter.clone()).await?;
        only_one!(replicas, ResourceKind::Replica)
    }

    /// Get replicas with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_replicas(filter: Filter) -> BusResult<Vec<Replica>> {
        let replicas = GetReplicas { filter }.request().await?;
        Ok(replicas.into_inner())
    }

    /// create replica
    #[tracing::instrument(level = "debug", err)]
    async fn create_replica(request: CreateReplica) -> BusResult<Replica> {
        Ok(request.request().await?)
    }

    /// destroy replica
    #[tracing::instrument(level = "debug", err)]
    async fn destroy_replica(request: DestroyReplica) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// share replica
    #[tracing::instrument(level = "debug", err)]
    async fn share_replica(request: ShareReplica) -> BusResult<String> {
        Ok(request.request().await?)
    }

    /// unshare replica
    #[tracing::instrument(level = "debug", err)]
    async fn unshare_replica(request: UnshareReplica) -> BusResult<()> {
        let _ = request.request().await?;
        Ok(())
    }

    /// Get nexuses with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_nexuses(filter: Filter) -> BusResult<Vec<Nexus>> {
        let nexuses = GetNexuses { filter }.request().await?;
        Ok(nexuses.into_inner())
    }

    /// Get nexus with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_nexus(filter: Filter) -> BusResult<Nexus> {
        let nexuses = Self::get_nexuses(filter.clone()).await?;
        only_one!(nexuses, ResourceKind::Nexus)
    }

    /// create nexus
    #[tracing::instrument(level = "debug", err)]
    async fn create_nexus(request: CreateNexus) -> BusResult<Nexus> {
        Ok(request.request().await?)
    }

    /// destroy nexus
    #[tracing::instrument(level = "debug", err)]
    async fn destroy_nexus(request: DestroyNexus) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// share nexus
    #[tracing::instrument(level = "debug", err)]
    async fn share_nexus(request: ShareNexus) -> BusResult<String> {
        Ok(request.request().await?)
    }

    /// unshare nexus
    #[tracing::instrument(level = "debug", err)]
    async fn unshare_nexus(request: UnshareNexus) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// add nexus child
    #[tracing::instrument(level = "debug", err)]
    #[allow(clippy::unit_arg)]
    async fn add_nexus_child(request: AddNexusChild) -> BusResult<Child> {
        Ok(request.request().await?)
    }

    /// remove nexus child
    #[tracing::instrument(level = "debug", err)]
    #[allow(clippy::unit_arg)]
    async fn remove_nexus_child(request: RemoveNexusChild) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// Get volumes with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_volumes(filter: Filter) -> BusResult<Vec<Volume>> {
        let volumes = GetVolumes { filter }.request().await?;
        Ok(volumes.into_inner())
    }

    /// Get volume with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_volume(filter: Filter) -> BusResult<Volume> {
        let volumes = Self::get_volumes(filter.clone()).await?;
        only_one!(volumes, ResourceKind::Volume)
    }

    /// create volume
    #[tracing::instrument(level = "debug", err)]
    async fn create_volume(request: CreateVolume) -> BusResult<Volume> {
        Ok(request.request().await?)
    }

    /// delete volume
    #[tracing::instrument(level = "debug", err)]
    async fn delete_volume(request: DestroyVolume) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// add volume nexus
    #[tracing::instrument(level = "debug", err)]
    async fn add_volume_nexus(request: AddVolumeNexus) -> BusResult<Nexus> {
        Ok(request.request().await?)
    }

    /// remove volume nexus
    #[tracing::instrument(level = "debug", err)]
    async fn remove_volume_nexus(request: RemoveVolumeNexus) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// publish volume on the given node and optionally make it available for IO through the
    /// specified protocol
    #[tracing::instrument(level = "debug", err)]
    async fn publish_volume(
        uuid: VolumeId,
        node: Option<NodeId>,
        protocol: Option<VolumeShareProtocol>,
    ) -> BusResult<Volume> {
        let request = PublishVolume::new(uuid, node, protocol);
        Ok(request.request().await?)
    }

    /// unpublish the given volume uuid
    #[tracing::instrument(level = "debug", err)]
    async fn unpublish_volume(uuid: VolumeId, force: bool) -> BusResult<Volume> {
        let request = UnpublishVolume::new(&uuid, force);
        Ok(request.request().await?)
    }

    /// set volume replica count
    #[tracing::instrument(level = "debug", err)]
    async fn set_volume_replica(uuid: VolumeId, replica: u8) -> BusResult<Volume> {
        let request = SetVolumeReplica::new(uuid, replica);
        Ok(request.request().await?)
    }

    /// share volume
    #[tracing::instrument(level = "debug", err)]
    async fn share_volume(id: VolumeId, protocol: VolumeShareProtocol) -> BusResult<String> {
        let request = ShareVolume::new(id, protocol);
        Ok(request.request().await?)
    }

    /// unshare volume
    #[tracing::instrument(level = "debug", err)]
    async fn unshare_volume(id: VolumeId) -> BusResult<()> {
        let request = UnshareVolume::new(id);
        request.request().await?;
        Ok(())
    }

    /// Generic JSON gRPC call
    #[tracing::instrument(level = "debug", err)]
    async fn json_grpc_call(request: JsonGrpcRequest) -> BusResult<serde_json::Value> {
        Ok(request.request().await?)
    }

    /// Get block devices on a node
    #[tracing::instrument(level = "debug", err)]
    async fn get_block_devices(request: GetBlockDevices) -> BusResult<BlockDevices> {
        Ok(request.request().await?)
    }

    /// Get all the specs from the registry
    #[tracing::instrument(level = "debug", err)]
    async fn get_specs(request: GetSpecs) -> BusResult<Specs> {
        Ok(request.request().await?)
    }

    /// Get all the states from the registry
    #[tracing::instrument(level = "debug", err)]
    async fn get_states(request: GetStates) -> BusResult<States> {
        Ok(request.request().await?)
    }
}

/// Implementation of the bus interface trait
pub struct MessageBus {}
impl MessageBusTrait for MessageBus {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::v0::{
        message_bus::{NodeState, NodeStatus},
        store::node::{NodeLabels, NodeSpec},
    };
    use composer::{rpc::mayastor::Null, *};

    async fn bus_init() -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            crate::mbus_api::message_bus_init("10.1.0.2".into()).await
        })
        .await?;
        Ok(())
    }
    async fn wait_for_node() -> Result<(), Box<dyn std::error::Error>> {
        let _ = GetNodes::default().request().await?;
        Ok(())
    }
    fn init_tracing() {
        if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        } else {
            tracing_subscriber::fmt().with_env_filter("info").init();
        }
    }
    // to avoid waiting for timeouts
    async fn orderly_start(test: &ComposeTest) -> Result<(), Box<dyn std::error::Error>> {
        test.start_containers(vec!["nats", "node"]).await?;

        bus_init().await?;
        wait_for_node().await?;

        test.start("mayastor").await?;

        let mut hdl = test.grpc_handle("mayastor").await?;
        hdl.mayastor.list_nexus(Null {}).await?;
        Ok(())
    }

    #[tokio::test]
    async fn bus() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();
        let mayastor = "node-test-name";
        let test = Builder::new()
            .name("rest_backend")
            .add_container_bin("nats", Binary::from_path("nats-server").with_arg("-DV"))
            .add_container_bin("node", Binary::from_dbg("node").with_nats("-n"))
            .add_container_bin(
                "mayastor",
                Binary::from_path("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor]),
            )
            .with_clean(true)
            .autorun(false)
            .build()
            .await?;

        orderly_start(&test).await?;

        test_bus_backend(&NodeId::from(mayastor), &test).await?;

        // run with --nocapture to see all the logs
        test.logs_all().await?;
        Ok(())
    }

    async fn test_bus_backend(
        mayastor: &NodeId,
        test: &ComposeTest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nodes = MessageBus::get_nodes().await?;
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.len(), 1);
        let node_check = Node::new(
            mayastor.clone(),
            Some(NodeSpec::new(
                mayastor.clone(),
                "0.0.0.0:10124".to_string(),
                NodeLabels::new(),
            )),
            Some(NodeState::new(
                mayastor.clone(),
                "0.0.0.0:10124".to_string(),
                NodeStatus::Online,
            )),
        );
        assert_eq!(nodes.first().unwrap(), &node_check);
        let node = MessageBus::get_node(mayastor).await?;
        assert_eq!(node, node_check);

        test.stop("mayastor").await?;

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(MessageBus::get_nodes().await?.is_empty());

        Ok(())
    }
}
