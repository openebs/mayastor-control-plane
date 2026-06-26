use rpc::v1::pb::{
    host_rpc_server::HostRpcServer, nexus_rpc_server::NexusRpcServer,
    pool_rpc_server::PoolRpcServer, replica_rpc_server::ReplicaRpcServer, *,
};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};
use stor_port::types::v0::{
    openapi::apis::Uuid, store::node::NodeSpec, transport, transport::PoolState,
};
use tonic::metadata::MetadataMap;
use tracing_subscriber::{FmtSubscriber, fmt::format::FmtSpan};

#[derive(Debug, Default, Clone)]
struct IoEngine {
    sims: HashMap<String, IoEngineCfg>,
}
#[derive(Debug, Clone)]
struct IoEngineCfg {
    version: String,
    id: String,
    endpoint: SocketAddr,
    pools: Vec<Pool>,
    replicas: Vec<Replica>,
    nexuses: Vec<Nexus>,
}

impl IoEngine {
    fn new() -> anyhow::Result<IoEngine> {
        use std::io::{BufRead, BufReader};

        let bundle = std::path::PathBuf::from(std::env::var("SIM_BUNDLE")?);
        let bundle = {
            let umbrella_bundle = bundle.join("mayastor");
            if umbrella_bundle.exists() && umbrella_bundle.is_dir() {
                umbrella_bundle
            } else {
                bundle
            }
        };
        let etcd_dump = std::fs::File::open(bundle.join("etcd_dump"))?;
        let mut io_engine = IoEngine::default();

        let reader = BufReader::new(etcd_dump);
        let mut lines = reader.lines();
        let mut node_section = false;
        while let Some(Ok(key)) = lines.next() {
            if key.is_empty() {
                continue;
            }
            let Some(Ok(mut value)) = lines.next() else {
                break;
            };

            let (compact, key) = match key.strip_suffix(":") {
                Some(key) => (false, key),
                None => (true, key.as_str()),
            };

            if !compact && value != "null" {
                while let Some(Ok(more)) = lines.next() {
                    value.push('\n');
                    value.push_str(&more);
                    if more == "}" {
                        break;
                    }
                }
                use std::str::FromStr;
                let json_value = serde_json::Value::from_str(&value)?;
                value = json_value.to_string();
            }

            if !key.contains("/NodeSpec/") {
                if node_section {
                    break;
                }
                continue;
            }
            node_section = true;
            let node = serde_json::from_str::<NodeSpec>(&value)?;
            io_engine.sims.insert(
                node.id().to_string(),
                IoEngineCfg {
                    version: node.version().clone().unwrap(),
                    id: node.id().to_string(),
                    endpoint: node.endpoint(),
                    pools: vec![],
                    replicas: vec![],
                    nexuses: vec![],
                },
            );
        }

        let path = bundle.join("topology/pool/");
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            let content = std::fs::read_to_string(&path)?;
            let value: serde_json::Value = serde_json::from_str(&content)?;
            let state = &value["pool"]["state"];
            if state.is_null() {
                continue;
            }
            let state = serde_json::from_value::<PoolState>(state.clone())?;
            let pool = Pool {
                uuid: Uuid::new_v4().to_string(),
                name: state.id.to_string(),
                disks: state.disks.into_iter().map(Into::into).collect(),
                state: state.status as i32,
                capacity: state.capacity,
                used: state.used,
                pooltype: PoolType::Lvs as i32,
                committed: state.committed.unwrap_or_default(),
                cluster_size: state.cluster_size,
                page_size: None,
                disk_capacity: state.disk_capacity.unwrap_or_default(),
                md_info: None,
                encrypted: Some(state.encrypted),
                max_expandable_size: state.max_expandable_size,
                disk_info: vec![],
                errors: None,
            };
            if let Some(node) = io_engine.sims.get_mut(state.node.as_str()) {
                node.pools.push(pool);
            }
        }

        let path = bundle.join("topology/volume/");
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            let content = match std::fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) => {
                    tracing::error!("Failed to load content for {}: {error}", path.display());
                    continue;
                }
            };
            let value: serde_json::Value = serde_json::from_str(&content)?;
            if let Some(replicas) = value.get("replicas_topology").and_then(|v| v.as_array()) {
                for item in replicas {
                    let Some(mut replicav) = item.get("replica").cloned() else {
                        continue;
                    };
                    let to_lower = |v: serde_json::Value| v.as_str().unwrap().to_lowercase();
                    // Since we're dumping the REST and not the transport, there are some nuances...
                    replicav["name"] = replicav["uuid"].clone();
                    replicav["status"] = to_lower(replicav["state"].take()).into();
                    replicav["poolId"] = replicav["pool"].take();
                    replicav["kind"] = to_lower(replicav["kind"].take()).into();
                    replicav["allowedHosts"] = serde_json::Value::Array(vec![]);
                    replicav["space"]["allocated_clusters_snapshots"] = 0.into();
                    let mut replica: transport::Replica = serde_json::from_value(replicav.clone())?;
                    replica.status = transport::ReplicaStatus::Online;
                    let replicaf = Replica {
                        name: replica.name.to_string(),
                        uuid: replica.uuid.to_string(),
                        pooluuid: replica.pool_uuid.unwrap_or_default().to_string(),
                        size: replica.size,
                        thin: replica.thin,
                        share: replica.share as i32,
                        uri: replica.uri,
                        poolname: replica.pool_id.to_string(),
                        usage: None,
                        allowed_hosts: replica.allowed_hosts.into_iter().map(Into::into).collect(),
                        is_snapshot: replica.kind == transport::ReplicaKind::Snapshot,
                        is_clone: replica.kind == transport::ReplicaKind::SnapshotClone,
                        snapshot_uuid: None,
                        entity_id: replica.entity_id.map(Into::into),
                        pooltype: PoolType::Lvs as i32,
                        encrypted: replica.encrypted,
                    };
                    if let Some(node) = io_engine.sims.get_mut(replica.node.as_str()) {
                        node.replicas.push(replicaf);
                    }
                }
            }
            let state = &value["volume"]["state"];
            if let Some(mut target) = state.get("target").cloned() {
                // Since we're dumping the REST and not the transport, there are some nuances...
                target["name"] = target["uuid"].clone();
                target["status"] = target["state"].take();
                target["share"] = "nvmf".into();
                target["allowedHosts"] = serde_json::Value::Array(vec![]);
                let mut state: transport::Nexus = serde_json::from_value(target.clone()).unwrap();
                state.status = transport::NexusStatus::Online;
                let nexus = Nexus {
                    name: state.name,
                    uuid: state.uuid.into(),
                    size: state.size,
                    state: state.status as i32,
                    children: state
                        .children
                        .into_iter()
                        .map(|c| Child {
                            uri: c.uri.to_string(),
                            state: c.state as i32,
                            state_reason: c.state_reason as i32,
                            rebuild_progress: c.rebuild_progress.unwrap_or_default() as i32,
                            device_name: None,
                            fault_timestamp: c.faulted_at.map(Into::into),
                            has_io_log: c.has_io_log.unwrap_or_default(),
                        })
                        .collect(),
                    device_uri: state.device_uri,
                    rebuilds: state.rebuilds,
                    ana_state: 0,
                    allowed_hosts: state.allowed_hosts.into_iter().map(Into::into).collect(),
                    label_version: Some(match state.version {
                        transport::NexusVersion::V1 => {
                            rpc::v1::nexus::NexusLabelVersion::LabelV1 as i32
                        }
                        transport::NexusVersion::V2 => {
                            rpc::v1::nexus::NexusLabelVersion::LabelV2 as i32
                        }
                    }),
                    bdev_size: state.bdev_size,
                };
                if let Some(node) = io_engine.sims.get_mut(state.node.as_str()) {
                    node.nexuses.push(nexus);
                }
            }
        }

        Ok(io_engine)
    }
    fn request_node(&self, metadata: &MetadataMap) -> Result<&IoEngineCfg, tonic::Status> {
        let Some(agent) = metadata.get("user-agent") else {
            return Err(tonic::Status::internal("no user-agent"));
        };
        let agent = agent.to_str().unwrap();
        let mut parts = agent.split_whitespace();
        let id = parts.next().unwrap_or_default();
        let Some(node) = self.sims.get(id) else {
            return Err(tonic::Status::internal(format!("node {id} not found")));
        };
        Ok(node)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let io_engine = IoEngine::new()?;
    tonic::transport::Server::builder()
        .add_service(PoolRpcServer::new(io_engine.clone()))
        .add_service(ReplicaRpcServer::new(io_engine.clone()))
        .add_service(NexusRpcServer::new(io_engine.clone()))
        .add_service(HostRpcServer::new(io_engine))
        .serve(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(0, 0, 0, 0),
            10124,
        )))
        .await?;
    Ok(())
}

#[tonic::async_trait]
impl rpc::v1::pb::host_rpc_server::HostRpc for IoEngine {
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn get_mayastor_info(
        &self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Response<MayastorInfoResponse>, tonic::Status> {
        let node = self.request_node(request.metadata())?;

        let info = MayastorInfoResponse {
            version: node.version.clone(),
            previous_features: None,
            registration_info: Some(RegisterRequest {
                id: node.id.clone(),
                grpc_endpoint: node.endpoint.to_string(),
                instance_uuid: None,
                api_version: vec![1],
                hostnqn: None,
                features: None,
                bugfixes: None,
                version: Some(node.version.clone()),
            }),
        };
        Ok(tonic::Response::new(info))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn list_block_devices(
        &self,
        _request: tonic::Request<ListBlockDevicesRequest>,
    ) -> Result<tonic::Response<ListBlockDevicesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn get_mayastor_resource_usage(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<GetMayastorResourceUsageResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn list_nvme_controllers(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<ListNvmeControllersResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn stat_nvme_controller(
        &self,
        _request: tonic::Request<StatNvmeControllerRequest>,
    ) -> Result<tonic::Response<StatNvmeControllerResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
}

#[tonic::async_trait]
impl rpc::v1::pb::pool_rpc_server::PoolRpc for IoEngine {
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn create_pool(
        &self,
        _request: tonic::Request<CreatePoolRequest>,
    ) -> Result<tonic::Response<Pool>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn destroy_pool(
        &self,
        _request: tonic::Request<DestroyPoolRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn export_pool(
        &self,
        _request: tonic::Request<ExportPoolRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn import_pool(
        &self,
        _request: tonic::Request<ImportPoolRequest>,
    ) -> Result<tonic::Response<Pool>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn list_pools(
        &self,
        request: tonic::Request<ListPoolOptions>,
    ) -> Result<tonic::Response<ListPoolsResponse>, tonic::Status> {
        let node = self.request_node(request.metadata())?;
        Ok(tonic::Response::new(ListPoolsResponse {
            pools: node.pools.clone(),
        }))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn grow_pool(
        &self,
        _request: tonic::Request<GrowPoolRequest>,
    ) -> Result<tonic::Response<GrowPoolResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn grow_pool_v2(
        &self,
        _request: tonic::Request<GrowPoolRequest>,
    ) -> Result<tonic::Response<Pool>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn clear_errors(
        &self,
        _request: tonic::Request<ClearErrorRequest>,
    ) -> Result<tonic::Response<Pool>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn probe_pool(
        &self,
        _request: tonic::Request<ProbePoolRequest>,
    ) -> Result<tonic::Response<ProbePoolResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
}

#[tonic::async_trait]
impl rpc::v1::pb::nexus_rpc_server::NexusRpc for IoEngine {
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn create_nexus_v2(
        &self,
        _request: tonic::Request<CreateNexusV2Request>,
    ) -> Result<tonic::Response<CreateNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn create_nexus(
        &self,
        _request: tonic::Request<CreateNexusRequest>,
    ) -> Result<tonic::Response<CreateNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn destroy_nexus(
        &self,
        _request: tonic::Request<DestroyNexusRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn resize_nexus(
        &self,
        _request: tonic::Request<ResizeNexusRequest>,
    ) -> Result<tonic::Response<ResizeNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn list_nexus(
        &self,
        request: tonic::Request<ListNexusOptions>,
    ) -> Result<tonic::Response<ListNexusResponse>, tonic::Status> {
        let node = self.request_node(request.metadata())?;
        Ok(tonic::Response::new(ListNexusResponse {
            nexus_list: node.nexuses.clone(),
        }))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn add_child_nexus(
        &self,
        _request: tonic::Request<AddChildNexusRequest>,
    ) -> Result<tonic::Response<AddChildNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn remove_child_nexus(
        &self,
        _request: tonic::Request<RemoveChildNexusRequest>,
    ) -> Result<tonic::Response<RemoveChildNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn fault_nexus_child(
        &self,
        _request: tonic::Request<FaultNexusChildRequest>,
    ) -> Result<tonic::Response<FaultNexusChildResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn shutdown_nexus(
        &self,
        _request: tonic::Request<ShutdownNexusRequest>,
    ) -> Result<tonic::Response<ShutdownNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn publish_nexus(
        &self,
        _request: tonic::Request<PublishNexusRequest>,
    ) -> Result<tonic::Response<PublishNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn unpublish_nexus(
        &self,
        _request: tonic::Request<UnpublishNexusRequest>,
    ) -> Result<tonic::Response<UnpublishNexusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn get_nvme_ana_state(
        &self,
        _request: tonic::Request<GetNvmeAnaStateRequest>,
    ) -> Result<tonic::Response<GetNvmeAnaStateResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn set_nvme_ana_state(
        &self,
        _request: tonic::Request<SetNvmeAnaStateRequest>,
    ) -> Result<tonic::Response<SetNvmeAnaStateResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn child_operation(
        &self,
        _request: tonic::Request<ChildOperationRequest>,
    ) -> Result<tonic::Response<ChildOperationResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn start_rebuild(
        &self,
        _request: tonic::Request<StartRebuildRequest>,
    ) -> Result<tonic::Response<StartRebuildResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn stop_rebuild(
        &self,
        _request: tonic::Request<StopRebuildRequest>,
    ) -> Result<tonic::Response<StopRebuildResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn pause_rebuild(
        &self,
        _request: tonic::Request<PauseRebuildRequest>,
    ) -> Result<tonic::Response<PauseRebuildResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn resume_rebuild(
        &self,
        _request: tonic::Request<ResumeRebuildRequest>,
    ) -> Result<tonic::Response<ResumeRebuildResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn get_rebuild_state(
        &self,
        _request: tonic::Request<RebuildStateRequest>,
    ) -> Result<tonic::Response<RebuildStateResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn get_rebuild_stats(
        &self,
        _request: tonic::Request<RebuildStatsRequest>,
    ) -> Result<tonic::Response<RebuildStatsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn get_rebuild_history(
        &self,
        _request: tonic::Request<RebuildHistoryRequest>,
    ) -> Result<tonic::Response<RebuildHistoryResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn list_rebuild_history(
        &self,
        _request: tonic::Request<ListRebuildHistoryRequest>,
    ) -> Result<tonic::Response<ListRebuildHistoryResponse>, tonic::Status> {
        Ok(tonic::Response::new(ListRebuildHistoryResponse {
            histories: Default::default(),
            end_time: None,
        }))
    }
}

#[tonic::async_trait]
impl rpc::v1::pb::replica_rpc_server::ReplicaRpc for IoEngine {
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn create_replica(
        &self,
        _request: tonic::Request<CreateReplicaRequest>,
    ) -> Result<tonic::Response<Replica>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn destroy_replica(
        &self,
        _request: tonic::Request<DestroyReplicaRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn list_replicas(
        &self,
        request: tonic::Request<ListReplicaOptions>,
    ) -> Result<tonic::Response<ListReplicasResponse>, tonic::Status> {
        let node = self.request_node(request.metadata())?;
        Ok(tonic::Response::new(ListReplicasResponse {
            replicas: node.replicas.clone(),
        }))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn share_replica(
        &self,
        _request: tonic::Request<ShareReplicaRequest>,
    ) -> Result<tonic::Response<Replica>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn unshare_replica(
        &self,
        _request: tonic::Request<UnshareReplicaRequest>,
    ) -> Result<tonic::Response<Replica>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn resize_replica(
        &self,
        _request: tonic::Request<ResizeReplicaRequest>,
    ) -> Result<tonic::Response<Replica>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    #[tracing::instrument(skip(self), err, level = "info")]
    async fn set_replica_entity_id(
        &self,
        _request: tonic::Request<SetReplicaEntityIdRequest>,
    ) -> Result<tonic::Response<Replica>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
}
