use crate::{ApiClientError, CreateVolumeTopology, CsiControllerConfig, IoEngineApiClient};

use csi_driver::context::{CreateParams, PublishParams};
use rpc::csi::{Topology as CsiTopology, *};
use stor_port::types::v0::openapi::{
    models,
    models::{
        AffinityGroup, LabelledTopology, NodeSpec, NodeStatus, Pool, PoolStatus, PoolTopology,
        SpecStatus, Volume, VolumeShareProtocol,
    },
};
use utils::{CREATED_BY_KEY, DSP_OPERATOR};

use regex::Regex;
use std::{collections::HashMap, str::FromStr};
use tonic::{Response, Status};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

const OPENEBS_TOPOLOGY_KEY: &str = "openebs.io/nodename";
const VOLUME_NAME_PATTERN: &str =
    r"pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";
const SNAPSHOT_NAME_PATTERN: &str =
    r"snapshot-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";

#[derive(Debug)]
pub(crate) struct CsiControllerSvc {
    create_volume_limiter: std::sync::Arc<tokio::sync::Semaphore>,
}
impl CsiControllerSvc {
    pub(crate) fn new(cfg: &CsiControllerConfig) -> Self {
        Self {
            create_volume_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(
                cfg.create_volume_limit(),
            )),
        }
    }
    async fn create_volume_permit(&self) -> Result<tokio::sync::SemaphorePermit, tonic::Status> {
        tokio::time::timeout(
            // if we take too long waiting for our turn just abort..
            std::time::Duration::from_secs(3),
            self.create_volume_limiter.acquire(),
        )
        .await
        .map_err(|_| tonic::Status::aborted("Too many create volumes in progress"))?
        .map_err(|_| tonic::Status::unavailable("Service is shutdown"))
    }
}

/// Check whether target volume capabilities are valid. As of now, only
/// SingleNodeWriter capability is supported.
fn check_volume_capabilities(capabilities: &[VolumeCapability]) -> Result<(), tonic::Status> {
    for c in capabilities {
        if let Some(access_mode) = c.access_mode.as_ref() {
            if access_mode.mode != volume_capability::access_mode::Mode::SingleNodeWriter as i32 {
                return Err(Status::invalid_argument(format!(
                    "Invalid volume access mode: {:?}",
                    access_mode.mode
                )));
            }
        }
    }
    Ok(())
}

/// Parse string protocol into REST API protocol enum.
fn parse_protocol(proto: Option<&String>) -> Result<VolumeShareProtocol, Status> {
    match proto.map(|s| s.as_str()) {
        None | Some("nvmf") => Ok(VolumeShareProtocol::Nvmf),
        _ => Err(Status::invalid_argument(format!(
            "Invalid protocol: {proto:?}"
        ))),
    }
}

/// Get share URI for existing volume object and the node where the volume is published.
fn get_volume_share_location(volume: &Volume) -> Option<(String, String)> {
    volume
        .state
        .target
        .as_ref()
        .map(|nexus| (nexus.node.to_string(), nexus.device_uri.to_string()))
}

impl From<ApiClientError> for Status {
    fn from(error: ApiClientError) -> Self {
        match error {
            ApiClientError::ResourceNotExists(reason) => Status::not_found(reason),
            ApiClientError::NotImplemented(reason) => Status::unimplemented(reason),
            ApiClientError::RequestTimeout(reason) => Status::deadline_exceeded(reason),
            ApiClientError::Conflict(reason) => Status::unavailable(reason),
            ApiClientError::Aborted(reason) => Status::unavailable(reason),
            error => Status::internal(format!("Operation failed: {error:?}")),
        }
    }
}

/// Check whether existing volume is compatible with requested configuration.
/// Target volume is assumed to exist.
/// TODO: Add full topology check once Control Plane supports full volume spec.
#[instrument]
fn check_existing_volume(
    volume: &Volume,
    replica_count: u8,
    size: u64,
    thin: bool,
) -> Result<(), Status> {
    // Check if the existing volume is compatible, which means
    //  - number of replicas is equal or greater
    //  - size is equal or greater
    //  - volume is fully created
    let spec = &volume.spec;

    if spec.status != SpecStatus::Created {
        let message = format!(
            "Existing volume {} is in insufficient state: {:?}",
            spec.uuid, spec.status
        );
        return Err(if spec.status == SpecStatus::Creating {
            Status::aborted(message)
        } else {
            Status::already_exists(message)
        });
    }

    if spec.num_replicas < replica_count {
        return Err(Status::already_exists(format!(
            "Existing volume {} has insufficient number of replicas: {} ({} requested)",
            spec.uuid, spec.num_replicas, replica_count
        )));
    }

    if spec.size < size {
        return Err(Status::already_exists(format!(
            "Existing volume {} has insufficient size: {} bytes ({} requested)",
            spec.uuid, spec.size, size
        )));
    }

    if spec.thin != thin {
        return Err(Status::already_exists(format!(
            "Existing volume {} has thin provisioning set to {} ({} requested)",
            spec.uuid, spec.thin, thin
        )));
    }

    Ok(())
}

struct VolumeTopologyMapper {}

impl VolumeTopologyMapper {
    async fn init() -> Result<VolumeTopologyMapper, Status> {
        Ok(Self {})
    }

    /// Determine the list of nodes where the workload can be placed.
    fn volume_accessible_topology(&self) -> Vec<CsiTopology> {
        // TODO: handle accessibility
        vec![CsiTopology {
            segments: CsiControllerConfig::get_config().node_selector_segment(),
        }]
    }
}

#[tonic::async_trait]
impl rpc::csi::controller_server::Controller for CsiControllerSvc {
    #[instrument(err, fields(volume.uuid = tracing::field::Empty), skip(self))]
    async fn create_volume(
        &self,
        request: tonic::Request<CreateVolumeRequest>,
    ) -> Result<tonic::Response<CreateVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(request = ?args);
        let _permit = self.create_volume_permit().await?;

        if args.volume_content_source.is_some() {
            return Err(Status::invalid_argument(
                "Source for create volume is not supported",
            ));
        }

        // k8s uses names pvc-{uuid} and we use uuid further as ID in SPDK so we
        // must require it.
        let re = Regex::new(VOLUME_NAME_PATTERN).unwrap();
        let volume_uuid = match re.captures(&args.name) {
            Some(captures) => captures.get(1).unwrap().as_str().to_string(),
            None => {
                return Err(Status::invalid_argument(format!(
                    "Expected the volume name in pvc-<UUID> format: {}",
                    args.name
                )))
            }
        };
        tracing::Span::current().record("volume.uuid", volume_uuid.as_str());

        check_volume_capabilities(&args.volume_capabilities)?;

        // Check volume size.
        let size = match args.capacity_range {
            Some(range) => {
                if range.required_bytes <= 0 {
                    return Err(Status::invalid_argument(
                        "Volume size must be a non-negative number",
                    ));
                }
                range.required_bytes as u64
            }
            None => {
                return Err(Status::invalid_argument(
                    "Volume capacity range is not provided",
                ))
            }
        };

        let context = CreateParams::try_from(&args.parameters)?;
        let replica_count = context.replica_count();

        let mut inclusive_label_topology: HashMap<String, String> = HashMap::new();

        inclusive_label_topology.insert(String::from(CREATED_BY_KEY), String::from(DSP_OPERATOR));

        let u = Uuid::parse_str(&volume_uuid).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {volume_uuid}"))
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(u)?;

        let vt_mapper = VolumeTopologyMapper::init().await?;

        let thin = match args.parameters.get("thin") {
            Some(value) => value == "true",
            None => false,
        };

        // First check if the volume already exists.
        match IoEngineApiClient::get_client()
            .get_volume_for_create(&u)
            .await
        {
            Ok(volume) => {
                check_existing_volume(&volume, replica_count, size, thin)?;
                debug!(
                    "Volume {} already exists and is compatible with requested config",
                    volume_uuid
                );
            }
            // If the volume doesn't exist, create it.
            Err(ApiClientError::ResourceNotExists(_)) => {
                let volume_topology = CreateVolumeTopology::new(
                    None,
                    Some(PoolTopology::labelled(LabelledTopology {
                        exclusion: Default::default(),
                        inclusion: inclusive_label_topology,
                    })),
                );

                let sts_affinity_group_name = context.sts_affinity_group();

                IoEngineApiClient::get_client()
                    .create_volume(
                        &u,
                        replica_count,
                        size,
                        volume_topology,
                        thin,
                        sts_affinity_group_name.clone().map(AffinityGroup::new),
                    )
                    .await?;

                if let Some(ag_name) = sts_affinity_group_name {
                    debug!(
                        volume.uuid = volume_uuid,
                        volume.affinity_group = ag_name,
                        "Volume successfully created"
                    );
                } else {
                    debug!(volume.uuid = volume_uuid, "Volume successfully created");
                }
            }
            Err(e) => return Err(e.into()),
        }

        let volume = rpc::csi::Volume {
            capacity_bytes: size as i64,
            volume_id: volume_uuid,
            volume_context: args.parameters.clone(),
            content_source: None,
            accessible_topology: vt_mapper.volume_accessible_topology(),
        };

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume),
        }))
    }
    #[instrument(err, fields(volume.uuid = %request.get_ref().volume_id), skip(self))]
    async fn delete_volume(
        &self,
        request: tonic::Request<DeleteVolumeRequest>,
    ) -> Result<tonic::Response<DeleteVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(volume.uuid = %args.volume_id, request = ?args);
        let volume_uuid = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(volume_uuid)?;
        IoEngineApiClient::get_client()
            .delete_volume(&volume_uuid)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to delete volume {}, error = {:?}",
                    args.volume_id, e
                ))
            })?;

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    #[instrument(err, fields(volume.uuid = %request.get_ref().volume_id), skip(self))]
    async fn controller_publish_volume(
        &self,
        request: tonic::Request<ControllerPublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerPublishVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(volume.uuid = %args.volume_id, request = ?args);
        if args.readonly {
            return Err(Status::invalid_argument(
                "Read-only volumes are not supported",
            ));
        }

        let protocol = parse_protocol(args.volume_context.get("protocol"))?;

        if args.node_id.is_empty() {
            return Err(Status::invalid_argument("Node ID must not be empty"));
        }
        let node_id = args.node_id.clone();

        if args.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID must not be empty"));
        }
        let volume_id = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(volume_id)?;

        match args.volume_capability {
            Some(c) => {
                check_volume_capabilities(&[c])?;
            }
            None => {
                return Err(Status::invalid_argument("Missing volume capability"));
            }
        }

        // Check if the volume is already published.
        let volume = IoEngineApiClient::get_client()
            .get_volume(&volume_id)
            .await?;

        let params = PublishParams::try_from(&args.volume_context)?;

        // Prepare the context for the csi-node plugin.
        let mut publish_context = params.into_context();

        let uri =
            // Volume is already published, make sure the protocol matches and get URI.
            match &volume.spec.target {
                Some(target) => {
                    if target.protocol != Some(protocol) {
                        let m = format!(
                            "Volume {} already shared via different protocol: {:?}",
                            volume_id, target.protocol,
                        );
                        error!("{}", m);
                        return Err(Status::failed_precondition(m));
                    }

                    if let Some((node, uri)) = get_volume_share_location(&volume) {
                        // Make sure volume is published at the same node.
                        if node_id != node {
                            let m = format!(
                                "Volume {volume_id} already published on a different node: {node}",
                            );
                            error!("{}", m);
                            return Err(Status::failed_precondition(m));
                        }

                        debug!("Volume {} already published at {}", volume_id, uri);
                        uri
                    } else {
                        let m = format!(
                            "Volume {volume_id} reports no info about its publishing status"
                        );
                        error!("{}", m);
                        return Err(Status::internal(m));
                    }
                },
            _ => {

                // Check for node being cordoned.
                fn cordon_check(spec: Option<&NodeSpec>) -> bool {
                    if let Some(spec) = spec {
                        return spec.cordondrainstate.is_some()
                    }
                    false
                }

                // if the csi-node happens to be a data-plane node, use that for nexus creation, otherwise
                // let the control-plane select the target node.
                let target_node = match IoEngineApiClient::get_client().get_node(&node_id).await {
                    Err(ApiClientError::ResourceNotExists(_)) => Ok(None),
                    Err(error) => Err(error),
                    // When nodes are not online for any reason (eg: io-engine no longer runs) on said node,
                    // then let the control-plane decide where to place the target. Node should not be cordoned.
                    Ok(node) if node.state.as_ref().map(|n| n.status).unwrap_or(NodeStatus::Unknown) != NodeStatus::Online || cordon_check(node.spec.as_ref()) => {
                        Ok(None)
                    },
                    Ok(_) => Ok(Some(node_id.as_str()))
                }?;

                // Volume is not published.
                let v = IoEngineApiClient::get_client()
                    .publish_volume(&volume_id, target_node, protocol, args.node_id.clone(), &publish_context)
                    .await?;

                if let Some((node, uri)) = get_volume_share_location(&v) {
                    debug!(
                        "Volume {} successfully published on node {} via {}",
                        volume_id, node, uri
                    );
                    uri
                } else {
                    let m = format!(
                        "Volume {volume_id} has been successfully published but URI is not available"
                    );
                    error!("{}", m);
                    return Err(Status::internal(m));
                }
            }
        };

        publish_context.insert("uri".to_string(), uri);

        debug!(
            "Publish context for volume {}: {:?}",
            volume_id, publish_context
        );
        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context,
        }))
    }

    #[instrument(err, fields(volume.uuid = %request.get_ref().volume_id), skip(self))]
    async fn controller_unpublish_volume(
        &self,
        request: tonic::Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerUnpublishVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(volume.uuid = %args.volume_id, request = ?args);

        let volume_uuid = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(volume_uuid)?;
        // Check if target volume exists.
        let volume = match IoEngineApiClient::get_client()
            .get_volume(&volume_uuid)
            .await
        {
            Ok(volume) => volume,
            Err(ApiClientError::ResourceNotExists { .. }) => {
                debug!("Volume {} does not exist, not unpublishing", args.volume_id);
                return Ok(Response::new(ControllerUnpublishVolumeResponse {}));
            }
            Err(e) => return Err(Status::from(e)),
        };

        if volume.spec.target.is_none() {
            // Volume is not published, bail out.
            debug!(
                "Volume {} is not published, not unpublishing",
                args.volume_id
            );
            return Ok(Response::new(ControllerUnpublishVolumeResponse {}));
        }

        // Do forced volume upublish as Kubernetes already detached the volume.
        IoEngineApiClient::get_client()
            .unpublish_volume(&volume_uuid, true)
            .await
            .map_err(|e| {
                Status::not_found(format!(
                    "Failed to unpublish volume {}, error = {:?}",
                    &args.volume_id, e
                ))
            })?;

        debug!("Volume {} successfully unpublished", args.volume_id);
        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    #[instrument(err, fields(volume.uuid = %request.get_ref().volume_id), skip(self))]
    async fn validate_volume_capabilities(
        &self,
        request: tonic::Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<tonic::Response<ValidateVolumeCapabilitiesResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(volume.uuid = %args.volume_id, request = ?args);

        debug!("Request to validate volume capabilities: {:?}", args);
        let volume_uuid = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(volume_uuid)?;
        let _volume = IoEngineApiClient::get_client()
            .get_volume(&volume_uuid)
            .await
            .map_err(|_e| Status::unimplemented("Not implemented"))?;

        let caps: Vec<VolumeCapability> = args
            .volume_capabilities
            .into_iter()
            .filter(|cap| {
                if let Some(access_mode) = cap.access_mode.as_ref() {
                    if access_mode.mode
                        == volume_capability::access_mode::Mode::SingleNodeWriter as i32
                    {
                        return true;
                    }
                }
                false
            })
            .collect();

        let response = if !caps.is_empty() {
            ValidateVolumeCapabilitiesResponse {
                confirmed: Some(validate_volume_capabilities_response::Confirmed {
                    volume_context: HashMap::new(),
                    parameters: HashMap::new(),
                    volume_capabilities: caps,
                }),
                message: "".to_string(),
            }
        } else {
            ValidateVolumeCapabilitiesResponse {
                confirmed: None,
                message: "The only supported capability is SINGLE_NODE_WRITER".to_string(),
            }
        };

        Ok(Response::new(response))
    }

    #[instrument(err, skip(self))]
    async fn list_volumes(
        &self,
        request: tonic::Request<ListVolumesRequest>,
    ) -> Result<tonic::Response<ListVolumesResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(request = ?args);

        let max_entries = args.max_entries;
        if max_entries < 0 {
            return Err(Status::invalid_argument("max_entries can't be negative"));
        }

        let vt_mapper = VolumeTopologyMapper::init().await?;

        let volumes = IoEngineApiClient::get_client()
            .list_volumes(max_entries, args.starting_token)
            .await
            .map_err(|e| Status::internal(format!("Failed to list volumes, error = {e:?}")))?;

        let entries = volumes
            .entries
            .into_iter()
            .map(|v| {
                let volume = rpc::csi::Volume {
                    volume_id: v.spec.uuid.to_string(),
                    capacity_bytes: v.spec.size as i64,
                    volume_context: HashMap::new(),
                    content_source: None,
                    accessible_topology: vt_mapper.volume_accessible_topology(),
                };

                list_volumes_response::Entry {
                    volume: Some(volume),
                    status: None,
                }
            })
            .collect();

        debug!("Available k8s volumes: {:?}", entries);

        Ok(Response::new(ListVolumesResponse {
            entries,
            next_token: volumes.next_token.map_or("".to_string(), |v| v.to_string()),
        }))
    }

    #[instrument(err, skip(self))]
    async fn get_capacity(
        &self,
        request: tonic::Request<GetCapacityRequest>,
    ) -> Result<tonic::Response<GetCapacityResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(request = ?args);

        // Check capabilities.
        check_volume_capabilities(&args.volume_capabilities)?;

        // Determine target node, if requested.
        let node: Option<&String> = if let Some(topology) = args.accessible_topology.as_ref() {
            topology.segments.get(OPENEBS_TOPOLOGY_KEY)
        } else {
            None
        };

        let pools: Vec<Pool> = if let Some(node) = node {
            debug!("Calculating pool capacity for node {}", node);
            IoEngineApiClient::get_client()
                .get_node_pools(node)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "Failed to list pools for node {node}, error = {e:?}",
                    ))
                })?
        } else {
            debug!("Calculating overall pool capacity");
            IoEngineApiClient::get_client()
                .list_pools()
                .await
                .map_err(|e| {
                    Status::internal(format!("Failed to list all pools, error = {e:?}",))
                })?
        };

        let available_capacity: i64 = pools.into_iter().fold(0, |acc, p| match p.state {
            Some(state) => match state.status {
                PoolStatus::Online | PoolStatus::Degraded => acc + state.capacity as i64,
                _ => {
                    warn!(
                        "Pool {} on node {} is in '{:?}' state, not accounting it for capacity",
                        p.id, state.node, state.status,
                    );
                    acc
                }
            },
            None => 0,
        });

        Ok(Response::new(GetCapacityResponse {
            available_capacity,
            maximum_volume_size: None,
            minimum_volume_size: None,
        }))
    }

    #[instrument(err, skip(self))]
    async fn controller_get_capabilities(
        &self,
        _request: tonic::Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<tonic::Response<ControllerGetCapabilitiesResponse>, tonic::Status> {
        let capabilities = vec![
            controller_service_capability::rpc::Type::CreateDeleteVolume,
            controller_service_capability::rpc::Type::PublishUnpublishVolume,
            controller_service_capability::rpc::Type::ListVolumes,
            controller_service_capability::rpc::Type::GetCapacity,
            controller_service_capability::rpc::Type::CreateDeleteSnapshot,
            controller_service_capability::rpc::Type::ListSnapshots,
        ];

        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: capabilities
                .into_iter()
                .map(|c| ControllerServiceCapability {
                    r#type: Some(controller_service_capability::Type::Rpc(
                        controller_service_capability::Rpc { r#type: c as i32 },
                    )),
                })
                .collect(),
        }))
    }

    #[instrument(err, fields(volume.uuid = request.get_ref().source_volume_id, snapshot.source_uuid = request.get_ref().source_volume_id, snapshot.uuid), skip(self))]
    async fn create_snapshot(
        &self,
        request: tonic::Request<CreateSnapshotRequest>,
    ) -> Result<tonic::Response<CreateSnapshotResponse>, tonic::Status> {
        let request = request.into_inner();
        tracing::trace!(volume.uuid = %request.source_volume_id, snapshot.uuid = %request.name, ?request);

        let volume_uuid = Uuid::parse_str(&request.source_volume_id).map_err(|_e| {
            Status::invalid_argument(format!(
                "Malformed volume UUID: {}",
                request.source_volume_id
            ))
        })?;
        let _guard = csi_driver::limiter::VolumeOpGuard::new(volume_uuid)?;

        // k8s side-car uses name as snapshot-{uuid} and we use uuid for idempotency.
        let re = Regex::new(SNAPSHOT_NAME_PATTERN).unwrap();
        let snapshot_uuid_str = match re.captures(&request.name) {
            Some(captures) => captures.get(1).unwrap().as_str().to_string(),
            None => {
                return Err(Status::invalid_argument(format!(
                    "Expected the snapshot name in snapshot-<UUID> format: {}",
                    request.name
                )));
            }
        };
        tracing::Span::current().record("snapshot.uuid", snapshot_uuid_str.as_str());
        let snap_uuid = Uuid::parse_str(&snapshot_uuid_str).map_err(|_e| {
            Status::invalid_argument(format!("Malformed snapshot ID: {}", request.name))
        })?;

        let snapshot = IoEngineApiClient::get_client()
            .create_volume_snapshot(&volume_uuid, &snap_uuid)
            .await?;

        Ok(tonic::Response::new(CreateSnapshotResponse {
            snapshot: Some(snapshot_to_csi(snapshot)),
        }))
    }

    #[instrument(err, fields(snapshot.uuid = request.get_ref().snapshot_id), skip(self))]
    async fn delete_snapshot(
        &self,
        request: tonic::Request<DeleteSnapshotRequest>,
    ) -> Result<tonic::Response<DeleteSnapshotResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(snapshot.uuid = %args.snapshot_id, ?args);

        let snapshot_uuid = Uuid::parse_str(&args.snapshot_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed snapshot UUID: {}", args.snapshot_id))
        })?;

        IoEngineApiClient::get_client()
            .delete_volume_snapshot(&snapshot_uuid)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to delete snapshot {}, error = {:?}",
                    args.snapshot_id, e
                ))
            })?;

        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    #[instrument(err, fields(volume.uuid = request.get_ref().source_volume_id, snapshot.source_uuid = request.get_ref().source_volume_id, snapshot.uuid), skip(self))]
    async fn list_snapshots(
        &self,
        request: tonic::Request<ListSnapshotsRequest>,
    ) -> Result<tonic::Response<ListSnapshotsResponse>, tonic::Status> {
        let request = request.into_inner();
        let opt_uuid = |src: &str, entity: &str| -> Result<Option<Uuid>, tonic::Status> {
            if src.is_empty() {
                return Ok(None);
            }
            Ok(Some(Uuid::parse_str(src).map_err(|error| {
                Status::invalid_argument(format!("{error}: Malformed {entity} UUID: {src}"))
            })?))
        };
        let snap_uuid = opt_uuid(&request.snapshot_id, "snapshot")?;
        let vol_uuid = opt_uuid(&request.source_volume_id, "volume")?;
        let max_entries = request.max_entries;
        if max_entries < 0 {
            return Err(Status::invalid_argument("max_entries can't be negative"));
        }

        let snapshots = IoEngineApiClient::get_client()
            .list_volume_snapshots(snap_uuid, vol_uuid, max_entries, request.starting_token)
            .await?;

        Ok(tonic::Response::new(ListSnapshotsResponse {
            entries: snapshots
                .entries
                .into_iter()
                .map(snapshot_to_csi)
                .map(|snapshot| list_snapshots_response::Entry {
                    snapshot: Some(snapshot),
                })
                .collect(),
            next_token: "".into(),
        }))
    }

    #[instrument(err, skip(self))]
    async fn controller_expand_volume(
        &self,
        _request: tonic::Request<ControllerExpandVolumeRequest>,
    ) -> Result<tonic::Response<ControllerExpandVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    #[instrument(err, skip(self))]
    async fn controller_get_volume(
        &self,
        _request: tonic::Request<ControllerGetVolumeRequest>,
    ) -> Result<tonic::Response<ControllerGetVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}

fn snapshot_to_csi(snapshot: models::VolumeSnapshot) -> Snapshot {
    Snapshot {
        size_bytes: snapshot.state.size as i64,
        snapshot_id: snapshot.definition.spec.uuid.to_string(),
        source_volume_id: snapshot.definition.spec.source_volume.to_string(),
        creation_time: snapshot
            .definition
            .metadata
            .timestamp
            .and_then(|t| prost_types::Timestamp::from_str(&t).ok()),
        // What about when snapshot is in deleted phase?
        ready_to_use: snapshot.definition.metadata.status == models::SpecStatus::Created,
    }
}
