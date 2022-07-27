use crate::{ApiClientError, CreateVolumeTopology, CsiControllerConfig, IoEngineApiClient};
use regex::Regex;
use rpc::csi::*;
use std::collections::{HashMap, HashSet};
use tonic::{Response, Status};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

use common_lib::types::v0::openapi::models::{
    Pool, PoolStatus, SpecStatus, Volume, VolumeShareProtocol,
};
use utils::{CREATED_BY_KEY, DSP_OPERATOR};

use rpc::csi::Topology as CsiTopology;

const OPENEBS_TOPOLOGY_KEY: &str = "openebs.io/nodename";
const VOLUME_NAME_PATTERN: &str =
    r"pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";
const SUPPORTED_FS_TYPES: [&str; 2] = ["ext4", "xfs"];

#[derive(Debug, Default)]
pub struct CsiControllerSvc {}

// TODO: Implement VolumeOpts
mod volume_opts {
    pub const IO_TIMEOUT: &str = "ioTimeout";
    pub const LOCAL_VOLUME: &str = "local";

    const YAML_TRUE_VALUE: [&str; 11] = [
        "y", "Y", "yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON",
    ];

    // Decode 'local' volume attribute into a boolean flag.
    pub fn decode_local_volume_flag(encoded: Option<&String>) -> bool {
        match encoded {
            Some(v) => YAML_TRUE_VALUE.iter().any(|p| p == v),
            None => true,
        }
    }
}

/// Check whether the passed fs type is supported or not,
/// if not provided we call it valid as fsType is an optional parameter
pub fn valid_fs_type(fs_type: Option<&String>) -> bool {
    match fs_type {
        Some(fs) => SUPPORTED_FS_TYPES.iter().any(|p| p == fs),
        None => true,
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
            "Invalid protocol: {:?}",
            proto
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
            error => Status::internal(format!("Operation failed: {:?}", error)),
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
    _pinned_volume: bool,
    thin: bool,
) -> Result<(), Status> {
    // Check if the existing volume is compatible, which means
    //  - number of replicas is equal or greater
    //  - size is equal or greater
    //  - volume is fully created
    let spec = &volume.spec;

    if spec.status != SpecStatus::Created {
        return Err(Status::already_exists(format!(
            "Existing volume {} is in insufficient state: {:?}",
            spec.uuid, spec.status
        )));
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
    /// If volume is created as pinned (i.e. local=true), then the nexus and the workload
    /// must be placed on the same node, which in fact means running workloads only on IO Engine
    /// daemonset nodes.
    /// For non-pinned volumes, workload can be put on any node in the cluster.
    pub fn volume_accessible_topology(&self, pinned_volume: bool) -> Vec<CsiTopology> {
        if pinned_volume {
            vec![rpc::csi::Topology {
                segments: CsiControllerConfig::get_config().io_engine_selector(),
            }]
        } else {
            Vec::new()
        }
    }

    /// Determines whether target volume is pinned.
    pub fn is_volume_pinned(volume: &Volume) -> bool {
        if let Some(labels) = &volume.spec.labels {
            volume_opts::decode_local_volume_flag(labels.get(volume_opts::LOCAL_VOLUME))
        } else {
            true
        }
    }
}

#[tonic::async_trait]
impl rpc::csi::controller_server::Controller for CsiControllerSvc {
    #[instrument(error, fields(volume.uuid = tracing::field::Empty))]
    async fn create_volume(
        &self,
        request: tonic::Request<CreateVolumeRequest>,
    ) -> Result<tonic::Response<CreateVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(request = ?args);

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
        tracing::Span::current().record("volume.uuid", &volume_uuid.as_str());

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

        // Check filesystem type.
        if !valid_fs_type(args.parameters.get("fsType")) {
            return Err(Status::invalid_argument("Invalid filesystem type"));
        }

        // Check storage protocol.
        let protocol = parse_protocol(args.parameters.get("protocol"))?;

        // Check I/O timeout.
        if let Some(io_timeout) = args.parameters.get(volume_opts::IO_TIMEOUT) {
            if protocol != VolumeShareProtocol::Nvmf {
                return Err(Status::invalid_argument(
                    "I/O timeout is valid only for nvmf protocol",
                ));
            }
            if io_timeout.parse::<u64>().is_err() {
                return Err(Status::invalid_argument("Invalid I/O timeout"));
            }
        }

        let replica_count: u8 = match args.parameters.get("repl") {
            Some(c) => match c.parse::<u8>() {
                Ok(c) => {
                    if c == 0 {
                        return Err(Status::invalid_argument(
                            "Replica count must be greater than zero",
                        ));
                    }
                    c
                }
                Err(_) => return Err(Status::invalid_argument("Invalid replica count")),
            },
            None => 1,
        };

        // Currently we only support pinned volumes
        let pinned_volume = true;

        // For explanation of accessibilityRequirements refer to a table at
        // https://github.com/kubernetes-csi/external-provisioner.
        // Our case is WaitForFirstConsumer = true, strict-topology = false.
        //
        // The first node in preferred array the node that was chosen for running
        // the app by the k8s scheduler. The rest of the entries are in random
        // order and perhaps don't even run the csi node plugin.
        //
        // The requisite array contains all nodes in the cluster irrespective
        // of what node was chosen for running the app.
        let mut allowed_nodes: HashSet<String> = HashSet::new();
        let mut preferred_nodes: HashSet<String> = HashSet::new();
        let mut inclusive_label_topology: HashMap<String, String> = HashMap::new();
        let supported_keys = vec![OPENEBS_TOPOLOGY_KEY];

        inclusive_label_topology.insert(String::from(CREATED_BY_KEY), String::from(DSP_OPERATOR));

        if let Some(reqs) = args.accessibility_requirements {
            for r in reqs.requisite.iter() {
                for (k, v) in r.segments.iter() {
                    // Reject all others than `supported_keys`
                    if supported_keys.contains(&k.as_str()) {
                        allowed_nodes.insert(v.to_string());
                    } else {
                        return Err(Status::invalid_argument(format!(
                            "Volume topology key other than {} is not supported",
                            OPENEBS_TOPOLOGY_KEY
                        )));
                    }
                }
            }

            for p in reqs.preferred.iter() {
                for (k, v) in p.segments.iter() {
                    // Reject all others than `supported_keys`
                    if supported_keys.contains(&k.as_str()) {
                        preferred_nodes.insert(v.to_string());
                    }
                }
            }
        }

        let u = Uuid::parse_str(&volume_uuid).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", volume_uuid))
        })?;

        let vt_mapper = VolumeTopologyMapper::init().await?;

        let thin = match args.parameters.get("thin") {
            Some(value) => value == "true",
            None => false,
        };

        // First check if the volume already exists.
        match IoEngineApiClient::get_client().get_volume(&u).await {
            Ok(volume) => {
                check_existing_volume(&volume, replica_count, size, pinned_volume, thin)?;
                debug!(
                    "Volume {} already exists and is compatible with requested config",
                    volume_uuid
                );
            }
            // If the volume doesn't exist, create it.
            Err(ApiClientError::ResourceNotExists(_)) => {
                let volume_topology = CreateVolumeTopology::new(
                    allowed_nodes.into_iter().collect::<Vec<String>>(),
                    preferred_nodes.into_iter().collect::<Vec<String>>(),
                    inclusive_label_topology,
                );

                IoEngineApiClient::get_client()
                    .create_volume(
                        &u,
                        replica_count,
                        size,
                        volume_topology,
                        pinned_volume,
                        thin,
                    )
                    .await?;

                debug!(
                    "Volume {} successfully created, pinned volume = {}",
                    volume_uuid, pinned_volume
                );
            }
            Err(e) => return Err(e.into()),
        }

        let volume = rpc::csi::Volume {
            capacity_bytes: size as i64,
            volume_id: volume_uuid,
            volume_context: args.parameters.clone(),
            content_source: None,
            accessible_topology: vt_mapper.volume_accessible_topology(pinned_volume),
        };

        debug!("Created volume: {:?}", volume);
        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume),
        }))
    }
    #[instrument(error, fields(volume.uuid = %request.get_ref().volume_id))]
    async fn delete_volume(
        &self,
        request: tonic::Request<DeleteVolumeRequest>,
    ) -> Result<tonic::Response<DeleteVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(volume.uuid = %args.volume_id, request = ?args);

        let volume_uuid = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;

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

    #[instrument(error, fields(volume.uuid = %request.get_ref().volume_id))]
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
        let node_id = args.node_id;

        if args.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID must not be empty"));
        }
        let volume_id = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;

        match args.volume_capability {
            Some(c) => {
                check_volume_capabilities(&[c])?;
            }
            None => {
                return Err(Status::invalid_argument("Missing volume capability"));
            }
        };

        // Check if the volume is already published.
        let volume = IoEngineApiClient::get_client()
            .get_volume(&volume_id)
            .await?;

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
                                "Volume {} already published on a different node: {}",
                                volume_id, node,
                            );
                            error!("{}", m);
                            return Err(Status::failed_precondition(m));
                        }

                        debug!("Volume {} already published at {}", volume_id, uri);
                        uri
                    } else {
                        let m = format!(
                            "Volume {} reports no info about its publishing status",
                            volume_id
                        );
                        error!("{}", m);
                        return Err(Status::internal(m));
                    }
                },
            _ => {
                // Volume is not published.
                let v = IoEngineApiClient::get_client()
                    .publish_volume(&volume_id, &node_id, protocol)
                    .await?;

                if let Some((node, uri)) = get_volume_share_location(&v) {
                    debug!(
                        "Volume {} successfully published on node {} via {}",
                        volume_id, node, uri
                    );
                    uri
                } else {
                    let m = format!(
                        "Volume {} has been successfully published but URI is not available",
                        volume_id
                    );
                    error!("{}", m);
                    return Err(Status::internal(m));
                }
            }
        };

        // Prepare the context for the IoEngine Node CSI plugin.
        let mut publish_context = HashMap::new();
        publish_context.insert("uri".to_string(), uri);

        if let Some(io_timeout) = args.volume_context.get(volume_opts::IO_TIMEOUT) {
            publish_context.insert(volume_opts::IO_TIMEOUT.to_string(), io_timeout.to_string());
        }

        debug!(
            "Publish context for volume {}: {:?}",
            volume_id, publish_context
        );
        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context,
        }))
    }

    #[instrument(error, fields(volume.uuid = %request.get_ref().volume_id))]
    async fn controller_unpublish_volume(
        &self,
        request: tonic::Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerUnpublishVolumeResponse>, tonic::Status> {
        let args = request.into_inner();
        tracing::trace!(volume.uuid = %args.volume_id, request = ?args);

        let volume_uuid = Uuid::parse_str(&args.volume_id).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", args.volume_id))
        })?;
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

        // Check if target volume is published and the node matches.
        if let Some(target) = &volume.spec.target.as_ref() {
            if !args.node_id.is_empty() && target.node != args.node_id {
                return Err(Status::not_found(format!(
                    "Volume {} is published on a different node: {}",
                    &args.volume_id, target.node
                )));
            }
        } else {
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

    #[instrument(error, fields(volume.uuid = %request.get_ref().volume_id))]
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

    #[instrument(error)]
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
            .map_err(|e| Status::internal(format!("Failed to list volumes, error = {:?}", e)))?;

        let entries = volumes
            .entries
            .into_iter()
            .map(|v| {
                let volume = rpc::csi::Volume {
                    volume_id: v.spec.uuid.to_string(),
                    capacity_bytes: v.spec.size as i64,
                    volume_context: HashMap::new(),
                    content_source: None,
                    accessible_topology: vt_mapper
                        .volume_accessible_topology(VolumeTopologyMapper::is_volume_pinned(&v)),
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

    #[instrument(error)]
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
                        "Failed to list pools for node {}, error = {:?}",
                        node, e,
                    ))
                })?
        } else {
            debug!("Calculating overall pool capacity");
            IoEngineApiClient::get_client()
                .list_pools()
                .await
                .map_err(|e| {
                    Status::internal(format!("Failed to list all pools, error = {:?}", e,))
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

    #[instrument(error)]
    async fn controller_get_capabilities(
        &self,
        _request: tonic::Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<tonic::Response<ControllerGetCapabilitiesResponse>, tonic::Status> {
        let capabilities = vec![
            controller_service_capability::rpc::Type::CreateDeleteVolume,
            controller_service_capability::rpc::Type::PublishUnpublishVolume,
            controller_service_capability::rpc::Type::ListVolumes,
            controller_service_capability::rpc::Type::GetCapacity,
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

    #[instrument(error)]
    async fn create_snapshot(
        &self,
        _request: tonic::Request<CreateSnapshotRequest>,
    ) -> Result<tonic::Response<CreateSnapshotResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    #[instrument(error)]
    async fn delete_snapshot(
        &self,
        _request: tonic::Request<DeleteSnapshotRequest>,
    ) -> Result<tonic::Response<DeleteSnapshotResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    #[instrument(error)]
    async fn list_snapshots(
        &self,
        _request: tonic::Request<ListSnapshotsRequest>,
    ) -> Result<tonic::Response<ListSnapshotsResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    #[instrument(error)]
    async fn controller_expand_volume(
        &self,
        _request: tonic::Request<ControllerExpandVolumeRequest>,
    ) -> Result<tonic::Response<ControllerExpandVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    #[instrument(error)]
    async fn controller_get_volume(
        &self,
        _request: tonic::Request<ControllerGetVolumeRequest>,
    ) -> Result<tonic::Response<ControllerGetVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
