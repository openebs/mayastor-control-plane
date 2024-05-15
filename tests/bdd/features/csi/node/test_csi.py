import os
import pytest
import subprocess
import time

import grpc
import csi_pb2 as pb
from common import csi_plugin_name

from common.apiclient import ApiClient
from common.csi import CsiHandle
from common.deployer import Deployer
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.publish_volume_body import PublishVolumeBody
from common.operations import Volume as VolumeOps
from common.operations import Pool as PoolOps
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_policy import VolumePolicy
from openapi.model.protocol import Protocol
from openapi.model.protocol import Protocol

POOL1_UUID = "ec176677-8202-4199-b461-2b68e53a055f"
NODE1 = "io-engine-1"
VOLUME_SIZE = 32 * 1024 * 1024


def get_uuid(n):
    return "11111100-0000-0000-0000-%.12d" % (n)


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1, csi_node=True)

    # Create 2 pools.
    pool_labels = {"openebs.io/created-by": "operator-diskpool"}
    pool_api = ApiClient.pools_api()
    pool_api.put_node_pool(
        NODE1,
        POOL1_UUID,
        CreatePoolBody(["malloc:///disk?size_mb=200"], labels=pool_labels),
    )
    yield
    PoolOps.delete_all()
    Deployer.stop()


@pytest.fixture(scope="module")
def fix_socket_permissions(setup):
    subprocess.run(
        ["sudo", "chmod", "go+rw", "/var/tmp/csi-app-node-1.sock"], check=True
    )
    yield


@pytest.fixture(scope="module")
def csi_instance(setup, fix_socket_permissions):
    yield CsiHandle("unix:///var/tmp/csi-app-node-1.sock")


def test_plugin_info(csi_instance):
    info = csi_instance.identity.GetPluginInfo(pb.GetPluginInfoRequest())
    assert info.name == csi_plugin_name
    assert info.vendor_version == "1.0.0"


def test_plugin_capabilities(csi_instance):
    response = csi_instance.identity.GetPluginCapabilities(
        pb.GetPluginCapabilitiesRequest()
    )
    services = [cap.service.type for cap in response.capabilities]
    volume_expansion = [cap.volume_expansion.type for cap in response.capabilities]
    assert pb.PluginCapability.Service.Type.CONTROLLER_SERVICE in services
    assert pb.PluginCapability.Service.Type.VOLUME_ACCESSIBILITY_CONSTRAINTS in services
    assert pb.PluginCapability.VolumeExpansion.Type.OFFLINE in volume_expansion
    assert pb.PluginCapability.VolumeExpansion.Type.ONLINE in volume_expansion


def test_probe(csi_instance):
    response = csi_instance.identity.Probe(pb.ProbeRequest())
    assert response.ready


def test_node_info(csi_instance):
    info = csi_instance.node.NodeGetInfo(pb.NodeGetInfoRequest())
    assert info.node_id == "app-node-1"
    assert info.max_volumes_per_node == 0


def test_node_capabilities(csi_instance):
    response = csi_instance.node.NodeGetCapabilities(pb.NodeGetCapabilitiesRequest())
    assert pb.NodeServiceCapability.RPC.Type.STAGE_UNSTAGE_VOLUME in [
        cap.rpc.type for cap in response.capabilities
    ]


@pytest.fixture(scope="module")
def volumes(setup):
    volumes = []

    for n in range(5):
        uuid = get_uuid(n)
        volume = ApiClient.volumes_api().put_volume(
            uuid, CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False)
        )
        volumes.append(volume)
    yield volumes
    VolumeOps.delete_all()


@pytest.fixture(scope="module")
def io_timeout():
    yield "33"


@pytest.fixture(params=["nvmf"])
def share_type(request):
    types = {
        "nbd": Protocol("nbd"),
        "nvmf": Protocol("nvmf"),
        "iscsi": Protocol("iscsi"),
    }
    yield types[request.param]


@pytest.fixture(scope="module")
def staging_target_path():
    yield "/var/tmp/staging/mount"


@pytest.fixture
def target_path():
    try:
        os.mkdir("/var/tmp/publish")
    except FileExistsError:
        pass
    yield "/var/tmp/publish/mount"


@pytest.fixture(params=["ext4", "xfs"])
def fs_type(request):
    yield request.param


@pytest.fixture
def volume_id(fs_type):
    # use a different (volume) uuid for each filesystem type
    yield get_uuid(["ext3", "ext4", "xfs"].index(fs_type))


@pytest.fixture
def published_nexus(volumes, share_type, volume_id):
    uuid = volume_id
    volume = ApiClient.volumes_api().put_volume_target(
        uuid,
        publish_volume_body=PublishVolumeBody(
            {}, Protocol("nvmf"), node=NODE1, frontend_node=""
        ),
    )
    yield volume.state["target"]
    ApiClient.volumes_api().del_volume_target(volume.spec.uuid)


@pytest.fixture
def test_get_volume_stats(csi_instance, published_nexus, volume_id, target_path):
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeGetVolumeStats(
            pb.NodeGetVolumeStatsRequest(volume_id=volume_id, volume_path=target_path)
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.fixture(params=["multi-node-reader-only", "multi-node-single-writer"])
def access_mode(request):
    MODES = {
        "single-node-writer": pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_WRITER,
        "single-node-reader-only": pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_READER_ONLY,
        "multi-node-reader-only": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_READER_ONLY,
        "multi-node-single-writer": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER,
        "multi-node-multi-writer": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_MULTI_WRITER,
    }
    yield MODES[request.param]


@pytest.fixture(params=["rw", "ro"])
def read_only(request):
    yield request.param == "ro"


@pytest.fixture
def compatible(access_mode, read_only):
    yield read_only or access_mode not in [
        pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_READER_ONLY,
        pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_READER_ONLY,
    ]


@pytest.fixture
def publish_mount_flags(read_only):
    yield ["ro"] if read_only else []


@pytest.fixture
def stage_context(published_nexus, io_timeout):
    yield {"uri": published_nexus["deviceUri"], "ioTimeout": io_timeout}


@pytest.fixture
def publish_context(published_nexus, volume_id):
    yield {"uri": published_nexus["deviceUri"]}


@pytest.fixture
def block_volume_capability(access_mode):
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        block=pb.VolumeCapability.BlockVolume(),
    )


@pytest.fixture
def stage_mount_volume_capability(access_mode, fs_type):
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        mount=pb.VolumeCapability.MountVolume(fs_type=fs_type, mount_flags=[]),
    )


@pytest.fixture
def publish_mount_volume_capability(access_mode, fs_type, publish_mount_flags):
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        mount=pb.VolumeCapability.MountVolume(
            fs_type=fs_type, mount_flags=publish_mount_flags
        ),
    )


@pytest.fixture
def staged_block_volume(
    csi_instance, volume_id, stage_context, staging_target_path, block_volume_capability
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=block_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    yield
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_stage_block_volume(
    csi_instance, volume_id, stage_context, staging_target_path, block_volume_capability
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=block_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    time.sleep(0.5)
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_publish_block_volume(
    csi_instance,
    volume_id,
    publish_context,
    staging_target_path,
    target_path,
    block_volume_capability,
    read_only,
    staged_block_volume,
    compatible,
):
    if compatible:
        csi_instance.node.NodePublishVolume(
            pb.NodePublishVolumeRequest(
                volume_id=volume_id,
                publish_context=publish_context,
                staging_target_path=staging_target_path,
                target_path=target_path,
                volume_capability=block_volume_capability,
                readonly=read_only,
                secrets={},
                volume_context={},
            )
        )
        time.sleep(0.5)
        csi_instance.node.NodeUnpublishVolume(
            pb.NodeUnpublishVolumeRequest(volume_id=volume_id, target_path=target_path)
        )
    else:
        with pytest.raises(grpc.RpcError) as error:
            csi_instance.node.NodePublishVolume(
                pb.NodePublishVolumeRequest(
                    volume_id=volume_id,
                    publish_context=publish_context,
                    staging_target_path=staging_target_path,
                    target_path=target_path,
                    volume_capability=block_volume_capability,
                    readonly=read_only,
                    secrets={},
                    volume_context={},
                )
            )
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.fixture
def staged_mount_volume(
    csi_instance,
    volume_id,
    stage_context,
    staging_target_path,
    stage_mount_volume_capability,
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=stage_mount_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    yield
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_stage_mount_volume(
    csi_instance,
    volume_id,
    stage_context,
    staging_target_path,
    stage_mount_volume_capability,
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=stage_mount_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    time.sleep(0.5)
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_publish_mount_volume(
    csi_instance,
    volume_id,
    publish_context,
    staging_target_path,
    target_path,
    publish_mount_volume_capability,
    read_only,
    staged_mount_volume,
    compatible,
):
    if compatible:
        csi_instance.node.NodePublishVolume(
            pb.NodePublishVolumeRequest(
                volume_id=volume_id,
                publish_context=publish_context,
                staging_target_path=staging_target_path,
                target_path=target_path,
                volume_capability=publish_mount_volume_capability,
                readonly=read_only,
                secrets={},
                volume_context={},
            )
        )
        time.sleep(0.5)
        csi_instance.node.NodeUnpublishVolume(
            pb.NodeUnpublishVolumeRequest(volume_id=volume_id, target_path=target_path)
        )
    else:
        with pytest.raises(grpc.RpcError) as error:
            csi_instance.node.NodePublishVolume(
                pb.NodePublishVolumeRequest(
                    volume_id=volume_id,
                    publish_context=publish_context,
                    staging_target_path=staging_target_path,
                    target_path=target_path,
                    volume_capability=publish_mount_volume_capability,
                    readonly=read_only,
                    secrets={},
                    volume_context={},
                )
            )
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
