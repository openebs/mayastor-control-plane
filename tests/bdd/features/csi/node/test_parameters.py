"""CSI Node plugin parameters feature tests."""
import os
import pytest
import threading
import time
from pytest_bdd import given, scenario, then, when, parsers
import subprocess

import csi_pb2 as pb

from common.apiclient import ApiClient
from common.deployer import Deployer
from common.nvme import nvme_find_device
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.protocol import Protocol
from openapi.model.volume_policy import VolumePolicy

from common.csi import CsiHandle


VOLUME_UUID = "f04e4756-999f-446f-8610-fbf879aff2a7"
NODE1 = "io-engine-1"


@scenario(
    "parameters.feature", "stage volume request with a specified nvme nr io queues"
)
def test_stage_volume_request_with_a_specified_nvme_nr_io_queues():
    """stage volume request with a specified nvme nr io queues."""


@given(
    parsers.parse("a csi node plugin with {io:d} IO queues configured"),
    target_fixture="io_queues",
)
def a_csi_node_plugin_with_io_queues_configured(io):
    """a csi node plugin with <IO> queues configured."""
    return io


@given("an io-engine cluster")
def an_io_engine_cluster(setup):
    """an io-engine cluster."""


@when("staging a volume")
def staging_a_volume(staging_a_volume):
    """staging a volume."""


@then(parsers.parse("the nvme device should report {total:d} TOTAL queues"))
def the_nvme_device_should_report_total_queues(total):
    """the nvme device should report <TOTAL> queues."""
    print(total)
    device = volume_device()
    print(device)
    dev = device.replace("/dev/", "").replace("n1", "")
    file = f"/sys/class/nvme/{dev}/queue_count"
    queue_count = int(subprocess.run(["sudo", "cat", file], capture_output=True).stdout)
    print(f"queue_count: {queue_count}")
    # max io queues is cpu_count
    # admin q is 1
    assert queue_count == min(total, os.cpu_count() + 1)
    file = f"/sys/block/nvme2c2n1/queue/io_timeout"
    io_timoout = int(subprocess.run(["sudo", "cat", file], capture_output=True).stdout)
    print(f"io_timeout: {io_timoout}")
    assert io_timoout == 33000


@pytest.fixture
def block_volume_capability():
    access_mode = pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_READER_ONLY
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        block=pb.VolumeCapability.BlockVolume(),
    )


@pytest.fixture
def staging_a_volume(staging_target_path, csi_instance, block_volume_capability):
    volume = ApiClient.volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(VolumePolicy(False), 1, 10241024, False)
    )
    volume = ApiClient.volumes_api().put_volume_target(
        volume.spec.uuid,
        publish_volume_body=PublishVolumeBody(
            {}, Protocol("nvmf"), node=NODE1, frontend_node=""
        ),
    )
    device_uri = volume.state["target"]["deviceUri"]
    print(device_uri)
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume.spec.uuid,
            publish_context={"uri": volume.state["target"]["deviceUri"]},
            staging_target_path=staging_target_path,
            volume_capability=block_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    yield
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=VOLUME_UUID, staging_target_path=staging_target_path
        )
    )
    ApiClient.volumes_api().del_volume(volume.spec.uuid)


def volume_device():
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    device = nvme_find_device(volume.state["target"]["deviceUri"])
    return device


@pytest.fixture(scope="module")
def staging_target_path():
    yield "/tmp/staging/mount"


@pytest.fixture
def start_csi_plugin(setup, io_queues, staging_target_path):
    def monitor(proc, result):
        stdout, stderr = proc.communicate()
        result["stdout"] = stdout.decode()
        result["stderr"] = stderr.decode()
        result["status"] = proc.returncode

    try:
        subprocess.run(["sudo", "umount", staging_target_path], check=True)
    except:
        pass

    proc = subprocess.Popen(
        args=[
            "sudo",
            os.environ["WORKSPACE_ROOT"] + "/target/debug/csi-node",
            "--csi-socket=/var/tmp/csi-node.sock",
            "--grpc-endpoint=0.0.0.0:50050",
            "--node-name=msn-test",
            "--nvme-io-timeout=33s",
            f"--nvme-nr-io-queues={io_queues}",
            "-v",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    result = {}
    handler = threading.Thread(target=monitor, args=[proc, result])
    handler.start()
    time.sleep(1)
    yield
    time.sleep(1)
    subprocess.run(["sudo", "pkill", "csi-node"], check=True)
    handler.join()
    print("[CSI] exit status: %d" % (result["status"]))
    print(result["stdout"])
    print(result["stderr"])


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1)

    pool_api = ApiClient.pools_api()
    pool = pool_api.put_node_pool(
        "io-engine-1",
        "pool-1",
        CreatePoolBody(["malloc:///disk?size_mb=200"]),
    )
    yield
    ApiClient.pools_api().del_pool(pool.id)
    Deployer.stop()


@pytest.fixture
def fix_socket_permissions(start_csi_plugin):
    subprocess.run(["sudo", "chmod", "go+rw", "/var/tmp/csi-node.sock"], check=True)
    yield


@pytest.fixture
def csi_instance(start_csi_plugin, fix_socket_permissions):
    yield CsiHandle("unix:///var/tmp/csi-node.sock")
