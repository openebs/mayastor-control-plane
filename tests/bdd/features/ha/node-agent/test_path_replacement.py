"""Swap ANA enabled Nexus on ANA enabled host feature tests."""
import http

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)
import pytest
import subprocess
from time import sleep

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.ha import HaNodeHandle
from common.fio import Fio
from common.nvme import (
    nvme_connect,
    nvme_disconnect,
    nvme_list_subsystems,
    nvme_disconnect_controller,
)

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_policy import VolumePolicy
from openapi.model.protocol import Protocol
from openapi.exceptions import ApiException

import cluster_agent_pb2 as pb

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_NODE = "io-engine-3"
TARGET_NODE_1 = "io-engine-1"
TARGET_NODE_2 = "io-engine-2"
NEXUS_NQN = "nqn.2019-05.io.openebs:%s" % VOLUME_UUID
# Interval to wait before HA Node agent classifies broken path as failed
PATH_DETECTION_TIME = 6
# FIO should be active long enough to outlive the detection interval.
FIO_RUNTIME = PATH_DETECTION_TIME * 2


@scenario(
    "node_agent.feature",
    "replace failed I/O path on demand for NVMe controller",
)
def test_replace_failed_io_path_on_demand_for_nvme_controller():
    """replace failed I/O path on demand for NVMe controller."""


@given("a client connected to one nexus via single I/O path")
def a_client_connected_to_one_nexus_via_single_io_path(connect_to_first_path):
    """a client connected to one nexus via single I/O path."""
    pass


@given(
    "a control plane, 2 ANA-enabled Io-Engine instances, 1 ANA-enabled host and a published volume"
)
def a_control_plane_2_anaenabled_io_engine_instances_1_anaenabled_host_and_a_published_volume(
    background,
):
    """a control plane, 2 ANA-enabled Io-Engine instances, 1 ANA-enabled host and a published volume."""
    volume = background
    assert hasattr(volume.state, "target")
    pass


@given("fio client is running against target nexus")
def fio_client_is_running_against_target_nexus(run_fio_to_first_path):
    """fio client is running against target nexus."""
    pass


@given("a running ha node agent", target_fixture="ha_node_instance")
def a_running_ha_node_agent():
    return HaNodeHandle("127.0.0.1:11600")


@when("the only I/O path degrades")
def the_only_io_path_degrades(degrade_first_path):
    """the only I/O path degrades."""
    pass


@then("fio client should successfully complete with the replaced I/O path")
def fio_client_should_successfully_complete_with_the_replaced_io_path(
    fio_completes_successfully,
):
    """fio client should successfully complete with the replaced I/O path."""
    pass


@then("it should be possible to create a second nexus and replace failed path with it")
def it_should_be_possible_to_create_a_second_nexus_and_replace_failed_path_with_it(
    publish_to_node_2, replace_failed_path_with_node2
):
    """it should be possible to create a second nexus and connect it as the second path."""
    pass


"""" FixTure Implementations """


@pytest.fixture
def background():
    Deployer.start(
        3,
        node_agent=True,
        cluster_agent=True,
        cache_period="1s",
        io_engine_env="NEXUS_NVMF_ANA_ENABLE=1,NEXUS_NVMF_RESV_ENABLE=1",
        agents_env="TEST_NEXUS_NVMF_ANA_ENABLE=1",
    )

    ApiClient.pools_api().put_node_pool(
        POOL_NODE, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=100"])
    )
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False)
    )
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID, Protocol("nvmf"), node=TARGET_NODE_1
    )
    yield volume
    Deployer.stop()


@pytest.fixture
def connect_to_first_path(background):
    volume = background
    device_uri = volume.state["target"]["deviceUri"]
    yield nvme_connect(device_uri)
    nvme_disconnect(device_uri)


@pytest.fixture
def run_fio_to_first_path(connect_to_first_path):
    device = connect_to_first_path
    desc = nvme_list_subsystems(device)
    assert (
        len(desc["Subsystems"]) == 1
    ), "Must be exactly one NVMe subsystem for target nexus"
    subsystem = desc["Subsystems"][0]
    assert len(subsystem["Paths"]) == 1, "Must be exactly one I/O path to target nexus"
    assert subsystem["Paths"][0]["State"] == "live", "I/O path is not healthy"
    # Launch fio in background and let it always run along with the test.
    fio = Fio("job", "randread", device, runtime=FIO_RUNTIME).build()
    return subprocess.Popen(fio, shell=True)


@pytest.fixture
def degrade_first_path():
    Docker.kill_container(TARGET_NODE_1)
    # Sleep some time to allow HA node agent detect failed path.
    # Add one extra second to make sure detection 100% happens.
    sleep(PATH_DETECTION_TIME + 1)


@pytest.fixture
def publish_to_node_2(background):
    volume = background
    device_uri = volume.state["target"]["deviceUri"]

    try:
        ApiClient.volumes_api().del_volume_target(VOLUME_UUID)
    except ApiException as e:
        # Timeout or node not online
        assert (
            e.status == http.HTTPStatus.REQUEST_TIMEOUT
            or e.status == http.HTTPStatus.PRECONDITION_FAILED
        )

    ApiClient.volumes_api().del_volume_target(VOLUME_UUID, force="true")
    volume_updated = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID, Protocol("nvmf"), node=TARGET_NODE_2
    )
    device_uri_2 = volume_updated.state["target"]["deviceUri"]
    assert device_uri != device_uri_2
    return device_uri_2


@pytest.fixture
def replace_failed_path_with_node2(
    connect_to_first_path, publish_to_node_2, ha_node_instance
):
    ha_node_instance.api.ReplacePath(
        pb.ReplacePathRequest(target_nqn=NEXUS_NQN, new_path=publish_to_node_2)
    )

    device = connect_to_first_path
    desc = nvme_list_subsystems(device)
    subsystem = desc["Subsystems"][0]
    assert len(subsystem["Paths"]) == 1, "Second nexus must be added to I/O path"
    assert (
        subsystem["Paths"][0]["State"] == "live"
    ), "Healthy I/O path has incorrect state"


@pytest.fixture
def fio_completes_successfully(run_fio_to_first_path):
    try:
        code = run_fio_to_first_path.wait(timeout=FIO_RUNTIME * 2)
    except subprocess.TimeoutExpired:
        assert False, "FIO timed out"
    assert code == 0, "FIO failed, exit code: %d" % code