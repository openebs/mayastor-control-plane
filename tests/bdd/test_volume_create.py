"""Volume creation feature tests."""
import time

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import docker
import requests

import common

from openapi.openapi_client.model.create_pool_body import CreatePoolBody
from openapi.openapi_client.model.create_volume_body import CreateVolumeBody
from openapi.openapi_client.model.volume_spec import VolumeSpec
from openapi.openapi_client.model.protocol import Protocol
from openapi.openapi_client.model.spec_state import SpecState
from openapi.openapi_client.model.volume_state import VolumeState
from openapi.openapi_client.model.volume_status import VolumeStatus

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 1
REST_SERVER = "http://localhost:8081/v0"
CREATE_REQUEST_KEY = "create_request"
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME = "mayastor"


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool is created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    common.deployer_start(1)
    common.get_pools_api().put_node_pool(
        NODE_NAME, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=50"])
    )
    yield
    common.deployer_stop()


# Fixture used to pass the volume create request between test steps.
@pytest.fixture(scope="function")
def create_request():
    return {}


@scenario(
    "features/volume/create.feature", "provisioning failure due to missing Mayastor"
)
def test_provisioning_failure():
    """provisioning failure."""


@scenario(
    "features/volume/create.feature", "desired number of replicas cannot be created"
)
def test_desired_number_of_replicas_cannot_be_created():
    """desired number of replicas cannot be created."""


@scenario("features/volume/create.feature", "sufficient suitable pools")
def test_sufficient_suitable_pools():
    """sufficient suitable pools."""


@given("a control plane, Mayastor instances and a pool")
def a_control_plane_a_mayastor_instance_and_a_pool():
    """a control plane, Mayastor instances and a pool."""
    docker_client = docker.from_env()

    # The control plane comprises the core agents, rest server and etcd instance.
    for component in ["core", "rest", "etcd"]:
        common.check_container_running(component)

    # Check all Mayastor instances are running
    try:
        mayastors = docker_client.containers.list(
            all=True, filters={"name": "mayastor"}
        )
    except docker.errors.NotFound:
        raise Exception("No Mayastor instances")

    for mayastor in mayastors:
        common.check_container_running(mayastor.attrs["Name"])

    # Check for a pool
    pool = common.get_pools_api().get_pool(POOL_UUID)
    assert pool.id == POOL_UUID


@given("a request for a volume")
def a_request_for_a_volume(create_request):
    """a request for a volume."""
    policy = {"self_heal": False, "topology": None}
    topology = {"explicit": None, "labelled": None}
    request = CreateVolumeBody(policy, NUM_VOLUME_REPLICAS, VOLUME_SIZE, topology)
    create_request[CREATE_REQUEST_KEY] = request


@when("the number of suitable pools is less than the number of desired volume replicas")
def the_number_of_suitable_pools_is_less_than_the_number_of_desired_volume_replicas(
    create_request,
):
    """the number of suitable pools is less than the number of desired volume replicas."""
    # Delete the pool so that there aren't enough
    pools_api = common.get_pools_api()
    pools_api.del_pool(POOL_UUID)
    num_pools = len(pools_api.get_pools())
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    assert num_pools < num_volume_replicas


@when(
    "the number of volume replicas is less than or equal to the number of suitable pools"
)
def the_number_of_volume_replicas_is_less_than_or_equal_to_the_number_of_suitable_pools(
    create_request,
):
    """the number of volume replicas is less than or equal to the number of suitable pools."""
    num_pools = len(common.get_pools_api().get_pools())
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    assert num_volume_replicas <= num_pools


@when("there are no available Mayastor instances")
def there_are_no_available_mayastor_instances():
    """there are no available Mayastor instances."""
    # Kill mayastor instance
    docker_client = docker.from_env()
    container = docker_client.containers.get("mayastor")
    container.kill()


@then("volume creation should fail with an insufficient storage error")
def volume_creation_should_fail_with_an_insufficient_storage_error(create_request):
    """volume creation should fail with an insufficient storage error."""
    request = create_request[CREATE_REQUEST_KEY]
    try:
        common.get_volumes_api().put_volume(VOLUME_UUID, request)
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["insufficient_storage"]
    # TODO: Uncomment the "finally" clause when CAS-1059 is completed.
    # finally:
    #     # Check that the volume wasn't created.
    #     volumes = common.get_volumes_api().get_volumes()
    #     assert len(volumes) == 0


@then("volume creation should succeed with a returned volume object")
def volume_creation_should_succeed_with_a_returned_volume_object(create_request):
    """volume creation should succeed with a returned volume object."""
    cfg = common.get_cfg()
    expected_spec = VolumeSpec(
        [],
        1,
        1,
        Protocol("none"),
        VOLUME_SIZE,
        SpecState("Created"),
        VOLUME_UUID,
        _configuration=cfg,
    )
    expected_state = VolumeState(
        [],
        Protocol("none"),
        VOLUME_SIZE,
        VolumeStatus("Online"),
        VOLUME_UUID,
        _configuration=cfg,
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = common.get_volumes_api().put_volume(VOLUME_UUID, request)
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state) == str(expected_state)
