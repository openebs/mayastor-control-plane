"""Volume creation feature tests."""
import json
import os
import subprocess
import time

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import docker

from openapi.openapi_client import api_client
from openapi.openapi_client import configuration
from openapi.openapi_client.api.volumes_api import VolumesApi
from openapi.openapi_client.api.pools_api import PoolsApi
from openapi.openapi_client.model.create_pool_body import CreatePoolBody
from openapi.openapi_client.model.create_volume_body import CreateVolumeBody
from openapi.openapi_client.model.volume import Volume
from openapi.openapi_client.model.volume_spec import VolumeSpec
from openapi.openapi_client.model.protocol import Protocol
from openapi.openapi_client.model.spec_state import SpecState
from openapi.openapi_client.model.volume_state import VolumeState
from openapi.openapi_client.model.volume_status import VolumeStatus

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 1
REST_SERVER = "http://localhost:8081/v0"
SUCCESSFUL_CREATE_KEY = "created_volume"

# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool is created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    deployer_path = os.environ["SRCDIR"] + "/target/debug/deployer"
    subprocess.run([deployer_path, "start"])

    # Allow time for containers to start
    time.sleep(5)

    # Create a pool
    get_pools_api().put_node_pool(
        "mayastor", "pool1", CreatePoolBody(["malloc:///disk?size_mb=500"])
    )

    yield
    subprocess.run([deployer_path, "stop"])


@pytest.fixture(scope="function")
def create_context():
    return {}


# Return a configuration which can be used for API calls.
# This is necessary for the API calls so that parameter type conversions can be performed. If the
# configuration is not passed, a type error is raised.
def get_cfg():
    return configuration.Configuration(host=REST_SERVER, discard_unknown_keys=True)


# Return a VolumesApi object which can be used for performing volume related REST calls.
def get_volumes_api():
    api = api_client.ApiClient(get_cfg())
    return VolumesApi(api)


# Return a PoolsApi object which can be used for performing pool related REST calls.
def get_pools_api():
    api = api_client.ApiClient(get_cfg())
    return PoolsApi(api)


# @scenario("features/volume/create.feature", "provisioning failure")
# def test_provisioning_failure():
#     """provisioning failure."""
#
#
# @scenario("features/volume/create.feature", "spec cannot be satisfied")
# def test_spec_cannot_be_satisfied():
#     """spec cannot be satisfied."""


@scenario("features/volume/create.feature", "successful creation")
def test_successful_creation():
    """successful creation."""


@given("a control plane")
def a_control_plane():
    """a control plane."""
    docker_client = docker.from_env()
    control_plane_components = ["core", "rest", "etcd"]
    for component in control_plane_components:
        try:
            container = docker_client.containers.get(component)
        except docker.errors.NotFound as exc:
            raise Exception("{} container not found", component)
        else:
            container_state = container.attrs["State"]
            if container_state["Status"] != "running":
                raise Exception("{} container not running", component)


@given("one or more Mayastor instances")
def one_or_more_mayastor_instances():
    """one or more Mayastor instances."""
    docker_client = docker.from_env()
    try:
        mayastors = docker_client.containers.list(
            all=True, filters={"name": "mayastor"}
        )
    except docker.errors.NotFound as exc:
        raise Exception("No Mayastor instances")

    # Check all Mayastor instances are running
    for mayastor in mayastors:
        container_state = mayastor.attrs["State"]
        if container_state["Status"] != "running":
            raise Exception("{} container not running", mayastor.attrs["Name"])


@when("a user attempts to create a volume")
def a_user_attempts_to_create_a_volume(create_context):
    """a user attempts to create a volume."""
    policy = {"self_heal": False, "topology": None}
    topology = {"explicit": None, "labelled": None}
    volume = get_volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(policy, NUM_VOLUME_REPLICAS, VOLUME_SIZE, topology),
    )
    create_context[SUCCESSFUL_CREATE_KEY] = volume


@when("during the provisioning there is a failure")
def during_the_provisioning_there_is_a_failure():
    """during the provisioning there is a failure."""
    raise NotImplementedError


@when("the request can initially be satisfied")
def the_request_can_initially_be_satisfied():
    """the request can initially be satisfied."""
    raise NotImplementedError


@when("the spec of the volume cannot be satisfied")
def the_spec_of_the_volume_cannot_be_satisfied():
    """the spec of the volume cannot be satisfied."""
    raise NotImplementedError


@when("there are at least as many suitable pools as there are requested replicas")
def there_are_at_least_as_many_suitable_pools_as_there_are_requested_replicas():
    """there are at least as many suitable pools as there are requested replicas."""
    pools = get_pools_api().get_pools()
    assert len(pools) == NUM_VOLUME_REPLICAS


@then("the partially created volume should eventually be cleaned up")
def the_partially_created_volume_should_eventually_be_cleaned_up():
    """the partially created volume should eventually be cleaned up."""
    raise NotImplementedError


@then("the reason the volume could not be created should be returned")
def the_reason_the_volume_could_not_be_created_should_be_returned():
    """the reason the volume could not be created should be returned."""
    raise NotImplementedError


@then("the volume object should be returned")
def the_volume_object_should_be_returned(create_context):
    """the volume object should be returned."""

    # Check the volume object was returned on create.
    assert SUCCESSFUL_CREATE_KEY in create_context
    assert len(create_context) == 1

    cfg = get_cfg()
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
        VOLUME_UUID,
        _configuration=cfg,
        status=VolumeStatus("Online"),
    )

    # Check the volume object returned on create is as expected.
    created_volume = create_context[SUCCESSFUL_CREATE_KEY]
    assert str(created_volume.spec) == str(expected_spec)
    assert str(created_volume.state) == str(expected_state)


@then("the volume should be created")
def the_volume_should_be_created():
    """the volume should be created."""
    volume = get_volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.uuid == VOLUME_UUID


@then("the volume should not be created")
def the_volume_should_not_be_created():
    """the volume should not be created."""
    raise NotImplementedError
