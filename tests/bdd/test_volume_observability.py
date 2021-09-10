"""Volume observability feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import common

from openapi.openapi_client.model.create_pool_body import CreatePoolBody
from openapi.openapi_client.model.create_volume_body import CreateVolumeBody
from openapi.openapi_client.model.volume_spec import VolumeSpec
from openapi.openapi_client.model.volume_state import VolumeState
from openapi.openapi_client.model.volume_status import VolumeStatus
from openapi.openapi_client.model.protocol import Protocol
from openapi.openapi_client.model.spec_status import SpecStatus

POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
NODE_NAME = "mayastor-1"
VOLUME_CTX_KEY = "volume"
VOLUME_SIZE = 10485761


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool and volume are created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    common.deployer_start(1)
    common.get_pools_api().put_node_pool(
        NODE_NAME, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=50"])
    )
    policy = {"self_heal": False, "topology": None}
    topology = {"explicit": None, "labelled": None}
    common.get_volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(policy, 1, VOLUME_SIZE, topology)
    )
    yield
    common.deployer_stop()


# Fixture used to pass the volume context between test steps.
@pytest.fixture(scope="function")
def volume_ctx():
    return {}


@scenario("features/volume/observability.feature", "requesting volume information")
def test_requesting_volume_information():
    """requesting volume information."""


@given("an existing volume")
def an_existing_volume():
    """an existing volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.uuid == VOLUME_UUID


@when("a user issues a GET request for a volume")
def a_user_issues_a_get_request_for_a_volume(volume_ctx):
    """a user issues a GET request for a volume."""
    volume_ctx[VOLUME_CTX_KEY] = common.get_volumes_api().get_volume(VOLUME_UUID)


@then("a volume object representing the volume should be returned")
def a_volume_object_representing_the_volume_should_be_returned(volume_ctx):
    """a volume object representing the volume should be returned."""
    cfg = common.get_cfg()
    expected_spec = VolumeSpec(
        [],
        1,
        Protocol("none"),
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        _configuration=cfg,
    )
    expected_state = VolumeState(
        Protocol("none"),
        VOLUME_SIZE,
        VolumeStatus("Online"),
        VOLUME_UUID,
        _configuration=cfg,
    )

    volume = volume_ctx[VOLUME_CTX_KEY]
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state) == str(expected_state)
