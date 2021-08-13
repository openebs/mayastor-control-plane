"""Volume deletion feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import requests
import common

from openapi.openapi_client.model.create_pool_body import CreatePoolBody
from openapi.openapi_client.model.create_volume_body import CreateVolumeBody
from openapi.openapi_client.model.protocol import Protocol

POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
NODE_NAME = "mayastor"
VOLUME_CTX_KEY = "volume"


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool and volume are created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    common.deployer_start()
    common.get_pools_api().put_node_pool(
        NODE_NAME, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=500"])
    )
    policy = {"self_heal": False, "topology": None}
    topology = {"explicit": None, "labelled": None}
    common.get_volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(policy, 1, 10485761, topology)
    )
    yield
    common.deployer_stop()


@pytest.fixture(scope="function")
def volume_ctx():
    return {}


@scenario(
    "features/volume/delete.feature", "delete a volume that is not shared/published"
)
def test_delete_a_volume_that_is_not_sharedpublished():
    """delete a volume that is not shared/published."""


@scenario("features/volume/delete.feature", "delete a volume that is shared/published")
def test_delete_a_volume_that_is_sharedpublished():
    """delete a volume that is shared/published."""


@given("a volume that is not shared/published")
def a_volume_that_is_not_sharedpublished(volume_ctx):
    """a volume that is not shared/published."""
    volume = volume_ctx[VOLUME_CTX_KEY]
    assert volume is not None
    assert str(volume.spec.protocol) == str(Protocol("none"))


@given("a volume that is shared/published")
def a_volume_that_is_sharedpublished():
    """a volume that is shared/published."""
    volume = common.get_volumes_api().put_volume_target(
        VOLUME_UUID, NODE_NAME, Protocol("nvmf")
    )
    assert str(volume.spec.protocol) == str(Protocol("nvmf"))


@given("an existing volume")
def an_existing_volume(volume_ctx):
    """an existing volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.uuid == VOLUME_UUID
    volume_ctx[VOLUME_CTX_KEY] = volume


@when("a user attempts to delete a volume")
def a_user_attempts_to_delete_a_volume():
    """a user attempts to delete a volume."""
    common.get_volumes_api().del_volume(VOLUME_UUID)


@then("the volume should be deleted")
def the_volume_should_be_deleted():
    """the volume should be deleted."""
    try:
        common.get_volumes_api().get_volume(VOLUME_UUID)
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["not_found"]
