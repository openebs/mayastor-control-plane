"""Volume publishing feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import common
import requests

from openapi.openapi_client.model.create_pool_body import CreatePoolBody
from openapi.openapi_client.model.create_volume_body import CreateVolumeBody
from openapi.openapi_client.model.protocol import Protocol

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


@scenario("features/volume/publish.feature", "publish an unpublished volume")
def test_publish_an_unpublished_volume():
    """publish an unpublished volume."""


@scenario(
    "features/volume/publish.feature",
    "share/publish an already shared/published volume",
)
def test_sharepublish_an_already_sharedpublished_volume():
    """share/publish an already shared/published volume."""


@given("a published volume")
def a_published_volume():
    """a published volume."""
    volume = common.get_volumes_api().put_volume_target(
        VOLUME_UUID, NODE_NAME, Protocol("nvmf")
    )
    assert str(volume.spec.protocol) == str(Protocol("nvmf"))


@given("an existing volume")
def an_existing_volume():
    """an existing volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.uuid == VOLUME_UUID


@given("an unpublished volume")
def an_unpublished_volume():
    """an unpublished volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    assert str(volume.spec.protocol) == str(Protocol("none"))


@then("publishing the volume should return an already published error")
def publishing_the_volume_should_return_an_already_published_error():
    """publishing the volume should return an already published error."""
    try:
        common.get_volumes_api().put_volume_target(
            VOLUME_UUID, NODE_NAME, Protocol("nvmf")
        )
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["precondition_failed"]
        assert "AlreadyPublished" in exception_info["body"]


@then(
    "publishing the volume should succeed with a returned volume object containing the share URI"
)
def publishing_the_volume_should_succeed_with_a_returned_volume_object_containing_the_share_uri():
    """publishing the volume should succeed with a returned volume object containing the share URI."""
    volume = common.get_volumes_api().put_volume_target(
        VOLUME_UUID, NODE_NAME, Protocol("nvmf")
    )
    assert str(volume.spec.protocol) == str(Protocol("nvmf"))
    assert len(volume.state.children) > 0
    assert "nvmf://" in volume.state.children[0].device_uri
