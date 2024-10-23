"""Volume publishing feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
)

import pytest
import requests

from common.deployer import Deployer
from common.apiclient import ApiClient

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_share_protocol import VolumeShareProtocol
from openapi.model.volume_policy import VolumePolicy
from openapi.model.publish_volume_body import PublishVolumeBody

POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
NODE_NAME = "io-engine-1"
VOLUME_CTX_KEY = "volume"
VOLUME_SIZE = 10485761


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool and volume are created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True, scope="module")
def init():
    Deployer.start(1)
    ApiClient.pools_api().put_node_pool(
        NODE_NAME, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=50"])
    )
    yield
    Deployer.stop()


@scenario("feature.feature", "publish an unpublished volume")
def test_publish_an_unpublished_volume():
    """publish an unpublished volume."""


@scenario(
    "feature.feature",
    "share/publish an already shared/published volume",
)
def test_sharepublish_an_already_sharedpublished_volume():
    """share/publish an already shared/published volume."""


@given("a published volume")
def a_published_volume():
    """a published volume."""
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, VolumeShareProtocol("nvmf"), node=NODE_NAME, frontend_node=""
        ),
    )
    assert hasattr(volume.spec, "target")
    assert str(volume.spec.target.protocol) == str(VolumeShareProtocol("nvmf"))


@given("an existing volume")
def an_existing_volume(an_existing_volume):
    """an existing volume."""


@given("an unpublished volume")
def an_unpublished_volume():
    """an unpublished volume."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert not hasattr(volume.spec, "target")


@then("publishing the volume should return an already published error")
def publishing_the_volume_should_return_an_already_published_error():
    """publishing the volume should return an already published error."""
    try:
        ApiClient.volumes_api().put_volume_target(
            VOLUME_UUID,
            publish_volume_body=PublishVolumeBody(
                {}, VolumeShareProtocol("nvmf"), node=NODE_NAME, frontend_node=""
            ),
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
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, VolumeShareProtocol("nvmf"), node=NODE_NAME, frontend_node=""
        ),
    )
    assert hasattr(volume.spec, "target")
    assert str(volume.spec.target.protocol) == str(VolumeShareProtocol("nvmf"))
    assert hasattr(volume.state, "target")
    assert (
        "nvmf://" in volume.state.target["device_uri"]
        or "nvmf+tcp://" in volume.state.target["device_uri"]
        or "nvmf+rdma+tcp://" in volume.state.target["device_uri"]
    )


@pytest.fixture
def an_existing_volume():
    volume = ApiClient.volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False)
    )
    assert volume.spec.uuid == VOLUME_UUID
    yield
    ApiClient.volumes_api().del_volume(volume.spec.uuid)
