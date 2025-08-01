"""Volume unpublishing feature tests."""

import pytest
import requests
from common.apiclient import ApiClient
from common.deployer import Deployer
from openapi.models.create_pool_body import CreatePoolBody
from openapi.models.create_volume_body import CreateVolumeBody
from openapi.models.publish_volume_body import PublishVolumeBody
from openapi.models.volume_policy import VolumePolicy
from openapi.models.volume_share_protocol import VolumeShareProtocol
from pytest_bdd import (
    given,
    scenario,
    then,
)

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
        NODE_NAME, POOL_UUID, CreatePoolBody(disks=["malloc:///disk?size_mb=50"])
    )
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            policy=VolumePolicy(self_heal=False),
            replicas=1,
            size=VOLUME_SIZE,
            thin=False,
            encrypted=False,
        ),
    )
    yield
    Deployer.stop()


@scenario("feature.feature", "unpublish a published volume")
def test_unpublish_a_published_volume():
    """unpublish a published volume."""


@scenario("feature.feature", "unpublish an already unpublished volume")
def test_unpublish_an_already_unpublished_volume():
    """unpublish an already unpublished volume."""


@given("a published volume")
def a_published_volume():
    """a published volume."""
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            publish_context={},
            protocol=VolumeShareProtocol("nvmf"),
            node=NODE_NAME,
            frontend_node="",
        ),
    )
    assert volume.spec.target
    assert volume.spec.target.protocol == "nvmf"


@given("an existing volume")
def an_existing_volume():
    """an existing volume."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.uuid == VOLUME_UUID


@given("an unpublished volume")
def an_unpublished_volume():
    """an unpublished volume."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.target is None


@then("unpublishing the volume should return an already unpublished error")
def unpublishing_the_volume_should_return_an_already_unpublished_error():
    """unpublishing the volume should return an already unpublished error."""
    try:
        ApiClient.volumes_api().del_volume_target(VOLUME_UUID)
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["precondition_failed"]
        assert "NotPublished" in exception_info["body"]


@then("unpublishing the volume should succeed")
def unpublishing_the_volume_should_succeed():
    """unpublishing the volume should succeed."""
    volume = ApiClient.volumes_api().del_volume_target(VOLUME_UUID)
    assert volume.spec.target is None
