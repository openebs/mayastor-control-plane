"""Persistent Nexus Info feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.etcd import Etcd

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_share_protocol import VolumeShareProtocol
from openapi.model.volume_policy import VolumePolicy
from openapi.exceptions import NotFoundException
from openapi.model.publish_volume_body import PublishVolumeBody

POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
NODE_NAME = "io-engine-1"
VOLUME_CTX_KEY = "volume"
VOLUME_CTX_KEY_OLD = "volume-old"
VOLUME_SIZE = 10485761
ETCD_CLIENT = Etcd()


# Fixture used to pass the volume context between test steps.
@pytest.fixture(scope="function")
def volume_ctx():
    return {}


@pytest.fixture(autouse=True, scope="module")
def init():
    Deployer.start(1)
    ApiClient.pools_api().put_node_pool(
        NODE_NAME, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=50"])
    )
    yield
    Deployer.stop()


@scenario("feature.feature", "Deleting a published volume")
def test_deleting_a_published_volume():
    """Deleting a published volume."""


@scenario("feature.feature", "publishing a volume")
def test_publishing_a_volume():
    """publishing a volume."""


@scenario("feature.feature", "re-publishing a volume")
def test_republishing_a_volume():
    """re-publishing a volume."""


@scenario("feature.feature", "unpublishing a volume")
def test_unpublishing_a_volume():
    """unpublishing a volume."""


@given("a volume that has been published and unpublished")
def a_volume_that_has_been_published_and_unpublished(volume_ctx):
    """a volume that has been published and unpublished."""
    # This volume will become the old entry after re-publishing the volume
    volume_ctx[VOLUME_CTX_KEY_OLD] = publish_volume()
    unpublish_volume()


@given("a volume that is not published")
def a_volume_that_is_not_published():
    """a volume that is not published."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert not hasattr(volume.spec, "target")


@given("a volume that is published")
def a_volume_that_is_published(volume_ctx):
    """a volume that is published."""
    volume_ctx[VOLUME_CTX_KEY] = publish_volume()


@given("an existing volume")
def an_existing_volume(an_existing_volume):
    """an existing volume."""


@when("the volume is deleted")
def the_volume_is_deleted():
    """the volume is deleted."""
    ApiClient.volumes_api().del_volume(VOLUME_UUID)


@when("the volume is published")
def the_volume_is_published(volume_ctx):
    """the volume is published."""
    volume_ctx[VOLUME_CTX_KEY] = publish_volume()


@when("the volume is re-published")
def the_volume_is_republished(volume_ctx):
    """the volume is re-published."""
    volume_ctx[VOLUME_CTX_KEY] = publish_volume()


@when("the volume is unpublished")
def the_volume_is_unpublished():
    """the volume is unpublished."""
    unpublish_volume()


@then("the nexus info structure should be present in the persistent store")
def the_nexus_info_structure_should_be_present_in_the_persistent_store(volume_ctx):
    """the nexus info structure should be present in the persistent store."""
    volume = volume_ctx[VOLUME_CTX_KEY]
    nexus_uuid = volume.state.target["uuid"]
    assert ETCD_CLIENT.get_nexus_info(VOLUME_UUID, nexus_uuid) is not None


@then("the nexus info structure should not be present in the persistent store")
def the_nexus_info_structure_should_not_be_present_in_the_persistent_store(volume_ctx):
    """the nexus info structure should not be present in the persistent store."""
    volume = volume_ctx[VOLUME_CTX_KEY]
    nexus_uuid = volume.state.target["uuid"]
    assert ETCD_CLIENT.get_nexus_info(VOLUME_UUID, nexus_uuid) is None


@then("the old nexus info structure should not be present in the persistent store")
def the_old_nexus_info_structure_should_not_be_present_in_the_persistent_store(
    volume_ctx,
):
    """the old nexus info structure should not be present in the persistent store."""
    old_volume = volume_ctx[VOLUME_CTX_KEY_OLD]
    nexus_uuid = old_volume.state.target["uuid"]
    assert ETCD_CLIENT.get_nexus_info(VOLUME_UUID, nexus_uuid) is None


@pytest.fixture
def an_existing_volume():
    volume = ApiClient.volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False)
    )
    yield
    try:
        ApiClient.volumes_api().del_volume(volume.spec.uuid)
    except NotFoundException:
        # If the volume is not found it was already deleted, so carry on.
        pass


# Publish the volume
def publish_volume():
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, VolumeShareProtocol("nvmf"), node=NODE_NAME, frontend_node=""
        ),
    )
    assert hasattr(volume.state, "target")
    return volume


# Unpublish the volume
def unpublish_volume():
    volume = ApiClient.volumes_api().del_volume_target(VOLUME_UUID)
    assert not hasattr(volume.spec, "target")
