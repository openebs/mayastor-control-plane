"""Adjusting the volume replicas feature tests."""

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
from openapi.openapi_client.model.protocol import Protocol
from openapi.openapi_client.exceptions import ApiValueError
from openapi_client.model.volume_policy import VolumePolicy

POOL_1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_2_UUID = "91a60318-bcfe-4e36-92cb-ddc7abf212ea"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
NODE_1_NAME = "mayastor-1"
NODE_2_NAME = "mayastor-2"
VOLUME_CTX_KEY = "volume"
VOLUME_SIZE = 10485761
NUM_MAYASTORS = 2
REPLICA_CONTEXT_KEY = "replica"


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool and volume are created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    common.deployer_start(num_mayastors=NUM_MAYASTORS)
    common.get_pools_api().put_node_pool(
        NODE_1_NAME, POOL_1_UUID, CreatePoolBody(["malloc:///disk?size_mb=50"])
    )
    common.get_pools_api().put_node_pool(
        NODE_2_NAME, POOL_2_UUID, CreatePoolBody(["malloc:///disk?size_mb=50"])
    )
    common.get_volumes_api().put_volume(
        VOLUME_UUID, CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE)
    )

    # Publish volume so that there is a nexus to add a replica to.
    volume = common.get_volumes_api().put_volume_target(
        VOLUME_UUID, NODE_1_NAME, Protocol("nvmf")
    )
    assert str(volume.spec.protocol) == str(Protocol("nvmf"))
    yield
    common.deployer_stop()


# Fixture used to pass the replica context between test steps.
@pytest.fixture(scope="function")
def replica_ctx():
    return {}


@scenario("features/volume/replicas.feature", "setting volume replicas to zero")
def test_setting_volume_replicas_to_zero():
    """setting volume replicas to zero."""


@scenario("features/volume/replicas.feature", "successfully adding a replica")
def test_successfully_adding_a_replica():
    """successfully adding a replica."""


@scenario("features/volume/replicas.feature", "successfully removing a replica")
def test_successfully_removing_a_replica():
    """successfully removing a replica."""


@given("a suitable available pool")
def a_suitable_available_pool():
    """a suitable available pool."""
    pools = common.get_pools_api().get_pools()
    assert len(pools) == 2


@given("an existing volume")
def an_existing_volume():
    """an existing volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.uuid == VOLUME_UUID


@given("the number of volume replicas is greater than one")
def the_number_of_volume_replicas_is_greater_than_one():
    """the number of volume replicas is greater than one."""
    volumes_api = common.get_volumes_api()
    volume = volumes_api.put_volume_replica_count(VOLUME_UUID, 2)
    assert volume.spec.num_replicas > 1


@when("a user attempts to decrease the number of volume replicas")
def a_user_attempts_to_decrease_the_number_of_volume_replicas(replica_ctx):
    """a user attempts to decrease the number of volume replicas."""
    volumes_api = common.get_volumes_api()
    volume = volumes_api.get_volume(VOLUME_UUID)
    num_replicas = volume.spec.num_replicas
    volume = volumes_api.put_volume_replica_count(VOLUME_UUID, num_replicas - 1)
    replica_ctx[REPLICA_CONTEXT_KEY] = volume.spec.num_replicas


@when("a user attempts to increase the number of volume replicas")
def a_user_attempts_to_increase_the_number_of_volume_replicas(replica_ctx):
    """a user attempts to increase the number of volume replicas."""
    volumes_api = common.get_volumes_api()
    volume = volumes_api.get_volume(VOLUME_UUID)
    num_replicas = volume.spec.num_replicas
    volume = volumes_api.put_volume_replica_count(VOLUME_UUID, num_replicas + 1)
    replica_ctx[REPLICA_CONTEXT_KEY] = volume.spec.num_replicas


@then("a replica should be removed from the volume")
def a_replica_should_be_removed_from_the_volume(replica_ctx):
    """a replica should be removed from the volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    assert hasattr(volume.state, "child")
    nexus = volume.state.child
    assert replica_ctx[REPLICA_CONTEXT_KEY] == len(nexus["children"])


@then("an additional replica should be added to the volume")
def an_additional_replica_should_be_added_to_the_volume(replica_ctx):
    """an additional replica should be added to the volume."""
    volume = common.get_volumes_api().get_volume(VOLUME_UUID)
    print(volume.state)
    assert hasattr(volume.state, "child")
    nexus = volume.state.child
    assert replica_ctx[REPLICA_CONTEXT_KEY] == len(nexus["children"])


@then("setting the number of replicas to zero should fail with a suitable error")
def setting_the_number_of_replicas_to_zero_should_fail_with_a_suitable_error():
    """the replica removal should fail with a suitable error."""
    volumes_api = common.get_volumes_api()
    volume = volumes_api.get_volume(VOLUME_UUID)
    assert hasattr(volume.state, "child")
    try:
        volumes_api.put_volume_replica_count(VOLUME_UUID, 0)
    except Exception as e:
        # TODO: Return a proper error rather than asserting for a substring
        assert "ApiValueError" in str(type(e))
