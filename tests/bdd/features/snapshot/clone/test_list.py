"""Volume Snapshot Clone Listing feature tests."""

import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

from common.deployer import Deployer
from common.apiclient import ApiClient

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_policy import VolumePolicy

VOLUME_UUID = "8d305974-43a3-484b-8e2c-c74afe2f4400"
SNAPSHOT_UUID = "8d305974-43a3-484b-8e2c-c74afe2f4401"
CLONE_UUID = "8d305974-43a3-484b-8e2c-c74afe2f4402"


@pytest.fixture(scope="module")
def disks():
    yield Deployer.create_disks(1)
    Deployer.delete_disks(1)


@pytest.fixture(scope="module")
def deployer_cluster(disks):
    Deployer.start(1, cache_period="100ms", reconcile_period="150ms")
    ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0), "pool", CreatePoolBody([disks[0]])
    )
    yield
    Deployer.stop()


@scenario("list.feature", "List a snapshot clone")
def test_list_a_snapshot_clone():
    """List a snapshot clone."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given("a valid snapshot of a single replica volume")
def a_valid_snapshot_of_a_single_replica_volume():
    """a valid snapshot of a single replica volume."""
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=20 * 1024 * 1024,
            thin=False,
        ),
    )
    ApiClient.snapshots_api().put_volume_snapshot(VOLUME_UUID, SNAPSHOT_UUID)
    yield
    ApiClient.snapshots_api().del_snapshot(SNAPSHOT_UUID)
    ApiClient.volumes_api().del_volume(VOLUME_UUID)


@when(
    "we create a new volume with the snapshot as its source",
    target_fixture="volume_clone",
)
def we_create_a_new_volume_with_the_snapshot_as_its_source():
    """we create a new volume with the snapshot as its source."""
    body = CreateVolumeBody(
        VolumePolicy(True),
        replicas=1,
        size=20 * 1024 * 1024,
        thin=True,
    )
    volume = ApiClient.volumes_api().put_snapshot_volume(
        SNAPSHOT_UUID, CLONE_UUID, body
    )
    yield volume
    ApiClient.volumes_api().del_volume(CLONE_UUID)


@then("the new volume's source should be the snapshot")
def the_new_volumes_source_should_be_the_snapshot(volume_clone):
    """the new volume's source should be the snapshot."""
    source = volume_clone.spec.content_source["snapshot"]
    assert source.snapshot == SNAPSHOT_UUID
    assert source.volume == VOLUME_UUID


@then("we should be able to list the new volume")
def we_should_be_able_to_list_the_new_volume():
    """we should be able to list the new volume."""
    volumes = ApiClient.volumes_api().get_volumes()
    assert len(volumes.entries) == 2
    volume = ApiClient.volumes_api().get_volume(CLONE_UUID)
    assert volume.spec.uuid == CLONE_UUID
