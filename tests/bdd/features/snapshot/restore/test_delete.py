"""Deleting Restored Snapshot Volume feature tests."""
import pytest
from pytest_bdd import given, scenario, then, when

import uuid

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.operations import Volume, Snapshot, wait_node_online

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_policy import VolumePolicy

POOL = "pool"
NODE = Deployer.node_name(0)


@pytest.fixture(scope="module")
def volume_uuids():
    return list(map(lambda x: str(uuid.uuid4()), range(10)))


@pytest.fixture(scope="module")
def snapshot_uuids():
    return list(map(lambda x: str(uuid.uuid4()), range(10)))


@pytest.fixture(scope="module")
def disks():
    yield Deployer.create_disks(1, size=300 * 1024 * 1024)
    Deployer.cleanup_disks(1)


@pytest.fixture(scope="module")
def deployer_cluster(disks):
    Deployer.start(1, cache_period="100ms", reconcile_period="150ms", fio_spdk=True)
    ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0), POOL, CreatePoolBody([disks[0]])
    )
    yield
    Deployer.stop()


@scenario("delete.feature", "Deleting in creation order")
def test_deleting_in_creation_order():
    """Deleting in creation order."""


@scenario("delete.feature", "Deleting in reverse creation order")
def test_deleting_in_reverse_creation_order():
    """Deleting in reverse creation order."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given("a single replica volume from the snapshot", target_fixture="restored_volume")
def a_single_replica_volume_from_the_snapshot(
    original_snapshot, volume_uuids, snapshot_uuids
):
    """a single replica volume from the snapshot."""
    volume = ApiClient.volumes_api().put_snapshot_volume(
        original_snapshot.definition.spec.uuid,
        volume_uuids[1],
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=original_snapshot.definition.metadata.size,
            thin=True,
        ),
    )
    # allocate 1 cluster worth of data
    volume = Volume.fio(volume, offset="7M", size="4096B")
    yield volume
    Volume.cleanup(volume)


@given("a snapshot", target_fixture="original_snapshot")
def a_snapshot(original_volume, snapshot_uuids):
    """a snapshot."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        original_volume.spec.uuid, snapshot_uuids[0]
    )
    # Why 3M you ask? Well because we know that we have 5M of unused partition at the start of the replica
    # so if we write before, we might span 2 clusters instead of 1, which could be confusing
    volume = Volume.fio(original_volume, offset="3M", size="4096B")
    snapshot = Snapshot.update(snapshot, original_volume, cached=False)
    yield snapshot
    Snapshot.cleanup(snapshot)


@given("a single replica volume", target_fixture="original_volume")
def a_single_replica_volume(volume_uuids):
    """a single replica volume."""
    volume = ApiClient.volumes_api().put_volume(
        volume_uuids[0],
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=88 * 1024 * 1024,
            thin=True,
        ),
    )
    yield volume
    Volume.cleanup(volume)


@when("we delete the original volume")
def we_delete_the_original_volume(original_volume):
    """we delete the original volume."""
    ApiClient.volumes_api().del_volume(original_volume.spec.uuid)


@when("we delete the restored volume")
def we_delete_the_restored_volume(restored_volume):
    """we delete the restored volume."""
    ApiClient.volumes_api().del_volume(restored_volume.spec.uuid)


@then("the pool space usage should be zero")
def the_pool_space_usage_should_be_zero():
    """the pool space usage should be zero."""
    pool = ApiClient.pools_api().get_pool(POOL)
    assert pool.state.used == 0


@then("the pool space usage should reflect the original volume")
def the_pool_space_usage_should_reflect_the_original_volume(original_volume):
    """the pool space usage should reflect the original volume."""
    pool = ApiClient.pools_api().get_pool(POOL)
    # Bug, dataplane caches allocated, requires a restart until fixed
    Docker.restart_container(NODE)
    wait_node_online(NODE)
    volume = Volume.update(original_volume, cached=False)
    assert pool.state.used == volume.state.usage.allocated


@then("the pool space usage should reflect the snapshot and restored volume")
def the_pool_space_usage_should_reflect_the_snapshot_and_restored_volume(
    restored_volume, original_snapshot
):
    """the pool space usage should reflect the snapshot and restored volume."""
    pool = ApiClient.pools_api().get_pool(POOL)
    # todo: use retry pattern instead
    restored_volume = Volume.update(restored_volume, cached=False)

    restored_usage = 4 * 1024 * 1024
    snapshot_usage = 8 * 1024 * 1024
    assert restored_volume.state.usage.allocated_replica == restored_usage
    assert original_snapshot.state.allocated_size == snapshot_usage
    assert pool.state.used == snapshot_usage + restored_usage


@then("the snapshot and the restored volume should still exist")
def the_snapshot_and_the_restored_volume_should_still_exist(
    original_volume, original_snapshot, restored_volume
):
    """the snapshot and the restored volume should still exist."""
    ApiClient.snapshots_api().get_volume_snapshot(
        original_volume.spec.uuid, original_snapshot.definition.spec.uuid
    )
    ApiClient.volumes_api().get_volume(restored_volume.spec.uuid)


@then("the snapshot should still exist")
def the_snapshot_should_still_exist(original_volume, original_snapshot):
    """the snapshot should still exist."""
    ApiClient.snapshots_api().get_volume_snapshot(
        original_volume.spec.uuid, original_snapshot.definition.spec.uuid
    )


@then("we delete the snapshot")
def we_delete_the_snapshot(original_snapshot):
    """we delete the snapshot."""
    ApiClient.snapshots_api().del_snapshot(original_snapshot.definition.spec.uuid)
