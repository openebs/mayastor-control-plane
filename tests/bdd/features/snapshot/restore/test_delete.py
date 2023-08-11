"""Deleting Restored Snapshot Volume feature tests."""
import pytest
from pytest_bdd import given, scenario, then, when

import uuid

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.operations import Volume, Snapshot, wait_node_online, Cluster

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


@scenario("delete.feature", "Delete a chain of restored volumes")
def test_delete_a_chain_of_restored_volumes():
    """Delete a chain of restored volumes."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given("a single replica volume from the snapshot", target_fixture="restored_volume")
def a_single_replica_volume_from_the_snapshot(original_snapshot, volume_uuids):
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
    yield volume
    Volume.cleanup(volume)


@given("a snapshot", target_fixture="original_snapshot")
def a_snapshot(original_volume, snapshot_uuids):
    """a snapshot."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        original_volume.spec.uuid, snapshot_uuids[0]
    )
    yield snapshot
    Snapshot.cleanup(snapshot)


@given("a single replica volume with 8MiB allocated", target_fixture="original_volume")
def a_single_replica_volume_with_8mib_allocated(volume_uuids):
    """a single replica volume with 8MiB allocated."""
    volume = ApiClient.volumes_api().put_volume(
        volume_uuids[0],
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=88 * 1024 * 1024,
            thin=True,
        ),
    )
    # 8MiB get allocated on volume creation
    assert volume.state.usage.allocated == 8 * 1024 * 1024
    yield volume
    Volume.cleanup(volume)


@given("we allocate 4MiB of the original volume")
def we_allocate_4mib_of_the_original_volume_1(original_volume):
    """we allocate 4MiB of the original volume."""
    # Why 3M you ask? Well because we know that we have 5M of unused partition at the start of the replica
    # so if we write before, we might span 2 clusters instead of 1, which could be confusing
    volume = Volume.fio(original_volume, offset="3M", size="4096B")
    Volume.update(volume, cached=False)


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
@when("we delete the snapshot")
def we_delete_the_snapshot(original_snapshot):
    """we delete the snapshot."""
    ApiClient.snapshots_api().del_snapshot(original_snapshot.definition.spec.uuid)


@given("the restored volume 1 allocated_replica size should be 4MiB")
def the_restored_volume_1_allocated_replica_size_should_be_4mib(restored_volume):
    """the restored volume 1 allocated_replica size should be 4MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated_replica == 4 * 1024 * 1024


@given("the restored volume 1 allocated_snapshot size should be zero")
def the_restored_volume_1_allocated_snapshot_size_should_be_zero(restored_volume):
    """the restored volume 1 allocated_snapshot size should be zero."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated_snapshots == 0


@then("the restored volume 1 allocated_snapshot size should be 8MiB")
def the_restored_volume_1_allocated_snapshot_size_should_be_8mib(restored_volume):
    """the restored volume 1 allocated_snapshot size should be 8MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated_snapshots == 8 * 1024 * 1024


@given("we allocate 4MiB of the restored volume 1")
@then("we allocate 4MiB of the restored volume 1")
def we_allocate_4mib_of_the_restored_volume_1(restored_volume):
    """we allocate 4MiB of the restored volume 1."""
    # Why 3M you ask? Well because we know that we have 5M of unused partition at the start of the replica
    # so if we write before, we might span 2 clusters instead of 1, which could be confusing
    volume = Volume.fio(restored_volume, offset="3M", size="4096B")
    Volume.update(volume, cached=False)


@given("the restored volume 1 allocation size should be 4MiB")
def the_restored_volume_1_allocation_size_should_be_4mib(restored_volume):
    """the restored volume 1 allocation size should be 4MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated == 4 * 1024 * 1024


@given("the restored volume 1 allocated_replica size should be zero")
@then("the restored volume 1 allocated_replica size should be zero")
def the_restored_volume_1_allocated_replica_size_should_be_zero(restored_volume):
    """the restored volume 1 allocated_replica size should be zero."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated_replica == 0


@then("the restored volume 1 allocated_snapshot size should be 4MiB")
def the_restored_volume_1_allocated_snapshot_size_should_be_4mib(restored_volume):
    """the restored volume 1 allocated_snapshot size should be 4MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated_snapshots == 4 * 1024 * 1024


@given("the restored volume 1 allocation size should be 12MiB")
def the_restored_volume_1_allocation_size_should_be_12mib(restored_volume):
    """the restored volume 1 allocation size should be 12MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated == 12 * 1024 * 1024


@then("the restored volume 1 snapshot 1 allocation size should be 4MiB")
def the_restored_volume_1_snapshot_1_allocation_size_should_be_4mib(
    restored_1_snapshot_1,
):
    """the restored volume 1 snapshot 1 allocation size should be 4MiB."""
    snapshot = Snapshot.update(restored_1_snapshot_1)
    assert snapshot.state.allocated_size == 4 * 1024 * 1024


@then("the restored volume 1 snapshot 1 allocation size should be 12MiB")
@when("the restored volume 1 snapshot 1 allocation size should be 12MiB")
def the_restored_volume_1_snapshot_1_allocation_size_should_be_12mib(
    restored_1_snapshot_1,
):
    """the restored volume 1 snapshot 1 allocation size should be 12MiB."""
    Docker.restart_container(NODE)
    wait_node_online(NODE)
    Cluster.wait_cache_update()
    snapshot = Snapshot.update(restored_1_snapshot_1)
    assert snapshot.state.allocated_size == 12 * 1024 * 1024


@then("the restored volume 1 snapshot 2 allocation size should be 4MiB")
@when("the restored volume 1 snapshot 2 allocation size should be 4MiB")
def the_restored_volume_1_snapshot_2_allocation_size_should_be_4mib(
    restored_1_snapshot_2,
):
    """the restored volume 1 snapshot 2 allocation size should be 4MiB."""
    snapshot = Snapshot.update(restored_1_snapshot_2)
    assert snapshot.state.allocated_size == 4 * 1024 * 1024


@then("the restored volume 1 snapshot 2 total allocation size should be 8MiB")
@when("the restored volume 1 snapshot 2 total allocation size should be 8MiB")
def the_restored_volume_1_snapshot_2_total_allocation_size_should_be_8mib(
    restored_1_snapshot_2,
):
    """the restored volume 1 snapshot 2 total allocation size should be 8MiB."""
    snapshot = Snapshot.update(restored_1_snapshot_2)
    assert snapshot.state.allocated_size == 4 * 1024 * 1024
    # todo: missing this on snapshot state!
    assert snapshot.definition.metadata.total_allocated_size == 8 * 1024 * 1024


@given("we create restored volume 1 snapshot 1", target_fixture="restored_1_snapshot_1")
def we_create_restored_volume_1_snapshot_1(snapshot_uuids, restored_volume):
    """we create restored volume 1 snapshot 1."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        restored_volume.spec.uuid, snapshot_uuids[1]
    )
    yield snapshot
    Snapshot.cleanup(snapshot)


@then("we create restored volume 1 snapshot 2", target_fixture="restored_1_snapshot_2")
def we_create_restored_volume_1_snapshot_2(snapshot_uuids, restored_volume):
    """we create restored volume 1 snapshot 2."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        restored_volume.spec.uuid, snapshot_uuids[2]
    )
    yield snapshot
    Snapshot.cleanup(snapshot)


@then(
    "we restore volume 1 snapshot 2 into restored volume 2",
    target_fixture="volume_2_from_snapshot_2",
)
def we_restore_volume_1_snapshot_2_into_restored_volume_2(
    restored_1_snapshot_2, volume_uuids
):
    """we restore volume 1 snapshot 2 into restored volume 2."""
    volume = ApiClient.volumes_api().put_snapshot_volume(
        restored_1_snapshot_2.definition.spec.uuid,
        volume_uuids[2],
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=restored_1_snapshot_2.definition.metadata.size,
            thin=True,
        ),
    )
    yield volume


@then("we allocate 4MiB of the restored volume 2")
def we_allocate_4mib_of_the_restored_volume_2(volume_2_from_snapshot_2):
    """we allocate 4MiB of the restored volume 2."""
    volume = Volume.fio(volume_2_from_snapshot_2, offset="3M", size="4096B")
    Volume.update(volume, cached=False)


@then("the restored volume 2 allocation size should be 4MiB")
def the_restored_volume_2_allocation_size_should_be_4mib(volume_2_from_snapshot_2):
    """the restored volume 2 allocation size should be 4MiB."""
    volume = Volume.update(volume_2_from_snapshot_2, cached=False)
    assert volume.state.usage.allocated_replica == 4 * 1024 * 1024
    assert volume.state.usage.allocated_snapshots == 0
    assert volume.state.usage.allocated == 4 * 1024 * 1024


@then(
    "the pool space usage should reflect the original snapshot, restored volume 1, snapshot 1, 2, and restored volume 2 (20MiB)"
)
def the_pool_space_usage_should_reflect_the_original_snapshot_restored_volume_1_snapshot_1_2_and_restored_volume_2_20mib(
    restored_volume,
    volume_2_from_snapshot_2,
    original_snapshot,
    restored_1_snapshot_1,
    restored_1_snapshot_2,
):
    """the pool space usage should reflect the original snapshot, restored volume 1, snapshot 1, 2, and restored volume 2 (20MiB)."""
    pool = ApiClient.pools_api().get_pool(POOL)
    original_snapshot = Snapshot.update(original_snapshot)
    restored_volume = Volume.update(restored_volume)
    restored_1_snapshot_1 = Snapshot.update(restored_1_snapshot_1)
    restored_1_snapshot_2 = Snapshot.update(restored_1_snapshot_2)
    volume_2_from_snapshot_2 = Volume.update(volume_2_from_snapshot_2)

    assert (
        pool.state.used
        == original_snapshot.state.allocated_size
        + restored_volume.state.usage.allocated_replica
        + restored_1_snapshot_1.state.allocated_size
        + restored_1_snapshot_2.state.allocated_size
        + volume_2_from_snapshot_2.state.usage.allocated_replica
    )
    assert pool.state.used == 20 * 1024 * 1024


@then(
    "the pool space usage should reflect the restored volume 1, snapshot 1, 2, restored volume 2, and deleted snapshot (20MiB)"
)
def the_pool_space_usage_should_reflect_the_restored_volume_1_snapshot_1_2_restored_volume_2_and_deleted_snapshot_20mib(
    restored_volume,
    volume_2_from_snapshot_2,
    original_snapshot,
    restored_1_snapshot_1,
    restored_1_snapshot_2,
):
    """the pool space usage should reflect the restored volume 1, snapshot 1, 2, restored volume 2, and deleted snapshot (20MiB)."""
    pool = ApiClient.pools_api().get_pool(POOL)
    restored_volume = Volume.update(restored_volume)
    restored_1_snapshot_1 = Snapshot.update(restored_1_snapshot_1)
    restored_1_snapshot_2 = Snapshot.update(restored_1_snapshot_2)
    volume_2_from_snapshot_2 = Volume.update(volume_2_from_snapshot_2)

    assert (
        pool.state.used
        == original_snapshot.state.allocated_size
        + restored_volume.state.usage.allocated_replica
        + restored_1_snapshot_1.state.allocated_size
        + restored_1_snapshot_2.state.allocated_size
        + volume_2_from_snapshot_2.state.usage.allocated_replica
    )
    assert pool.state.used == 20 * 1024 * 1024


@then(
    "the pool space usage should reflect the snapshot 1, 2, restored volume 2, and deleted snapshot and deleted restored volume 1 (20MiB)"
)
def the_pool_space_usage_should_reflect_the_snapshot_1_2_restored_volume_2_and_deleted_snapshot_and_deleted_restored_volume_1_20mib(
    volume_2_from_snapshot_2,
    restored_1_snapshot_1,
    restored_1_snapshot_2,
):
    """the pool space usage should reflect the snapshot 1, 2, restored volume 2, and deleted snapshot and deleted restored volume 1 (20MiB)."""
    pool = ApiClient.pools_api().get_pool(POOL)
    restored_1_snapshot_1 = Snapshot.update(restored_1_snapshot_1)
    restored_1_snapshot_2 = Snapshot.update(restored_1_snapshot_2)
    volume_2_from_snapshot_2 = Volume.update(volume_2_from_snapshot_2)

    assert (
        pool.state.used
        == restored_1_snapshot_1.state.allocated_size
        + restored_1_snapshot_2.state.allocated_size
        + volume_2_from_snapshot_2.state.usage.allocated_replica
    )
    assert pool.state.used == 20 * 1024 * 1024


@then(
    "the pool space usage should reflect the snapshot 2, restored volume 2, and deleted snapshot and deleted restored volume 1 (16MiB)"
)
def the_pool_space_usage_should_reflect_the_snapshot_2_restored_volume_2_and_deleted_snapshot_and_deleted_restored_volume_1_16mib(
    volume_2_from_snapshot_2,
    restored_1_snapshot_2,
):
    """the pool space usage should reflect the snapshot 2, restored volume 2, and deleted snapshot and deleted restored volume 1 (16MiB)."""
    # Bug, dataplane caches allocated, requires a restart until fixed
    Docker.restart_container(NODE)
    wait_node_online(NODE)
    Cluster.wait_cache_update()

    pool = ApiClient.pools_api().get_pool(POOL)
    restored_1_snapshot_2 = Snapshot.update(restored_1_snapshot_2)
    volume_2_from_snapshot_2 = Volume.update(volume_2_from_snapshot_2)

    assert (
        pool.state.used
        == restored_1_snapshot_2.state.allocated_size
        + volume_2_from_snapshot_2.state.usage.allocated_replica
    )
    assert pool.state.used == 16 * 1024 * 1024


@then(
    "the pool space usage should reflect the restored volume 2, and deleted snapshot 1,2 and deleted restored volume 1 (16MiB)"
)
def the_pool_space_usage_should_reflect_the_restored_volume_2_and_deleted_snapshot_12_and_deleted_restored_volume_1_16mib(
    volume_2_from_snapshot_2,
):
    """the pool space usage should reflect the restored volume 2, and deleted snapshot 1,2 and deleted restored volume 1 (16MiB)."""
    pool = ApiClient.pools_api().get_pool(POOL)
    volume_2_from_snapshot_2 = Volume.update(volume_2_from_snapshot_2)

    assert (
        pool.state.used
        == volume_2_from_snapshot_2.state.usage.allocated
        + volume_2_from_snapshot_2.state.usage.allocated_all_snapshots
    )
    assert pool.state.used == 16 * 1024 * 1024


@when("we delete the restored volume 1 snapshot 1")
def we_delete_the_restored_volume_1_snapshot_1(restored_1_snapshot_1):
    """we delete the restored volume 1 snapshot 1.""" ""
    ApiClient.snapshots_api().del_snapshot(restored_1_snapshot_1.definition.spec.uuid)


@when("we delete the restored volume 1 snapshot 2")
def we_delete_the_restored_volume_1_snapshot_2(restored_1_snapshot_2):
    """we delete the restored volume 1 snapshot 2."""
    ApiClient.snapshots_api().del_snapshot(restored_1_snapshot_2.definition.spec.uuid)


@when("we delete the restored volume 2")
def we_delete_the_restored_volume_2(volume_2_from_snapshot_2):
    """we delete the restored volume 2."""
    ApiClient.volumes_api().del_volume(volume_2_from_snapshot_2.spec.uuid)
