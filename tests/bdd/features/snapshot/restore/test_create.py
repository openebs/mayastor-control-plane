"""Create Volume From Snapshot feature tests."""

import pytest
from pytest_bdd import given, scenario, then, when, parsers

import uuid
import openapi
from common.deployer import Deployer
from common.apiclient import ApiClient

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_policy import VolumePolicy


@pytest.fixture(scope="module")
def volume_uuids():
    return list(map(lambda x: str(uuid.uuid4()), range(10)))


@pytest.fixture(scope="module")
def snapshot_uuids():
    return list(map(lambda x: str(uuid.uuid4()), range(10)))


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


@scenario(
    "create.feature", "Create a new volume as a snapshot restore from a valid snapshot"
)
def test_create_a_new_volume_as_a_snapshot_restore_from_a_valid_snapshot():
    """Create a new volume as a snapshot restore from a valid snapshot."""


@scenario(
    "create.feature",
    "Create multiple new volumes as snapshot restores for a valid snapshot",
)
def test_create_multiple_new_volumes_as_snapshot_restores_for_a_valid_snapshot():
    """Create multiple new volumes as snapshot restores for a valid snapshot."""


@scenario("create.feature", "Create a chain of restored volumes")
def test_create_a_chain_of_restored_volumes():
    """Create a chain of restored volumes."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given("a valid snapshot of a single replica volume")
def a_valid_snapshot_of_a_single_replica_volume(volume_uuids, snapshot_uuids):
    """a valid snapshot of a single replica volume."""
    ApiClient.volumes_api().put_volume(
        volume_uuids[0],
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=20 * 1024 * 1024,
            thin=False,
        ),
    )
    ApiClient.snapshots_api().put_volume_snapshot(volume_uuids[0], snapshot_uuids[0])
    yield
    ApiClient.snapshots_api().del_snapshot(snapshot_uuids[0])
    ApiClient.volumes_api().del_volume(volume_uuids[0])


@when(
    "we attempt to create 4 new volumes with the snapshot as their source",
    target_fixture="snaprestore_attempts",
)
def we_attempt_to_create_4_new_volumes_with_the_snapshot_as_their_source(
    volume_uuids, snapshot_uuids
):
    """we attempt to create 4 new volumes with the snapshot as their source."""
    context = {"ok": [], "failed": []}
    for attempt in range(1, 5):
        body = CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=20 * 1024 * 1024,
            thin=True,
        )
        try:
            volume = ApiClient.volumes_api().put_snapshot_volume(
                snapshot_uuids[0], volume_uuids[attempt], body
            )
            context["ok"].append(volume)
        except openapi.exceptions.ApiException as e:
            context["failed"].append(e)
    yield context
    for volume in context["ok"]:
        ApiClient.volumes_api().del_volume(volume.spec.uuid)


@when(
    "we create a new volume with the snapshot as its source",
    target_fixture="new_volume",
)
def we_create_a_new_volume_with_the_snapshot_as_its_source(
    volume_uuids, snapshot_uuids
):
    """we create a new volume with the snapshot as its source."""
    body = CreateVolumeBody(
        VolumePolicy(True),
        replicas=1,
        size=20 * 1024 * 1024,
        thin=True,
    )
    yield ApiClient.volumes_api().put_snapshot_volume(
        snapshot_uuids[0], volume_uuids[1], body
    )
    ApiClient.volumes_api().del_volume(volume_uuids[1])


@then(parsers.parse("we create a snapshot from volume restore {index:d}"))
@given(parsers.parse("we create a snapshot from volume restore {index:d}"))
def we_create_a_snapshot_from_volume_restore_index(volume_uuids, snapshot_uuids, index):
    """we create a snapshot from volume restore <index>."""
    ApiClient.snapshots_api().put_volume_snapshot(
        volume_uuids[index], snapshot_uuids[index]
    )
    yield
    ApiClient.snapshots_api().del_snapshot(snapshot_uuids[index])


@then("a new replica will be created for the new volume")
def a_new_replica_will_be_created_for_the_new_volume(volume_uuids, new_volume):
    """a new replica will be created for the new volume."""
    # check volume has a replica in the topology
    assert new_volume.spec.uuid == volume_uuids[1]
    assert new_volume.spec.num_replicas == 1
    assert new_volume.spec.status == SpecStatus("Created")


@then("all requests should succeed")
def all_requests_should_succeed(snaprestore_attempts):
    """all requests should succeed."""
    assert len(snaprestore_attempts["failed"]) == 0
    assert len(snaprestore_attempts["ok"]) == 4
    created = list(
        filter(
            lambda v: v.spec.status == SpecStatus("Created"), snaprestore_attempts["ok"]
        )
    )
    assert len(created) == 4, f"Only created these: {created}"


@then("the replica's capacity will be same as the snapshot")
def the_replicas_capacity_will_be_same_as_the_snapshot(new_volume):
    """the replica's capacity will be same as the snapshot."""
    replicas = list(new_volume.state.replica_topology.values())
    assert len(replicas) == 1
    assert replicas[0].usage.capacity == new_volume.spec.size
    assert replicas[0].usage.allocated == 0
    # Seems allocated_snapshots also reports snapshots usage for a restore !?
    assert replicas[0].usage.allocated_snapshots == new_volume.spec.size


@then(
    parsers.parse(
        "we can create volume restore {index:d} with the previous snapshot as its source"
    )
)
@given(
    parsers.parse(
        "we can create volume restore {index:d} with the previous snapshot as its source"
    )
)
def we_create_volume_restore_index_with_the_previous_snapshot_as_its_source(
    volume_uuids, snapshot_uuids, index
):
    """we create volume restore <index> with the previous snapshot as its source."""
    body = CreateVolumeBody(
        VolumePolicy(True),
        replicas=1,
        size=20 * 1024 * 1024,
        thin=True,
    )
    yield ApiClient.volumes_api().put_snapshot_volume(
        snapshot_uuids[index - 1], volume_uuids[index], body
    )
    ApiClient.volumes_api().del_volume(volume_uuids[index])
    if index == 1:
        volumes = ApiClient.volumes_api().get_volumes()
        assert len(volumes.entries) == 1
        snapshots = ApiClient.snapshots_api().get_volumes_snapshots()
        assert len(snapshots.entries) == 1
