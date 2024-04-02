"""Create Volume From Snapshot feature tests."""
import http
import json

import pytest
from pytest_bdd import given, scenario, then, when, parsers
from retrying import retry

import uuid
import openapi
from common.deployer import Deployer
from common.docker import Docker
from common.apiclient import ApiClient
from common.operations import Volume, Snapshot

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_policy import VolumePolicy
from openapi.model.pool_status import PoolStatus
from openapi.model.node_status import NodeStatus

REMOTE_NODE_NAME_A = "io-engine-2"


@pytest.fixture(scope="module")
def volume_uuids():
    return list(map(lambda x: str(uuid.uuid4()), range(10)))


@pytest.fixture(scope="module")
def snapshot_uuids():
    return list(map(lambda x: str(uuid.uuid4()), range(10)))


@pytest.fixture(scope="module")
def disks():
    yield Deployer.create_disks(3)
    Deployer.delete_disks(3)


@pytest.fixture
def remote_node_a():
    return REMOTE_NODE_NAME_A


@pytest.fixture(scope="module")
def deployer_cluster(disks):
    Deployer.start(3, fio_spdk=True, cache_period="100ms", reconcile_period="150ms")
    ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0), "pool-0", CreatePoolBody([disks[0]])
    )
    ApiClient.pools_api().put_node_pool(
        Deployer.node_name(1), "pool-1", CreatePoolBody([disks[1]])
    )
    ApiClient.pools_api().put_node_pool(
        Deployer.node_name(2), "pool-2", CreatePoolBody([disks[2]])
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


@scenario("create.feature", "Snapshotting a restored volume")
def test_snapshotting_a_restored_volume():
    """Snapshotting a restored volume."""


@scenario(
    "create.feature", "Create a new volume as a snapshot restore with one node down"
)
def test_create_a_new_volume_as_a_snapshot_restore_with_one_node_down():
    """Create a new volume as a snapshot restore with one node down."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given("a valid snapshot of a multi replica volume")
def a_valid_snapshot_of_a_multi_replica_volume(volume_uuids, snapshot_uuids):
    """a valid snapshot of a multi replica volume."""
    ApiClient.volumes_api().put_volume(
        volume_uuids[0],
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=3,
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
    parsers.parse(
        "we create a new volume with {repl_count:d} replicas using snapshot as its source"
    ),
    target_fixture="restored_volume",
)
@given(
    parsers.parse(
        "we create a new volume with {repl_count:d} replicas using snapshot as its source"
    ),
    target_fixture="restored_volume",
)
def we_create_a_new_volume_with_repl_count_replicas_using_snapshot_as_its_source(
    volume_uuids, snapshot_uuids, repl_count
):
    """we create a new volume with <repl_count> replicas using snapshot as its source."""
    body = CreateVolumeBody(
        VolumePolicy(True),
        replicas=repl_count,
        size=20 * 1024 * 1024,
        thin=True,
    )
    volume = ApiClient.volumes_api().put_snapshot_volume(
        snapshot_uuids[0], volume_uuids[1], body
    )
    yield volume
    Volume.cleanup(volume)


@then(parsers.parse("we create a snapshot from volume restore {index:d}"))
@given(parsers.parse("we create a snapshot from volume restore {index:d}"))
def we_create_a_snapshot_from_volume_restore_index(volume_uuids, snapshot_uuids, index):
    """we create a snapshot from volume restore <index>."""
    ApiClient.snapshots_api().put_volume_snapshot(
        volume_uuids[index], snapshot_uuids[index]
    )
    yield
    ApiClient.snapshots_api().del_snapshot(snapshot_uuids[index])


@then(parsers.parse("a new {repl_count:d} replicas will be created for the new volume"))
def a_new_replica_will_be_created_for_the_new_volume(
    volume_uuids, restored_volume, repl_count
):
    """a new replica will be created for the new volume."""
    # check volume has a replica in the topology
    assert restored_volume.spec.uuid == volume_uuids[1]
    assert restored_volume.spec.num_replicas == repl_count
    assert restored_volume.spec.status == SpecStatus("Created")


@then("all requests should succeed", target_fixture="created_restores")
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
    yield created


@then(
    parsers.parse("all replicas {repl_count:d} capacity will be same as the snapshot")
)
def the_replicas_capacity_will_be_same_as_the_snapshot(restored_volume, repl_count):
    """the replica's capacity will be same as the snapshot."""
    replicas = list(restored_volume.state.replica_topology.values())
    assert len(replicas) == repl_count
    for replica in replicas:
        assert replica.usage.capacity == restored_volume.spec.size
        assert replica.usage.allocated == 0
        assert replica.usage.allocated_snapshots == 0


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
    volumes = ApiClient.volumes_api().get_volumes()
    assert len(volumes.entries) == index
    snapshots = ApiClient.snapshots_api().get_volumes_snapshots()
    assert len(snapshots.entries) == index


@then("the restored volume own allocated_snapshot size should be 0")
def the_restored_volume_own_allocated_snapshot_size_should_be_0(restored_volume):
    """the restored volume own allocated_snapshot size should be 0."""
    assert restored_volume.state.usage.allocated_replica == 0
    assert restored_volume.state.usage.allocated_snapshots == 0
    assert restored_volume.state.usage.total_allocated == 0


@then("the restored volume's own allocated_snapshot size should be 0")
def the_restored_volumes_own_allocated_snapshot_size_should_be_0(created_restores):
    """the restored volume's own allocated_snapshot size should be 0."""
    for restore in created_restores:
        assert restore.state.usage.allocated_replica == 0
        assert restore.state.usage.allocated_snapshots == 0
        assert restore.state.usage.total_allocated == 0


@then("we allocate 4MiB of data of the restored volume")
@when("we allocate 4MiB of data of the restored volume")
def we_allocate_4mib_of_data_of_the_restored_volume(restored_volume):
    """we allocate 4MiB of data of the restored volume."""
    Volume.fio(restored_volume, offset="7M", size="4096B")
    Volume.update(restored_volume, cached=False)


@then(
    parsers.parse("the restored volume allocation size should be {repl_count:d} * 4MiB")
)
def the_restored_volume_allocation_size_should_be_repl_count_4mib(
    restored_volume, repl_count
):
    """the restored volume allocation size should be replica_count * 4MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.allocated_replica == 4 * 1024 * 1024
    assert volume.state.usage.allocated_snapshots == 0
    assert volume.state.usage.total_allocated == repl_count * 4 * 1024 * 1024


@then(
    parsers.parse(
        "the restored volume total allocation size should be {repl_count:d} * 4MiB"
    )
)
def the_restored_volume_total_allocation_size_should_be_repl_count_4mib(
    restored_volume, repl_count
):
    """the restored volume total allocation size should be repl_count * 4MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.total_allocated == repl_count * 4 * 1024 * 1024


@then(
    parsers.parse(
        "the restored volume total allocation size should be {repl_count:d} * 8MiB"
    )
)
def the_restored_volume_total_allocation_size_should_be_repl_count_8mib(
    restored_volume, repl_count
):
    """the restored volume total allocation size should be repl_count * 8MiB."""
    volume = Volume.update(restored_volume)
    assert volume.state.usage.total_allocated == repl_count * 8 * 1024 * 1024


@then("the snapshot allocated size should be 4MiB")
def the_snapshot_allocated_size_should_be_4mib(restore_snapshot):
    """the snapshot allocated size should be 4MiB."""
    assert restore_snapshot.state.allocated_size == 4 * 1024 * 1024


@then("the snapshot 2 allocated size should be 4MiB")
def the_snapshot_2_allocated_size_should_be_4mib(restore_snapshot_2):
    """the snapshot 2 allocated size should be 4MiB."""
    assert restore_snapshot_2.state.allocated_size == 4 * 1024 * 1024


@then("we create a snapshot of the restored volume", target_fixture="restore_snapshot")
def we_create_a_snapshot_of_the_restored_volume(snapshot_uuids, volume_uuids):
    """we create a snapshot of the restored volume."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        volume_uuids[1], snapshot_uuids[1]
    )
    yield snapshot
    Snapshot.cleanup(snapshot)


@then(
    "we create another snapshot of the restored volume",
    target_fixture="restore_snapshot_2",
)
def we_create_another_snapshot_of_the_restored_volume(snapshot_uuids, volume_uuids):
    """we create another snapshot of the restored volume."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        volume_uuids[1], snapshot_uuids[2]
    )
    yield snapshot
    Snapshot.cleanup(snapshot)


@when("we have a node down")
def we_have_a_node_down(remote_node_a):
    """we have a node down."""
    Docker.stop_container(remote_node_a)


@when(
    "a request to create a new volume with the snapshot as its source",
    target_fixture="request",
)
def a_request_to_create_a_new_volume_with_the_snapshot_as_its_source():
    """a request to create a new volume with the snapshot as its source."""
    yield CreateVolumeBody(
        VolumePolicy(True),
        replicas=3,
        size=20 * 1024 * 1024,
        thin=True,
    )


@when("we bring the node back up")
def we_bring_the_node_back_up():
    """we bring the node back up."""
    restart_node(REMOTE_NODE_NAME_A)


@then("the request should fail with PreconditionFailed")
def the_request_should_fail_with_preconditionfailed(
    request, volume_uuids, snapshot_uuids
):
    """the request should fail with PreconditionFailed."""
    restore_expect(
        request,
        volume_uuids,
        snapshot_uuids,
        http.HTTPStatus.PRECONDITION_FAILED,
        "FailedPrecondition",
    )


@then(parsers.parse("the request should succeed with replica count as {repl_count:d}"))
def the_request_should_succeed_with_replica_count(
    restored_volume, volume_uuids, repl_count
):
    """the request should succeed with replica count."""
    assert restored_volume.spec.uuid == volume_uuids[1]
    assert restored_volume.spec.num_replicas == repl_count
    assert restored_volume.spec.status == SpecStatus("Created")


def restore_expect(request, volume_uuids, snapshot_uuids, status, kind):
    with pytest.raises(openapi.exceptions.ApiException) as exception:
        ApiClient.volumes_api().put_snapshot_volume(
            snapshot_uuids[0], volume_uuids[1], request
        )
    assert exception.value.status == status
    error_body = json.loads(exception.value.body)
    assert error_body["kind"] == kind


def restart_node(name):
    Docker.restart_container(name)
    wait_node_ready(name)


@retry(wait_fixed=100, stop_max_attempt_number=20)
def wait_node_ready(name):
    node = ApiClient.nodes_api().get_node(name)
    assert node.state.status == NodeStatus("Online")
    for pool in ApiClient.pools_api().get_node_pools(name):
        assert pool.state.status == PoolStatus("Online")
