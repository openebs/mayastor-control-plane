"""Volume Snapshot deletion feature tests."""
import time

import pytest
from pytest_bdd import given, scenario, then, when, parsers
from retrying import retry

import openapi.exceptions
from common.apiclient import ApiClient
from common.deployer import Deployer
from common.operations import Snapshot, Volume, Cluster, wait_node_online
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_share_protocol import VolumeShareProtocol
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.volume_policy import VolumePolicy

VOLUME1_UUID = "d01b8bfb-0116-47b0-a03a-447fcbdc0e99"
POOL1_NAME = "pool-1"
NODE1 = "io-engine-1"
VOLUME1_SIZE = 1024 * 1024 * 32
SNAP1_UUID = "3f49d30d-a446-4b40-b3f6-f439345f1ce9"
SNAP2_UUID = "3f49d30d-a446-4b40-b3f6-f439345f1ce1"


@pytest.fixture(scope="module")
def disks():
    yield Deployer.create_disks(1, size=300 * 1024 * 1024)
    Deployer.cleanup_disks(1)


@pytest.fixture(scope="module")
def deployer_cluster(disks):
    Deployer.start(io_engines=1, cache_period="200ms", reconcile_period="300ms")
    put_pool(disks[0])
    pytest.exception = None
    yield
    Deployer.stop()


@scenario("delete.feature", "Remove pool where snapshot is present")
def test_remove_pool_where_snapshot_is_present():
    """Remove pool where snapshot is present."""


@scenario("delete.feature", "Remove volume source after snapshot creation")
def test_remove_volume_source_after_snapshot_creation():
    """Remove published volume source after snapshot creation."""


@scenario("delete.feature", "Snapshot deletion volume")
def test_snapshot_deletion_volume():
    """Snapshot deletion volume."""


@scenario("delete.feature", "Snapshot deletion volume after volume deletion")
def test_snapshot_deletion_volume_after_volume_deletion():
    """Snapshot deletion volume after volume deletion."""


@scenario("delete.feature", "Delete Snapshots in Order")
def test_delete_snapshots_in_order():
    """Delete Snapshots in Order."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given(parsers.parse("we have a single replica {publish_status} volume"))
def a_single_replica_publish_status_volume(publish_status):
    """a single replica <publish_status> volume."""
    ApiClient.volumes_api().put_volume(
        VOLUME1_UUID,
        CreateVolumeBody(
            VolumePolicy(False),
            replicas=1,
            size=VOLUME1_SIZE,
            thin=False,
        ),
    )
    if publish_status == "published":
        ApiClient.volumes_api().put_volume_target(
            VOLUME1_UUID,
            publish_volume_body=PublishVolumeBody(
                {}, VolumeShareProtocol("nvmf"), node=NODE1, frontend_node="app-node-1"
            ),
        )
    yield
    Volume.cleanup(VOLUME1_UUID)


@given("we've created a snapshot for the volume")
def weve_created_a_snapshot_for_the_volume():
    """we've created a snapshot for the volume."""
    ApiClient.snapshots_api().put_volume_snapshot(VOLUME1_UUID, SNAP1_UUID)
    yield
    Snapshot.cleanup(SNAP1_UUID)


@when("the snapshot is deleted")
@given("the snapshot is deleted")
def the_snapshot_is_deleted():
    """the snapshot is deleted."""
    ApiClient.snapshots_api().del_snapshot(SNAP1_UUID)


@when("the source volume is deleted")
def the_source_volume_is_deleted():
    """the source volume is deleted."""
    ApiClient.volumes_api().del_volume(VOLUME1_UUID)


@when("we attempt to delete the pool hosting the snapshot")
def we_attempt_to_delete_the_pool_hosting_the_snapshot(disks):
    """we attempt to delete the pool hosting the snapshot."""
    try:
        ApiClient.pools_api().del_pool(POOL1_NAME)
    except openapi.exceptions.ApiException as e:
        pytest.exception = e
    yield
    put_pool(disks[0])


@when("we attempt to delete the snapshot")
def we_attempt_to_delete_the_snapshot():
    """we attempt to delete the snapshot."""
    pytest.exception = None
    try:
        ApiClient.snapshots_api().del_snapshot(SNAP1_UUID)
    except openapi.exceptions.ApiException as e:
        pytest.exception = e


@when("we attempt to delete the source volume")
def we_attempt_to_delete_the_source_volume():
    """we attempt to delete the source volume."""
    pytest.exception = None
    try:
        ApiClient.volumes_api().del_volume(VOLUME1_UUID)
    except openapi.exceptions.ApiException as e:
        pytest.exception = e


@then("the pool deletion should fail")
def the_pool_deletion_should_fail():
    """the pool deletion should fail."""
    assert pytest.exception is not None
    assert pytest.exception.status == 409


@then("the snapshot deletion should not fail")
def the_snapshot_deletion_should_not_fail():
    """the snapshot deletion should not fail."""
    assert pytest.exception is None


@then("the snapshot should be present upon listing")
def the_snapshot_should_be_present_upon_listing():
    """the snapshot should be present."""
    try:
        ApiClient.snapshots_api().get_volumes_snapshot(SNAP1_UUID)
    except openapi.exceptions.ApiException:
        # We should not have reached here for the test to pass.
        assert False


@then("the snapshot should not be present upon listing")
def the_snapshot_should_not_be_present_upon_listing():
    """the snapshot should not be present upon listing."""
    try:
        ApiClient.snapshots_api().get_volumes_snapshot(SNAP1_UUID)
        # We should have panicked above for the test to pass.
        assert False
    except openapi.exceptions.ApiException as e:
        assert e.status == 404


@then("the volume should be deleted")
def the_volume_should_be_deleted():
    """the volume should be deleted."""
    assert pytest.exception is None


@then("the volume should not be present upon listing")
def the_volume_should_not_be_present_upon_listing():
    try:
        ApiClient.volumes_api().get_volume(VOLUME1_UUID)
        # We should have panicked above for the test to pass.
        assert False
    except openapi.exceptions.ApiException as e:
        assert e.status == 404


@then("we should be able to delete the pool")
def we_should_be_able_to_delete_the_pool(disks):
    """we should be able to delete the pool."""
    try:
        ApiClient.pools_api().del_pool(POOL1_NAME)
    except openapi.exceptions.ApiException:
        # We should not have reached here for the test to pass.
        assert False
    yield
    put_pool(disks[0])


def put_pool(disk):
    try:
        ApiClient.pools_api().put_node_pool(NODE1, POOL1_NAME, CreatePoolBody([disk]))
    except openapi.exceptions.ApiException:
        pass


@given("we've created a snapshot 2 for the volume", target_fixture="snapshot_2")
def weve_created_a_snapshot_2_for_the_volume():
    """we've created a snapshot 2 for the volume."""
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(VOLUME1_UUID, SNAP2_UUID)
    yield snapshot
    Snapshot.cleanup(SNAP2_UUID)


@given("we've deleted the volume")
def weve_deleted_the_volume():
    """we've deleted the volume."""
    ApiClient.volumes_api().del_volume(VOLUME1_UUID)


@when("the io-engine node where snapshot 2 resides on is restarted")
def the_ioengine_node_where_snapshot_2_resides_on_is_restarted(snapshot_2):
    """the io-engine node where snapshot 2 resides on is restarted."""
    assert len(snapshot_2.state.replica_snapshots) == 1
    replica_snapshot = snapshot_2.state.replica_snapshots[0]
    assert hasattr(replica_snapshot, "online")
    pool_id = replica_snapshot.get("online").pool_id
    pool_node = ApiClient.pools_api().get_pool(pool_id).spec.node
    Cluster.restart_node(pool_node)
    time.sleep(0.1)
    wait_node_online(pool_node)


@when("the snapshot 2 is deleted")
def the_snapshot_2_is_deleted(snapshot_2):
    """the snapshot 2 is deleted."""
    ApiClient.snapshots_api().del_snapshot(snapshot_2.definition.spec.uuid)


@then("the pool usage should be zero")
def the_pool_usage_should_be_zero():
    """the pool usage should be zero."""
    pools = ApiClient.pools_api().get_pools()
    assert len(pools) == 1
    wait_pool_zero(pools[0].id)


@then("the snapshot 2 should still be online")
def the_snapshot_2_should_still_be_online(snapshot_2):
    """the snapshot 2 should still be online."""
    snapshot = Snapshot.update(snapshot_2, cached=False)
    assert len(snapshot.state.replica_snapshots) == 1
    replica_snapshot = snapshot.state.replica_snapshots[0]
    assert hasattr(replica_snapshot, "online")


@retry(wait_fixed=10, stop_max_attempt_number=200)
def wait_pool_zero(pool_id):
    pool = ApiClient.pools_api().get_pool(pool_id)
    assert pool.state.used == 0
