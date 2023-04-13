"""Partial Rebuild feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import http
import os
import subprocess
import time
from urllib.parse import urlparse
from retrying import retry

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.fio import Fio

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.protocol import Protocol
from openapi.exceptions import ApiException
from openapi.model.volume_status import VolumeStatus
from openapi.model.volume_policy import VolumePolicy
from openapi.model.pool_status import PoolStatus
from openapi.model.replica_state import ReplicaState
from openapi.model.child_state import ChildState

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 3
NODE_1_NAME = "io-engine-1"
NODE_2_NAME = "io-engine-2"
NODE_3_NAME = "io-engine-3"
NODE_4_NAME = "io-engine-4"
POOL_1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_2_UUID = "91a60318-bcfe-4e36-92cb-ddc7abf212ea"
POOL_3_UUID = "4d471e62-ca17-44d1-a6d3-8820f6156c1a"
POOL_4_UUID = "d5c5e3de-d77b-11ed-afa1-0242ac120002"
RECONCILE_PERIOD_SECS = 1
FIO_RUNTIME = 120
faulted_child_uri = None


@pytest.fixture(autouse=True)
def init(disks):
    Deployer.start(
        io_engines="4",
        wait="10s",
        reconcile_period=f"{RECONCILE_PERIOD_SECS}s",
        faulted_child_wait_period="15s",
        cache_period="1s",
    )

    assert len(disks) == (NUM_VOLUME_REPLICAS + 1)
    # Only create 3 pools so we can control where the intial replicas are placed.
    ApiClient.pools_api().put_node_pool(
        NODE_1_NAME, POOL_1_UUID, CreatePoolBody([f"aio://{disks[0]}"])
    )

    ApiClient.pools_api().put_node_pool(
        NODE_2_NAME, POOL_2_UUID, CreatePoolBody([f"aio://{disks[1]}"])
    )

    ApiClient.pools_api().put_node_pool(
        NODE_3_NAME, POOL_3_UUID, CreatePoolBody([f"aio://{disks[2]}"])
    )

    yield
    Deployer.stop()


@pytest.fixture
def tmp_files():
    files = []
    for itr in range(NUM_VOLUME_REPLICAS + 1):
        files.append(f"/tmp/node-{itr + 1}-disk")
    yield files


@pytest.fixture
def disks(tmp_files):
    for disk in tmp_files:
        if os.path.exists(disk):
            os.remove(disk)
        with open(disk, "w") as file:
            file.truncate(100 * 1024 * 1024)

    # /tmp is mapped into /host/tmp within the io-engine containers
    yield list(map(lambda file: f"/host{file}", tmp_files))

    for disk in tmp_files:
        if os.path.exists(disk):
            os.remove(disk)


@scenario(
    "log-based-rebuild.feature",
    "Faulted child is not online again within timed-wait period",
)
def test_faulted_child_is_not_online_again_within_timedwait_period():
    """Faulted child is not online again within timed-wait period."""


@scenario(
    "log-based-rebuild.feature",
    "Faulted child is online again within timed-wait period",
)
def test_faulted_child_is_online_again_within_timedwait_period():
    """Faulted child is online again within timed-wait period."""


@scenario(
    "log-based-rebuild.feature",
    "Node goes permanently down while log-based rebuild running",
)
def test_node_goes_permanently_down_while_logbased_rebuild_running():
    """Node goes permanently down while log-based rebuild running."""


@scenario(
    "log-based-rebuild.feature",
    "Volume target is moved before starting log-based rebuild",
)
def test_volume_target_is_moved_before_starting_logbased_rebuild():
    """Volume target is moved before starting log-based rebuild."""


@scenario(
    "log-based-rebuild.feature", "Volume target is moved during log-based rebuild"
)
def test_volume_target_is_moved_during_logbased_rebuild():
    """Volume target is moved during log-based rebuild."""


@given("a volume with three replicas, filled with user data")
def a_volume_with_three_replicas_filled_with_user_data(disks):
    """a volume with three replicas, filled with user data."""
    request = CreateVolumeBody(
        VolumePolicy(True), NUM_VOLUME_REPLICAS, VOLUME_SIZE, False
    )
    ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, Protocol("nvmf"), node=NODE_1_NAME, frontend_node=""
        ),
    )

    # Now the volume has been created, create an additional pool.
    ApiClient.pools_api().put_node_pool(
        NODE_4_NAME, POOL_4_UUID, CreatePoolBody([f"aio://{disks[3]}"])
    )

    # Launch fio in background and let it always run along with the test.
    """
    uri = urlparse(volume["state"]["target"]["deviceUri"])
    fio = Fio("job", "randrw", uri=uri, runtime=FIO_RUNTIME).build_for_userspace()
    try:
        subprocess.run(fio, shell=True, check=True)
    except subprocess.CalledProcessError:
        remove_stale_files_on_error(uri.hostname, uri.path[1:])
        assert False, "FIO is not expected to be errored out"
    """


@given("io-engine is installed and running")
def io_engine_is_installed_and_running():
    """io-engine is installed and running."""
    pass


@when("a child becomes faulted")
def a_child_becomes_faulted():
    """a child becomes faulted."""
    # Fault a replica by stopping the container with the replica.
    # Check the replica becomes unhealthy by waiting for the volume to become degraded.
    Docker.stop_container(NODE_2_NAME)
    wait_for_degraded_volume()
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    target = vol.state.target
    childlist = target["children"]
    for child in childlist:
        if child["state"] == ChildState("Faulted"):
            faulted_child_uri = child["uri"]
            assert faulted_child_uri is not None


@when("a non-local child becomes faulted")
def a_non_local_child_becomes_faulted():
    """a non-local child becomes faulted."""
    pass


@when(
    "network connection between nexus and a replica node temporarily goes down and restored"
)
def _():
    """network connection between nexus and a replica node temporarily goes down and restored."""
    pass


@when("the replica is not online again within the timed-wait period")
def the_replica_is_not_online_again_within_the_timed_wait_period():
    """the replica is not online again within the timed-wait period."""
    # The replica shouldn't be available on this node still.
    time.sleep(2)
    replica = ApiClient.replicas_api().get_node_replicas(NODE_2_NAME)
    assert len(replica) == 0, "Replica not expected to be available on " + NODE_2_NAME


@when("the replica is online again within the timed-wait period")
def the_replica_is_online_again_within_the_timed_wait_period():
    """the replica is online again within the timed-wait period."""
    # The control plane should have attempted the online child by now.
    # Let's wait a few moments.
    time.sleep(2)
    replica = ApiClient.replicas_api().get_node_replicas(NODE_2_NAME)
    if len(replica) == 1:
        assert replica[0].state == ReplicaState("Online")
    else:
        # assert False, "Expected a replica to be available again on " + NODE_2_NAME
        pass


@when("the volume target moves to a different node before the log-based rebuild starts")
def _():
    """the volume target moves to a different node before the log-based rebuild starts."""
    pass


@then("a full rebuild starts after some time")
def a_full_rebuild_starts_after_some_time():
    """a full rebuild starts after some time."""
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    target = vol.state.target
    childlist = target["children"]
    assert len(childlist) == NUM_VOLUME_REPLICAS
    for child in childlist:
        if child["state"] == ChildState("Faulted") or child["state"] == ChildState(
            "Degraded"
        ):
            assert child["uri"] != faulted_child_uri
            assert child["rebuild_progress"] != 0


@then("a log-based rebuild is started after reconnection")
def _():
    """a log-based rebuild is started after reconnection."""
    pass


@then("a log-based rebuild is started after the io-engine restarts")
def _():
    """a log-based rebuild is started after the io-engine restarts."""
    pass


@then("log-based rebuild starts after some time")
@retry(wait_fixed=1000, stop_max_attempt_number=2)
def log_based_rebuild_starts_after_some_time():
    """log-based rebuild starts after some time."""
    # TODO: Check log-based rebuild is running.
    # No direct way to check partial rebuild via openapi yet
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    target = vol.state.target
    childlist = target["children"]
    assert len(childlist) == NUM_VOLUME_REPLICAS
    for child in childlist:
        if child["state"] == ChildState("Faulted") or child["state"] == ChildState(
            "Degraded"
        ):
            assert child["uri"] == faulted_child_uri
            assert child["rebuild_progress"] != 0


@then("the node hosting rebuilding replica crashes permanently")
def _():
    """the node hosting rebuilding replica crashes permanently."""
    pass


@then("the replica is not online again within the timeout period")
def _():
    """the replica is not online again within the timeout period."""
    pass


@then("the volume target moves to a different node before rebuild completion")
def _():
    """the volume target moves to a different node before rebuild completion."""
    pass


@retry(wait_fixed=1000, stop_max_attempt_number=10)
def wait_for_degraded_volume():
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert volume.state.status == VolumeStatus("Degraded")


@retry(wait_fixed=1000, stop_max_attempt_number=10)
def wait_for_pool_online(pool_uuid):
    pool = ApiClient.pools_api().get_pool(pool_uuid)
    assert pool.state.status == PoolStatus("Online")


def remove_stale_files_on_error(ip, nqn):
    filename = (
        "{}/'trtype=tcp adrfam=IPv4 traddr={} trsvcid=8420 subnqn={} ns=1'".format(
            os.environ["ROOT_DIR"], ip, nqn
        )
    )
    command = "rm -f {}".format(filename)
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError:
        assert False, "Could not clean up the stale files, needs manual cleanup"
