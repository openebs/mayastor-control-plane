"""Volume resize feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

import http
import os
import pytest
import time
import subprocess

from urllib.parse import urlparse
from retrying import retry
from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.operations import Volume
from time import sleep

import openapi.exceptions
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from common.fio import Fio
from openapi.model.protocol import Protocol
from openapi.model.volume_policy import VolumePolicy
from openapi.model.nexus_state import NexusState
from openapi.model.child_state import ChildState
from openapi.model.pool_status import PoolStatus
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.resize_volume_body import ResizeVolumeBody


POOL1_UUID = "91a60318-bcfe-4e36-92cb-ddc7abf212ea"
POOL2_UUID = "92a60318-bcfe-4e36-92cb-ddc7abf212ea"
POOL3_UUID = "93a60318-bcfe-4e36-92cb-ddc7abf212ea"
POOL4_UUID = "94a60318-bcfe-4e36-92cb-ddc7abf212ea"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
DEFAULT_REPLICA_CNT = 3
DEFAULT_POOL_SIZE = 209715200
NODE1_NAME = "io-engine-1"
NODE2_NAME = "io-engine-2"
NODE3_NAME = "io-engine-3"
NODE4_NAME = "io-engine-4"
VOLUME_CTX_KEY = "volume"
VOLUME_SIZE = 52428800  # 50MiB
VOLUME_NEW_SIZE = 104857610  # 100MiB
VOLUME_SHRINK_SIZE = 41943040  # 40MiB


@scenario("resize_offline.feature", "Expand a published volume")
def test_expand_a_published_volume():
    """Expand a published volume."""


@scenario(
    "resize_offline.feature",
    "Expand a published volume after unpublishing it while having an offline replica",
)
def test_expand_a_published_volume_after_unpublishing_it_while_having_an_offline_replica():
    """Expand a published volume after unpublishing it while having an offline replica."""


@scenario("resize_offline.feature", "Expand an unpublished volume")
def test_expand_an_unpublished_volume():
    """Expand an unpublished volume."""


@scenario(
    "resize_offline.feature", "Expand an unpublished volume and then make it published"
)
def test_expand_an_unpublished_volume_and_then_make_it_published():
    """Expand an unpublished volume and then make it published."""


@scenario(
    "resize_offline.feature", "Expand an unpublished volume with an offline replica"
)
def test_expand_an_unpublished_volume_with_an_offline_replica():
    """Expand an unpublished volume with an offline replica."""


@scenario("resize_offline.feature", "Shrink an unpublished volume")
def test_shrink_an_unpublished_volume():
    """Shrink an unpublished volume."""


@given("a deployer cluster")
def a_deployer_cluster():
    """a deployer cluster."""


@pytest.fixture(scope="module")
def tmp_files():
    files = []
    for itr in range(DEFAULT_REPLICA_CNT + 1):
        files.append(f"/tmp/node-{itr + 1}-disk")
    yield files


@pytest.fixture(scope="module")
def disks(tmp_files):
    for disk in tmp_files:
        if os.path.exists(disk):
            os.remove(disk)
        with open(disk, "w") as file:
            file.truncate(DEFAULT_POOL_SIZE)

    # /tmp is mapped into /host/tmp within the io-engine containers
    yield list(map(lambda file: f"/host{file}", tmp_files))

    for disk in tmp_files:
        if os.path.exists(disk):
            os.remove(disk)


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# Pools are created for convenience such that they are available for use by the tests.
@pytest.fixture(autouse=True, scope="module")
def init(disks):
    Deployer.start(
        4,
        faulted_child_wait_period="2s",
        reconcile_period="100ms",
        cache_period="100ms",
        fio_spdk=True,
    )
    assert len(disks) == DEFAULT_REPLICA_CNT + 1
    ApiClient.pools_api().put_node_pool(
        NODE1_NAME, POOL1_UUID, CreatePoolBody([f"aio://{disks[0]}"])
    )
    ApiClient.pools_api().put_node_pool(
        NODE2_NAME, POOL2_UUID, CreatePoolBody([f"aio://{disks[1]}"])
    )
    ApiClient.pools_api().put_node_pool(
        NODE3_NAME, POOL3_UUID, CreatePoolBody([f"aio://{disks[2]}"])
    )
    pytest.replica_count = DEFAULT_REPLICA_CNT
    yield
    Deployer.stop()


@pytest.fixture(scope="function")
def create_volume(disks):
    volume = ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            VolumePolicy(True), int(pytest.replica_count), VOLUME_SIZE, False
        ),
    )
    assert volume.spec.uuid == VOLUME_UUID
    replicas = volume.state.replica_topology
    assert len(replicas) == int(pytest.replica_count)
    # Create an extra pool for scale up or rebuild case, if doesn't exist already.
    try:
        ApiClient.pools_api().get_node_pool(NODE4_NAME, POOL4_UUID)
    except openapi.exceptions.ApiException as err:
        if err.status == 404:
            ApiClient.pools_api().put_node_pool(
                NODE4_NAME, POOL4_UUID, CreatePoolBody([f"aio://{disks[3]}"])
            )
    yield volume
    Volume.cleanup(volume)


@given("a published volume with more than one replica and all are healthy")
def a_published_volume_with_more_than_one_replica_and_all_are_healthy(create_volume):
    """a published volume with more than one replica and all are healthy."""
    the_volume_is_published()
    pytest.exception = None


@given(
    parsers.parse(
        "an unpublished volume with {repl_count:d} replicas and all are healthy"
    )
)
@given("an unpublished volume with <repl_count> replicas and all are healthy")
def an_unpublished_volume_with_repl_count_replicas_and_all_are_healthy(repl_count):
    """an unpublished volume with <repl_count> replicas and all are healthy."""
    pytest.exception = None
    pytest.replica_count = repl_count
    volume = ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(VolumePolicy(True), int(repl_count), VOLUME_SIZE, False),
    )
    assert volume.spec.uuid == VOLUME_UUID
    replicas = volume.state.replica_topology
    assert len(replicas) == int(repl_count)
    yield volume
    Volume.cleanup(volume)


@given("an unpublished volume with more than one replica")
def an_unpublished_volume_with_more_than_one_replica(create_volume):
    """an unpublished volume with more than one replica."""


@given("an unpublished volume with more than one replica and all are healthy")
def an_unpublished_volume_with_more_than_one_replica_and_all_are_healthy(create_volume):
    """an unpublished volume with more than one replica and all are healthy."""


@when("one of the replica goes offline")
def one_of_the_replica_goes_offline():
    """one of the replica goes offline."""
    # Offline NODE3_NAME to make the replica Offline/Unknown
    Docker.stop_container(NODE3_NAME)
    wait_child_faulted()


@given("one of the replica is not in online state")
def one_of_the_replica_is_not_in_online_state():
    """one of the replica is not in online state."""
    pytest.exception = None
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    # Offline NODE3_NAME to make the replica Offline/Unknown
    Docker.stop_container(NODE3_NAME)
    yield
    the_replica_comes_online_again()


@when("the volume is receiving IO")
def the_volume_is_receiving_io():
    """the volume is receiving IO."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    uri = urlparse(volume.state.target["deviceUri"])
    fio = Fio(name="fio-pre-resize", rw="write", uri=uri, extra_args="--loops=2")
    pytest.fio = fio.open()


@when("the volume is unpublished")
def the_volume_is_unpublished():
    """the volume is unpublished."""
    # Ensure fio is still running till unpublish
    time.sleep(1)
    assert pytest.fio.poll() is None
    unpublish_volume()


@when("the replica comes online again")
def the_replica_comes_online_again():
    """the replica comes online again."""
    Docker.restart_container(NODE3_NAME)
    wait_pool_online(POOL3_UUID)


@when("the volume is published")
def the_volume_is_published():
    """the volume is published."""
    publish_volume(False, False)


@when("the volume is republished")
def the_volume_is_republished():
    """the volume is republished."""
    publish_volume(False, True)
    wait_rebuild_start()


@when("the volume replica count is increased by 1")
def the_volume_replica_count_is_increased_by_1():
    """the volume replica count is increased by 1."""
    volume = ApiClient.volumes_api().put_volume_replica_count(
        VOLUME_UUID, DEFAULT_REPLICA_CNT + 1
    )
    assert len(volume.state.replica_topology) == (DEFAULT_REPLICA_CNT + 1)


@when("we issue a volume expand request")
def we_issue_a_volume_expand_request():
    """we issue a volume expand request."""
    try:
        time.sleep(1)
        volume = ApiClient.volumes_api().put_volume_size(
            VOLUME_UUID, ResizeVolumeBody(VOLUME_NEW_SIZE)
        )
        pytest.exception = None
    except openapi.exceptions.ApiException as err:
        pytest.exception = err


@when("we issue a volume shrink request")
def we_issue_a_volume_shrink_request():
    """we issue a volume shrink request."""
    pytest.exception = None
    try:
        ApiClient.volumes_api().put_volume_size(
            VOLUME_UUID, ResizeVolumeBody(VOLUME_SHRINK_SIZE)
        )
    except openapi.exceptions.ApiException as err:
        pytest.exception = err
        volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
        Volume.cleanup(volume)


@then("all the replicas of the volume should be resized to the new capacity")
def all_the_replicas_of_the_volume_should_be_resized_to_the_new_capacity():
    """all the replicas of the volume should be resized to the new capacity."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    replicas = volume.state.replica_topology
    for replica_uuid in replicas:
        replica = ApiClient.replicas_api().get_replica(replica_uuid)
        assert replica.size >= VOLUME_NEW_SIZE


@then("the failure reason should be invalid arguments")
def the_failure_reason_should_be_invalid_arguments():
    """the failure reason should be invalid arguments."""
    # Reason code: Invalid Arguments, mapped from
    # SvcError::VolumeResizeArgsInvalid
    assert pytest.exception.status == http.HTTPStatus.BAD_REQUEST


@then("the failure reason should be volume-in-use precondition")
def the_failure_reason_should_be_volumeinuse_precondition():
    """the failure reason should be volume-in-use precondition."""
    # Reason code: Conflict, mapped from SvcError::InUse
    assert pytest.exception.status == http.HTTPStatus.CONFLICT


@then("the new capacity should be available for the application to use")
def the_new_capacity_should_be_available_for_the_application_to_use():
    """the new capacity should be available for the application to use."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert volume.state.target["size"] >= VOLUME_NEW_SIZE
    # Run IO across expanded volume capacity
    uri = urlparse(volume.state.target["deviceUri"])
    fio = Fio(name="fio-post-resize", rw="write", uri=uri, size=VOLUME_NEW_SIZE)

    try:
        code = fio.run().returncode
        assert code == 0, "Fio is expected to execute successfully"
    except subprocess.CalledProcessError:
        assert False, "FIO is not expected to be errored out"


@then("the new replica should have expanded size")
def the_new_replica_should_have_expanded_size():
    """the new replica should have expanded size."""
    all_the_replicas_of_the_volume_should_be_resized_to_the_new_capacity()


@then("the onlined replica should be rebuilt")
def the_onlined_replica_should_be_rebuilt():
    """the onlined replica should be rebuilt."""
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    target = vol.state.target
    childlist = target["children"]
    assert len(childlist) == DEFAULT_REPLICA_CNT
    wait_rebuild_finish()


@then("the request should succeed")
def the_request_should_succeed():
    """the request should succeed."""
    assert pytest.exception is None


@then("the volume expand status should be failure")
def the_volume_expand_status_should_be_failure():
    """the volume expand status should be failure."""
    the_volume_resize_status_should_be_failure()


@then("the volume is expanded to the new capacity")
def the_volume_is_expanded_to_the_new_capacity():
    """the volume is expanded to the new capacity."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert volume.spec.size == VOLUME_NEW_SIZE


@then("the volume resize status should be failure")
def the_volume_resize_status_should_be_failure():
    """the volume resize status should be failure."""
    assert pytest.exception is not None


@then("the volume should get published with expanded capacity")
def the_volume_should_get_published_with_expanded_capacity():
    """the volume should get published with expanded capacity."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert hasattr(volume.spec, "target")
    assert str(volume.spec.target.protocol) == str(Protocol("nvmf"))
    assert volume.spec.size == VOLUME_NEW_SIZE


@retry(wait_fixed=200, stop_max_attempt_number=30)
def wait_pool_online(pool_id):
    pool = ApiClient.pools_api().get_pool(pool_id)
    assert pool.state.status == PoolStatus("Online")


@retry(wait_fixed=500, stop_max_attempt_number=30)
def wait_rebuild_start():
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    childlist = vol.state.target["children"]
    assert (
        (len(childlist) == DEFAULT_REPLICA_CNT)
        and (NexusState(vol.state.target["state"]) == NexusState("Degraded"))
        and (vol.state.target["rebuilds"] == 1)
    )


@retry(wait_fixed=1000, stop_max_attempt_number=30)
def wait_rebuild_finish():
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert (vol.state.target["rebuilds"] == 0) and (
        NexusState(vol.state.target["state"]) == NexusState("Online")
    )


@retry(wait_fixed=1000, stop_max_attempt_number=20)
def wait_child_faulted():
    vol = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    target = vol.state.target
    childlist = target["children"]
    faulted = list(filter(lambda child: child.get("state") == "Faulted", childlist))
    assert len(faulted) == 1, "Failed to find 1 Faulted child!"


# Unpublish the volume
def unpublish_volume():
    volume = ApiClient.volumes_api().del_volume_target(VOLUME_UUID)
    assert not hasattr(volume.spec, "target")


# Publish the volume
def publish_volume(repub, reuse):
    volume = ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {},
            Protocol("nvmf"),
            republish=repub,
            reuse_existing=reuse,
            node=NODE2_NAME,
            frontend_node="",
        ),
    )
