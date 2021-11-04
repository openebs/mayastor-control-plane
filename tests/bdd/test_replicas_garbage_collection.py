"""Garbage collection of replicas feature tests."""
import subprocess
import time

import requests
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

from retrying import retry

import os
import pytest
import common

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.protocol import Protocol
from openapi.model.volume_policy import VolumePolicy

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 2

MAYASTOR_1 = "mayastor-1"
MAYASTOR_2 = "mayastor-2"

POOL_DISK1 = "disk1.img"
POOL1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_DISK2 = "disk2.img"
POOL2_UUID = "4cc6ee64-7232-497d-a26f-38284a444990"


@pytest.fixture(scope="function")
def create_pool_disk_images():
    # When starting Mayastor instances with the deployer a bind mount is created from /tmp to
    # /host/tmp, so create disk images in /tmp
    for disk in [POOL_DISK1, POOL_DISK2]:
        path = "/tmp/{}".format(disk)
        with open(path, "w") as file:
            file.truncate(20 * 1024 * 1024)

    yield
    for disk in [POOL_DISK1, POOL_DISK2]:
        path = "/tmp/{}".format(disk)
        if os.path.exists(path):
            os.remove(path)


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool is created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init(create_pool_disk_images):
    # Shorten the reconcile periods and cache period to speed up the tests.
    common.deployer_start_with_args(
        [
            "-j",
            "-m=2",
            "-w=10s",
            "--reconcile-idle-period=1s",
            "--reconcile-period=1s",
            "--cache-period=1s",
        ]
    )

    # Create pools
    common.get_pools_api().put_node_pool(
        MAYASTOR_1,
        POOL1_UUID,
        CreatePoolBody(["aio:///host/tmp/{}".format(POOL_DISK1)]),
    )
    common.get_pools_api().put_node_pool(
        MAYASTOR_2,
        POOL2_UUID,
        CreatePoolBody(["aio:///host/tmp/{}".format(POOL_DISK2)]),
    )

    # Create and publish a volume on node 1
    request = CreateVolumeBody(VolumePolicy(False), NUM_VOLUME_REPLICAS, VOLUME_SIZE)
    common.get_volumes_api().put_volume(VOLUME_UUID, request)
    common.get_volumes_api().put_volume_target(
        VOLUME_UUID, MAYASTOR_1, Protocol("nvmf")
    )

    yield
    common.deployer_stop()


@scenario(
    "features/garbage-collection/replicas.feature", "destroying an orphaned replica"
)
def test_destroying_an_orphaned_replica():
    """destroying an orphaned replica."""


@given("a replica which is managed but does not have any owners")
def a_replica_which_is_managed_but_does_not_have_any_owners():
    """a replica which is managed but does not have any owners."""

    # Kill the Mayastor instance which does not host the nexus.
    common.kill_container(MAYASTOR_2)

    # Attempt to delete the volume. This will leave a replica behind on the node that is
    # inaccessible.
    try:
        common.get_volumes_api().del_volume(VOLUME_UUID)
    except Exception as e:
        # A Mayastor node is inaccessible, so deleting the volume will fail because the replica
        # on this node cannot be destroyed. Attempting to do so results in a timeout. This is
        # expected and results in a replica being orphaned.
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["request_timeout"]
        pass

    check_orphaned_replica()


@then("the replica should eventually be destroyed")
def the_replica_should_eventually_be_destroyed():
    """the replica should eventually be destroyed."""

    # Restart the previously killed Mayastor instance. This makes the previously inaccessible
    # node accessible, allowing the garbage collector to delete the replica.
    common.restart_container(MAYASTOR_2)
    check_zero_replicas()


@retry(wait_fixed=1000, stop_max_attempt_number=5)
def check_zero_replicas():
    assert len(common.get_replicas_api().get_replicas()) == 0


@retry(wait_fixed=1000, stop_max_attempt_number=10)
def check_orphaned_replica():
    # There should only be one replica remaining - the one on the node that is inaccessible.
    replicas = common.get_specs_api().get_specs()["replicas"]
    assert len(replicas) == 1

    # Check that the replica is an orphan (i.e. it is managed but does not have any owners).
    replica = replicas[0]
    assert replica["managed"]
    assert len(replica["owners"]["nexuses"]) == 0
    assert "volume" not in replica["owners"]
