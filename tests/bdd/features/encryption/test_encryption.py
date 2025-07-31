"""Encryption-at-Rest on Mayastor DiskPool feature tests."""

import json
import os

import pytest
from common.apiclient import ApiClient
from common.deployer import Deployer, workspace_tmp
from common.docker import Docker
from common.operations import Cluster
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.encryption import Encryption
from openapi.model.encryption_secret import EncryptionSecret
from openapi.model.node_status import NodeStatus
from openapi.model.pool_status import PoolStatus
from openapi.model.volume_policy import VolumePolicy
from pytest_bdd import (
    given,
    parsers,
    scenario,
    then,
    when,
)
from retrying import retry

ENCR_VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
NON_ENCR_VOLUME_UUID = "05451170-ac6a-43cb-a92a-e5e3581d1111"
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 1
ENCR_POOL = "67cda0eb-1772-4e96-8463-ca686a76196d"
NON_ENCR_POOL = "d12eaa63-2c4c-4256-a825-fdc426d1fa2d"
NODE_NAME = "io-engine-1"

SECRET_FILE_NAME = "pool-encr-secret"
SECRET_FILE_PATH = os.path.join(workspace_tmp(), SECRET_FILE_NAME)


@pytest.fixture(scope="module")
def init():
    Deployer.start(1)
    yield
    Deployer.stop()


@pytest.fixture(scope="module")
def disks():
    pools = Deployer.create_disks(2, size=100 * 1024 * 1024)
    yield pools
    Deployer.cleanup_disks(len(pools))


@pytest.fixture(autouse=True)
@given("an io-engine cluster")
def setup_io_engine_cluster(init, disks):
    """an io-engine cluster."""
    pytest.disks = disks
    yield
    Cluster.cleanup()


@scenario("feature.feature", "Creating and Importing an encrypted DiskPool")
def test_creating_and_importing_an_encrypted_diskpool():
    """Creating and Importing an encrypted DiskPool."""


@scenario("feature.feature", "Volume replica scheduling")
def test_volume_replica_scheduling():
    """Volume replica scheduling."""


@given(
    parsers.parse(
        "a user created Secret file containing key parameters with {cipher} and {keysize:d}"
    )
)
def _(cipher, keysize):
    """a user created Secret file containing key parameters with <cipher> and <keysize>."""
    if cipher == "AesXts":
        key1 = generate_key(keysize)
        key2 = generate_key(keysize)
    elif cipher == "AesCbc":
        key1 = generate_key(keysize)
        key2 = None

    SECRET_DATA = {
        "cipher": cipher,
        "key": key1,
        "key_len": keysize,
        "key2": key2,
        "key_len2": keysize if key2 else None,
    }

    with open(SECRET_FILE_PATH, "w") as file:
        json.dump(SECRET_DATA, file, indent=4)

    yield
    if os.path.exists(SECRET_FILE_PATH):
        os.remove(SECRET_FILE_PATH)


@given("the product is installed and running")
def _():
    """the product is installed and running."""
    nodes = ApiClient.nodes_api().get_nodes()
    assert len(nodes) == 1
    assert nodes[0].state.status == NodeStatus("Online")


@when("a diskpool is created with this Secret file")
def _():
    """a diskpool is created with this Secret file."""
    ApiClient.pools_api().put_node_pool(
        NODE_NAME,
        ENCR_POOL,
        CreatePoolBody(
            [f"{pytest.disks[0]}"],
            encryption=Encryption(secret=EncryptionSecret(name=SECRET_FILE_NAME)),
        ),
    )
    yield
    ApiClient.pools_api().del_node_pool(NODE_NAME, ENCR_POOL)


@when("a single replica volume is created with encryption")
def _():
    """a single replica volume is created with encryption."""
    ApiClient.volumes_api().put_volume(
        ENCR_VOLUME_UUID,
        CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False, True),
    )
    yield
    ApiClient.volumes_api().del_volume(ENCR_VOLUME_UUID)


@when("a diskpool is created without encryption")
def _():
    """a diskpool is created without encryption."""
    ApiClient.pools_api().put_node_pool(
        NODE_NAME,
        NON_ENCR_POOL,
        CreatePoolBody([f"{pytest.disks[1]}"]),
    )
    yield
    ApiClient.pools_api().del_node_pool(NODE_NAME, NON_ENCR_POOL)


@when("a single replica volume is created without encryption")
def _():
    """a single replica volume is created without encryption."""
    ApiClient.volumes_api().put_volume(
        NON_ENCR_VOLUME_UUID,
        CreateVolumeBody(VolumePolicy(False), 1, VOLUME_SIZE, False, False),
    )
    yield
    ApiClient.volumes_api().del_volume(NON_ENCR_VOLUME_UUID)


@when("the node hosting the pool reboots")
def _():
    """the node hosting the pool reboots."""
    Docker.restart_container(NODE_NAME)


@then("the encrypted disk pool gets created successfully")
def _():
    """the encrypted disk pool gets created successfully."""
    pool = ApiClient.pools_api().get_node_pool(NODE_NAME, ENCR_POOL)
    assert pool.state.status == PoolStatus("Online")
    assert pool.state.encrypted == True


@then("the encrypted disk pool gets imported successfully eventually")
@retry(wait_fixed=10, stop_max_attempt_number=200)
def _():
    """the encrypted disk pool gets imported successfully eventually."""
    pool = ApiClient.pools_api().get_node_pool(NODE_NAME, ENCR_POOL)
    assert pool.state.status == PoolStatus("Online")
    assert pool.state.encrypted == True


@then("the replica for encrypted volume should be on encrypted pool")
def _():
    """the replica for encrypted volume should be on encrypted pool."""
    volume = ApiClient.volumes_api().get_volume(ENCR_VOLUME_UUID)
    assert volume.spec.num_replicas == 1
    assert next(iter(volume.state.replica_topology.values()))["pool"] == ENCR_POOL


@then("the non encrypted disk pool gets created successfully")
def _():
    """the non encrypted disk pool gets created successfully."""
    pool = ApiClient.pools_api().get_node_pool(NODE_NAME, NON_ENCR_POOL)
    assert pool.state.status == PoolStatus("Online")
    assert pool.state.encrypted == False


@then("the replica for non encrypted volume should be on non encrypted pool")
def _():
    """the replica for non encrypted volume should be on non encrypted pool."""
    volume = ApiClient.volumes_api().get_volume(NON_ENCR_VOLUME_UUID)
    assert volume.spec.num_replicas == 1
    assert next(iter(volume.state.replica_topology.values()))["pool"] == NON_ENCR_POOL


##########
# HELPERS
##########


def generate_key(keysize):
    """Generate a random key of length keysize in bits (converted to bytes)."""
    return os.urandom(keysize // 8).hex()
