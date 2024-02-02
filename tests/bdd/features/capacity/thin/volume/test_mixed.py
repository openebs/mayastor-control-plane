"""Thin Provisioning feature tests."""

from urllib.parse import urlparse
import pytest
import subprocess

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.fio import Fio

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.protocol import Protocol
from openapi.model.volume_status import VolumeStatus
from openapi.model.volume_policy import VolumePolicy

THIN_VOLUME_UUID = "ec4e66fd-3b33-4439-b504-d49aba53da26"
THICK_VOLUME_UUID = "ec4e66fd-3b33-4439-b504-d49aba53da27"
VOLUME_SIZE = 20 * 1024 * 1024
POOL_SIZE = VOLUME_SIZE + 16 * 1024 * 1024
POOL_UUID_1 = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME_1 = "io-engine-1"


@pytest.fixture(autouse=True)
def init():
    Deployer.start(
        io_engines=2,
        cache_period="250ms",
        reconcile_period="500ms",
        fio_spdk=True,
        io_engine_coreisol=True,
    )
    pool_size_mb = int(POOL_SIZE / (1024 * 1024))
    ApiClient.pools_api().put_node_pool(
        NODE_NAME_1,
        POOL_UUID_1,
        CreatePoolBody([f"malloc:///disk1?size_mb={pool_size_mb}"]),
    )

    yield
    Deployer.stop()


@scenario(
    "mixed.feature",
    "Out-of-space condition does not affect thick volumes on mixed pools",
)
def test_outofspace_condition_does_not_affect_thick_volumes_on_mixed_pools():
    """Out-of-space condition does not affect thick volumes on mixed pools."""


@given("a control plane, Io-Engine instances and pools")
def a_control_plane_ioengine_instances_and_pools():
    """a control plane, Io-Engine instances and pools."""


@given("a single replica overcommitted thin volume")
def a_single_replica_overcommitted_thin_volume():
    """a single replica overcommitted thin volume."""
    volume = ApiClient.volumes_api().put_volume(
        THIN_VOLUME_UUID,
        create_volume_body=CreateVolumeBody(VolumePolicy(True), 1, VOLUME_SIZE, True),
    )
    volume = ApiClient.volumes_api().put_volume_target(
        volume.spec.uuid,
        publish_volume_body=PublishVolumeBody({}, Protocol("nvmf")),
    )
    assert hasattr(volume.spec, "target")
    assert str(volume.spec.target.protocol) == str(Protocol("nvmf"))
    pytest.thin_volume = volume


@given("a single replica thick volume")
def a_single_replica_thick_volume():
    """a single replica thick volume."""
    volume = ApiClient.volumes_api().put_volume(
        THICK_VOLUME_UUID,
        create_volume_body=CreateVolumeBody(VolumePolicy(True), 1, VOLUME_SIZE, True),
    )
    volume = ApiClient.volumes_api().put_volume_target(
        volume.spec.uuid,
        publish_volume_body=PublishVolumeBody({}, Protocol("nvmf")),
    )
    assert hasattr(volume.spec, "target")
    assert str(volume.spec.target.protocol) == str(Protocol("nvmf"))
    pytest.thick_volume = volume


@given("both volumes share the same pool")
def both_volumes_share_the_same_pool():
    """both volumes share the same pool."""
    thin_volume = pytest.thin_volume
    thick_volume = pytest.thick_volume

    thin_replica = list(thin_volume.state.replica_topology.values())[0]
    thick_replica = list(thick_volume.state.replica_topology.values())[0]

    assert thin_replica.pool == thick_replica.pool


@when("data is being written to the thick volume")
def data_is_being_written_to_the_thick_volume():
    """data is being written to the thick volume."""
    volume = pytest.thick_volume
    uri = urlparse(volume.state.target["deviceUri"])

    fio = Fio(name="job", rw="write", uri=uri)
    pytest.thick_fio = fio.open()


@when(
    "data is being written to the thin volume which exceeds the free space on the pool"
)
def data_is_being_written_to_the_thin_volume_which_exceeds_the_free_space_on_the_pool():
    """data is being written to the thin volume which exceeds the free space on the pool."""
    volume = pytest.thin_volume
    thin_replica = list(volume.state.replica_topology.values())[0]
    pool = ApiClient.pools_api().get_pool(thin_replica.pool)

    pool_avail_size_mb = int((pool.state.capacity - pool.state.used) / 1024 / 1024)
    assert (
        volume.spec.size > pool_avail_size_mb
    ), "Volume data cannot fit in the pool free space"

    uri = urlparse(volume.state.target["deviceUri"])

    fio = Fio(name="job", rw="write", uri=uri, size=f"{pool_avail_size_mb+1}M")
    pytest.thin_fio = fio.open()


@then("a write failure with ENOSPC error is reported on the thin volume")
def a_write_failure_with_enospc_error_is_reported_on_the_thin_volume():
    """a write failure with ENOSPC error is reported on the thin volume."""
    fio = pytest.thin_fio
    try:
        code = fio.wait(timeout=30)
        assert code != 0, "FIO should have failed!"
    except subprocess.TimeoutExpired:
        assert False, "FIO timed out"


@then("the app using the thick volume continues to run unaffected")
def the_app_using_the_thick_volume_continues_to_run_unaffected():
    """the app using the thick volume continues to run unaffected."""
    fio = pytest.thick_fio
    try:
        code = fio.wait(timeout=30)
        assert code == 0, "FIO failed, exit code: %d" % code
    except subprocess.TimeoutExpired:
        assert False, "FIO timed out"


@then("the thick volume is not affected and remains healthy")
def the_thick_volume_is_not_affected_and_remains_healthy():
    """the thick volume is not affected and remains healthy."""
    volume = pytest.thick_volume
    volume = ApiClient.volumes_api().get_volume(volume.spec.uuid)
    assert volume.state.status == VolumeStatus("Online")
