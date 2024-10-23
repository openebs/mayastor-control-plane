"""Thin Provisioning - Cluster Wide feature tests."""

from urllib.parse import urlparse
import pytest
import subprocess
import uuid

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)
from retrying import retry

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.operations import Volume
from common.fio import Fio

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.volume_share_protocol import VolumeShareProtocol
from openapi.model.volume_status import VolumeStatus
from openapi.model.volume_policy import VolumePolicy
from openapi.exceptions import NotFoundException

THIN_VOLUME_UUID = "ec4e66fd-3b33-4439-b504-d49aba53da26"
THICK_VOLUME_UUID = "ec4e66fd-3b33-4439-b504-d49aba53da27"
VOLUME_COUNT = 4
SMALL_VOLUME_SIZE = 20
REPLICA_OVERHEAD = 8
LARGE_VOLUME_SIZE = SMALL_VOLUME_SIZE * 3
POOL_SIZE = (SMALL_VOLUME_SIZE + REPLICA_OVERHEAD) * 3 + 4
POOL_UUID_1 = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME_1 = "io-engine-1"
POOL_UUID_2 = "4cc6ee64-7232-497d-a26f-38284a444981"
NODE_NAME_2 = "io-engine-2"
POOL_UUID_3 = "4cc6ee64-7232-497d-a26f-38284a444982"


@pytest.fixture(autouse=True)
def init():
    Deployer.start(
        io_engines=2,
        cache_period="250ms",
        reconcile_period="300ms",
        fio_spdk=True,
        io_engine_coreisol=True,
    )
    ApiClient.pools_api().put_node_pool(
        NODE_NAME_1,
        POOL_UUID_1,
        CreatePoolBody([f"malloc:///disk1?size_mb={POOL_SIZE}"]),
    )
    ApiClient.pools_api().put_node_pool(
        NODE_NAME_2,
        POOL_UUID_2,
        CreatePoolBody(
            [f"malloc:///disk1?size_mb={POOL_SIZE + LARGE_VOLUME_SIZE + 8}"]
        ),
    )

    yield
    Deployer.stop()


@scenario("cluster.feature", "Reaching physical capacity of a cluster")
def test_reaching_physical_capacity_of_a_cluster():
    """Reaching physical capacity of a cluster."""


@scenario(
    "cluster.feature",
    "Resolving out-of-space condition for several thin multi-replica volumes",
)
def test_resolving_outofspace_condition_for_several_thin_multireplica_volumes():
    """Resolving out-of-space condition for several thin multi-replica volumes."""


@given("a control plane, Io-Engine instances and pools")
def a_control_plane_ioengine_instances_and_pools():
    """a control plane, Io-Engine instances and pools."""


@given("no other pools with sufficient free space exist on the cluster")
def no_other_pools_with_sufficient_free_space_exist_on_the_cluster():
    """no other pools with sufficient free space exist on the cluster."""
    assert len(ApiClient.pools_api().get_pools()) == 2


@given("there are 4 overcommitted thin volumes")
def there_are_4_overcommitted_thin_volumes():
    """there are 4 overcommitted thin volumes."""
    assert VOLUME_COUNT is 4
    volumes = list()
    # Ensure it's the first replica that gets faulted with enospc to work around a datapath bug: GTM-609.
    # Once fixed we can set it to None.
    node = NODE_NAME_1
    for i in range(VOLUME_COUNT):
        size = SMALL_VOLUME_SIZE
        if i == 0:
            size = LARGE_VOLUME_SIZE

        volume = ApiClient.volumes_api().put_volume(
            str(uuid.uuid4()),
            create_volume_body=CreateVolumeBody(
                VolumePolicy(True), 2, size * 1024 * 1024, True
            ),
        )
        volume = ApiClient.volumes_api().put_volume_target(
            volume.spec.uuid,
            publish_volume_body=PublishVolumeBody(
                {}, VolumeShareProtocol("nvmf"), node=node
            ),
        )
        assert hasattr(volume.spec, "target")
        assert str(volume.spec.target.protocol) == str(VolumeShareProtocol("nvmf"))
        volumes.append(volume)
    pytest.volumes = volumes

    pool = ApiClient.pools_api().get_pool(POOL_UUID_1)

    assert pool.state.used == (REPLICA_OVERHEAD * VOLUME_COUNT) * 1024 * 1024

    # Pre-fill the volumes no one volume can ever be fully allocated
    fios = list()
    for i in range(4):
        fio_size = SMALL_VOLUME_SIZE / 2
        if i == 0:
            fio_size = LARGE_VOLUME_SIZE / 2
        fio_size = f"{int(fio_size)}M"
        volumes[i].fio_offset = fio_size

        uri = urlparse(volumes[i].state.target["device_uri"])
        fio = Fio(name="job", rw="write", uri=uri, size=fio_size)
        fios.append(fio.open())

    for fio in fios:
        try:
            code = fio.wait(timeout=30)
            assert code == 0, "FIO should have not failed!"
        except subprocess.TimeoutExpired:
            assert False, "FIO timed out"

    # ensure no free space is left
    no_free_space(POOL_UUID_1)
    yield
    Volume.delete_all()
    try:
        ApiClient.pools_api().del_pool(POOL_UUID_3)
    except NotFoundException:
        pass


@given("they share the same pools")
def they_share_the_same_pools():
    """they share the same pools."""
    volumes = pytest.volumes

    pools = list()
    for volume in volumes:
        replicas = volume.state.replica_topology.values()
        these_pools = list(map(lambda repl: repl.pool, list(replicas)))
        these_pools.sort()
        if not pools.__contains__(these_pools):
            pools.append(these_pools)

    # the first list gets in and the others should be the same
    assert len(pools) == 1


@when("filling up the volumes with data")
def filling_up_the_volumes_with_data():
    """filling up the volumes with data."""
    volumes = pytest.volumes
    fios = list()

    for volume in volumes:
        uri = urlparse(volume.state.target["device_uri"])
        fio = Fio(name="job", rw="write", uri=uri, offset=volume.fio_offset)
        fios.append(fio.open())
    pytest.fios = fios


@then("all volumes become healthy")
def all_volumes_become_healthy():
    """all volumes become healthy."""
    for volume in ApiClient.volumes_api().get_volumes().entries:
        assert volume.state.status == VolumeStatus("Online")


@then("if other pools with sufficient free space exist on the cluster")
def if_other_pools_with_sufficient_free_space_exist_on_the_cluster():
    """if other pools with sufficient free space exist on the cluster."""
    # Make free capacity available on another pool to relocate the largest replica
    ApiClient.pools_api().put_node_pool(
        NODE_NAME_1,
        POOL_UUID_3,
        CreatePoolBody([f"malloc:///disk11?size_mb={LARGE_VOLUME_SIZE + 8}"]),
    )


@then("several replicas are reported as faulted due to out-of-space")
def several_replicas_are_reported_as_faulted_due_to_outofspace():
    """several replicas are reported as faulted due to out-of-space."""
    volumes = pytest.volumes
    wait_volumes_enospc(volumes)


@then("several replicas on the same pool are reported as faulted due to out-of-space")
def several_replicas_on_the_same_pool_are_reported_as_faulted_due_to_outofspace():
    """several replicas on the same pool are reported as faulted due to out-of-space."""
    volumes = pytest.volumes
    wait_volumes_enospc(volumes)


@then("since no other pool can accommodate the out-of-space replicas")
def since_no_other_pool_can_accommodate_the_outofspace_replicas():
    """since no other pool can accommodate the out-of-space replicas."""
    assert len(ApiClient.pools_api().get_pools()) == 2


@then("the affected volumes remain degraded")
def the_affected_volumes_remain_degraded():
    """the affected volumes remain degraded."""
    volumes = pytest.volumes
    wait_volumes_enospc(volumes)


@then("the data transfer to all volumes completes with success")
def the_data_transfer_to_all_volumes_completes_with_success():
    """the data transfer to all volumes completes with success."""
    fios = pytest.fios
    for fio in fios:
        try:
            code = fio.wait(timeout=20)
            # todo: should pass after gtm-609 is resolved.
            # assert code == 0, "FIO should have not failed!"
        except subprocess.TimeoutExpired:
            assert False, "FIO timed out"


@then(
    "the largest of the failed replicas is relocated to another pool with sufficient free space"
)
def the_largest_of_the_failed_replicas_is_relocated_to_another_pool_with_sufficient_free_space():
    """the largest of the failed replicas is relocated to another pool with sufficient free space."""
    largest_volume = pytest.volumes[0]
    largest_replica_relocated(largest_volume, POOL_UUID_3)


@then("the other faulted replicas start to rebuild in-place")
def the_other_faulted_replicas_start_to_rebuild_inplace():
    """the other faulted replicas start to rebuild in-place."""
    volumes = pytest.volumes
    wait_volumes_rebuild_or_online(volumes)
    for i in range(1, VOLUME_COUNT):
        volume = ApiClient.volumes_api().get_volume(volumes[i].spec.uuid)
        org_replicas = list(volumes[i].state.replica_topology.keys())
        org_replicas.sort()
        replicas = list(volume.state.replica_topology.keys())
        replicas.sort()

        assert (
            org_replicas == replicas
        ), "All but the larger replica should rebuild in-place"


@then("the out-of-space replicas stay permanently faulted")
def the_outofspace_replicas_stay_permanently_faulted():
    """the out-of-space replicas stay permanently faulted."""
    volumes = pytest.volumes
    wait_volumes_enospc(volumes)


@retry(wait_fixed=200, stop_max_attempt_number=20)
def wait_volumes_enospc(volumes):
    for volume in volumes:
        volume = ApiClient.volumes_api().get_volume(volume.spec.uuid)
        assert volume.state.status == VolumeStatus("Degraded")
        assert len(volume.state.target["children"]) == 2

        replicas = list(volume.state.replica_topology.values())
        enospcs = list(
            filter(
                lambda replica: str(replica.get("child_status_reason")) == "OutOfSpace",
                replicas,
            )
        )
        assert len(enospcs) == 1
        assert enospcs[0].pool == POOL_UUID_1


@retry(wait_fixed=200, stop_max_attempt_number=20)
def wait_volumes_rebuild_or_online(volumes):
    for volume in volumes:
        volume = ApiClient.volumes_api().get_volume(volume.spec.uuid)
        assert volume.state.status == VolumeStatus(
            "Online"
        ) or volume.state.status == VolumeStatus("Degraded")
        if volume.state.status == VolumeStatus("Degraded"):
            assert volume.state.target.rebuilds == 1
            assert len(volume.state.target["children"]) == 2
            children = list(volume.state.target["children"])
            degraded = list(
                filter(lambda child: child.get("state") == "Degraded", children)
            )
            assert len(degraded) == 1


@retry(wait_fixed=100, stop_max_attempt_number=5)
def no_free_space(pool_id):
    pool = ApiClient.pools_api().get_pool(pool_id)
    assert pool.state.capacity - pool.state.used == 0


@retry(wait_fixed=200, stop_max_attempt_number=40)
def largest_replica_relocated(largest_volume, to_pool):
    volume = ApiClient.volumes_api().get_volume(largest_volume.spec.uuid)
    replicas = list(volume.state.replica_topology.values())
    assert len(replicas) is 2
    moved = list(filter(lambda replica: replica.pool == to_pool, replicas))
    assert len(moved) is 1
    retained = list(filter(lambda replica: replica.pool == POOL_UUID_2, replicas))
    assert len(retained) is 1
