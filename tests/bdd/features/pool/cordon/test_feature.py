"""Pool Cordoning feature tests."""

import http
import uuid

import openapi.exceptions
import pytest
from common import human_sleep
from common.apiclient import ApiClient
from common.deployer import Deployer
from common.operations import Cluster
from common.operations import Pool as PoolOps
from common.operations import Snapshot as SnapshotOps
from common.operations import Volume as VolumeOps
from common.operations import wait_node_status, wait_pool_online, wait_volume_status
from openapi.models.create_pool_body import CreatePoolBody
from openapi.models.create_volume_body import CreateVolumeBody
from openapi.models.labelled_topology import LabelledTopology
from openapi.models.node_status import NodeStatus
from openapi.models.pool_status import PoolStatus
from openapi.models.pool_topology import PoolTopology
from openapi.models.publish_volume_body import PublishVolumeBody
from openapi.models.topology import Topology
from openapi.models.volume import Volume
from openapi.models.volume_policy import VolumePolicy
from openapi.models.volume_share_protocol import VolumeShareProtocol
from openapi.models.volume_status import VolumeStatus
from pytest_bdd import given, parsers, scenario, then, when

VOLUME_SIZE = 10 * 1024 * 1024
RECONCILE_PERIOD = "80ms"


@pytest.fixture(scope="module")
def init():
    Deployer.start(
        3,
        cache_period="50ms",
        reconcile_period=RECONCILE_PERIOD,
        faulted_child_wait_period="0ms",
    )
    yield
    Deployer.stop()


@pytest.fixture
def disks(init):
    yield Deployer.create_disks(3, size=100 * 1024 * 1024)
    Cluster.cleanup()
    Deployer.cleanup_disks(3)


@scenario("feature.feature", "Cordoning a cordoned pool")
def test_cordoning_a_cordoned_pool():
    """Cordoning a cordoned pool."""


@scenario("feature.feature", "Cordoning a pool with resources")
def test_cordoning_a_pool_with_resources():
    """Cordoning a pool with resources."""


@scenario("feature.feature", "Cordoning a pool")
def test_cordoning_a_pool():
    """Cordoning a pool."""


@scenario("feature.feature", "Deleting resources on a cordoned pool")
def test_deleting_resources_on_a_cordoned_pool():
    """Deleting resources on a cordoned pool."""


@scenario(
    "feature.feature", "Pool should be cordoned if there is at least one cordon applied"
)
def test_pool_should_be_cordoned_if_there_is_at_least_one_cordon_applied():
    """Pool should be cordoned if there is at least one cordon applied."""


@scenario(
    "feature.feature", "Pool should be uncordoned when all cordons have been removed"
)
def test_pool_should_be_uncordoned_when_all_cordons_have_been_removed():
    """Pool should be uncordoned when all cordons have been removed."""


@scenario("feature.feature", "Restarting a cordoned pool")
def test_restarting_a_cordoned_pool():
    """Restarting a cordoned pool."""


@scenario("feature.feature", "Restarting a cordoned pool with import constraint")
def test_restarting_a_cordoned_pool_with_import_constraint():
    """Restarting a cordoned pool with import constraint."""


@scenario("feature.feature", "Uncordoning a pool")
def test_uncordoning_a_pool():
    """Uncordoning a pool."""


@scenario("feature.feature", "No replica rebuild due to pool cordon")
def test_no_replica_rebuild_due_to_pool_cordon():
    """No replica rebuild due to pool cordon."""


@scenario("feature.feature", "No replica count increase due to pool cordon")
def test_no_replica_count_increase_due_to_pool_cordon():
    """No replica count increase due to pool cordon."""


@given("a cordoned pool", target_fixture="pool")
def _(disks):
    """a cordoned pool."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    pool = PoolOps.cordon(pool)
    yield pool
    PoolOps.cleanup(pool)


@given("a cordoned pool with import constraint", target_fixture="pool")
def _(disks):
    """a cordoned pool with import constraint."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    pool = PoolOps.cordon(pool, imports=True)
    yield pool
    PoolOps.cleanup(pool)


@given("a cordoned pool with multiple cordon resources", target_fixture="pool")
def _(disks):
    """a cordoned pool with multiple cordon resources."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    pool = PoolOps.cordon(pool)
    yield pool
    PoolOps.cleanup(pool)


@given("a cordoned pool with resources", target_fixture="pool_resources")
def _(disks):
    """a cordoned pool with resources."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    # create volumes
    volume_1 = ApiClient.volumes_api().put_volume(
        str(uuid.uuid4()),
        CreateVolumeBody(
            policy=VolumePolicy(self_heal=True),
            replicas=1,
            size=VOLUME_SIZE,
            thin=True,
            encrypted=False,
        ),
    )
    volume_2 = ApiClient.volumes_api().put_volume(
        str(uuid.uuid4()),
        CreateVolumeBody(
            policy=VolumePolicy(self_heal=True),
            replicas=1,
            size=VOLUME_SIZE,
            thin=True,
            encrypted=False,
        ),
    )
    # then cordon
    pool = PoolOps.cordon(pool)
    yield {
        "pool": pool,
        "volumes": [volume_1, volume_2],
    }
    VolumeOps.cleanup(volume_1)
    VolumeOps.cleanup(volume_2)
    PoolOps.cleanup(pool)


@pytest.fixture
def pool(pool_resources):
    return pool_resources["pool"]


@given("a published volume with multiple replicas", target_fixture="volume")
def _(disks):
    """a published volume with multiple replicas."""
    pool_1 = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    pool_2 = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(1),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[1]}"]),
    )
    volume = ApiClient.volumes_api().put_volume(
        str(uuid.uuid4()),
        CreateVolumeBody(
            policy=VolumePolicy(self_heal=True),
            replicas=2,
            size=VOLUME_SIZE,
            thin=True,
            encrypted=False,
        ),
    )
    volume = ApiClient.volumes_api().put_volume_target(
        volume.spec.uuid,
        publish_volume_body=PublishVolumeBody(
            publish_context={},
            protocol=VolumeShareProtocol("nvmf"),
            node=Deployer.node_name(0),
        ),
    )
    yield volume
    VolumeOps.cleanup(volume)
    PoolOps.cleanup(pool_1)
    PoolOps.cleanup(pool_2)


@given("a cordoned pool, otherwise schedulable for the volume", target_fixture="pool")
def _(disks):
    """a cordoned pool, otherwise schedulable for the volume."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(2),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[2]}"]),
    )
    pool = PoolOps.cordon(pool)
    yield pool
    PoolOps.cleanup(pool)


@given("an uncordoned pool", target_fixture="pool")
def _(disks):
    """an uncordoned pool."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    yield pool
    PoolOps.cleanup(pool)


@given("an uncordoned pool with test resources", target_fixture="pool")
def _(disks):
    """an uncordoned pool with test resources."""
    pool = ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0),
        Deployer.pool_name(),
        CreatePoolBody(disks=[f"{disks[0]}"]),
    )
    # Create test requirements, ie snapshots need volume present
    volume = schedule_repl(pool, False, False)
    snapshot = ApiClient.snapshots_api().put_volume_snapshot(
        volume.spec.uuid, str(uuid.uuid4())
    )
    pool.additional_properties["volume"] = volume.spec.uuid
    pool.additional_properties["snapshot"] = snapshot.definition.spec.uuid
    yield pool
    PoolOps.cleanup(pool)


@given("multiple uncordoned nodes")
def _(disks):
    """multiple uncordoned nodes."""


@when("the pool is uncordoned")
def _(pool):
    """the pool is uncordoned."""
    PoolOps.uncordon(pool)


@when("the cordoned pool is restarted")
def _(pool):
    """the cordoned pool is restarted."""
    node = pool.spec.node
    Deployer.stop_node(node)
    wait_node_status(node, [NodeStatus("Offline"), NodeStatus("Unknown")])
    Deployer.restart_node(node)


@when("the volume becomes degraded")
def _(volume):
    """the volume becomes degraded."""
    Deployer.stop_node(Deployer.node_name(1))
    wait_volume_status(volume, VolumeStatus("Degraded"))
    yield
    Deployer.restart_node(Deployer.node_name(1))
    wait_node_status(Deployer.node_name(1), NodeStatus("Online"))


@when("there are insufficient uncordoned pools to accommodate new replicas")
def _():
    """there are insufficient uncordoned pools to accommodate new replicas."""


@when("we attempt to delete resources on the cordoned pool")
def _(pool_resources):
    """we attempt to delete resources on the cordoned pool."""
    for volume in pool_resources["volumes"]:
        ApiClient.volumes_api().del_volume(volume.spec.uuid)


@when("we issue a cordon command")
def _(pool):
    """we issue a cordon command."""
    PoolOps.cordon(pool)


@when("we issue a cordon command with additional constraints")
def _(pool):
    """we issue a cordon command with additional constraints."""
    PoolOps.cordon(pool, True, True, True, False)


@when("we issue an uncordon command with all resources")
def _(pool):
    """we issue an uncordon command with all resources."""
    remove_all_cordons(pool)


@when("we issue an uncordon command with the cordoned resources")
def _(pool):
    """we issue an uncordon command with the cordoned resources."""
    remove_all_cordons(pool)


@when("we issue an uncordon command without all resources")
def _(pool):
    """we issue an uncordon command without all resources."""
    resources = cordon_resources(pool)
    assert resources is not None
    assert resources.replicas == True
    assert resources.snapshots == False
    assert resources.restores == True
    assert resources.var_import == False
    PoolOps.uncordon(pool, True, False, False, False)


@when("we attempt to increase the replica count", target_fixture="set_repl_request")
@then("we attempt to increase the replica count", target_fixture="set_repl_request")
def _(volume):
    """we attempt to increase the replica count."""

    def set_repl(volume):
        try:
            response = ApiClient.volumes_api().put_volume_replica_count(
                volume.spec.uuid, volume.spec.num_replicas + 1
            )
        except openapi.exceptions.ApiException as e:
            response = e
        return response

    yield set_repl
    VolumeOps.cleanup(volume)


@when(
    parsers.parse("we issue a cordon command with resource {resource}"),
    target_fixture="resources",
)
def _(pool, resource):
    """we issue a cordon command with resource <resource>."""
    assert resource in ["replicas", "snapshots", "restores"]
    cordon = {
        "replicas": resource == "replicas",
        "snapshots": resource == "snapshots",
        "restores": resource == "restores",
        "import": resource == "import",
    }
    PoolOps.cordon(pool, cordon["replicas"], cordon["snapshots"], cordon["restores"])
    yield cordon


@then(
    parsers.parse("new {resource} resources cannot be scheduled on the cordoned pool")
)
def _(pool, resource, resources):
    """new <resource> resources cannot be scheduled on the cordoned pool."""
    pool_rsc = cordon_resources(PoolOps.update(pool))
    assert pool_rsc, f"{pool.id} is not cordoned!"
    assert pool_rsc.replicas == resources["replicas"]
    assert pool_rsc.snapshots == resources["snapshots"]
    assert pool_rsc.restores == resources["restores"]
    assert pool_rsc.var_import == resources["import"]
    assert pool_rsc.to_dict()[resource] == True

    if resource == "replicas":
        schedule_repl(pool, True)
    elif resource == "snapshots":
        schedule_snap(pool, True)
    elif resource == "restores":
        schedule_restore(pool, True)
    else:
        assert False, "Unexpected!"


@then("other resources can")
def _(pool, resources):
    """other resources can."""
    if not resources["replicas"]:
        schedule_repl(pool, False)
    if not resources["snapshots"]:
        schedule_snap(pool, False)
    if not resources["restores"]:
        schedule_restore(pool, False)


@then("a new set-replica request should succeed")
def _(volume, set_repl_request):
    """a new set-replica request should succeed."""
    response = set_repl_request(volume)
    assert isinstance(response, Volume)
    assert response.spec.num_replicas, volume.spec.num_replicas + 1


@then("the request should fail with insufficient storage")
def _(volume, set_repl_request):
    """the request should fail with insufficient storage."""
    response = set_repl_request(volume)
    assert isinstance(response, openapi.exceptions.ApiException)
    assert response.status == http.HTTPStatus.INSUFFICIENT_STORAGE
    assert ApiClient.exception_to_error(response).kind == "ResourceExhausted"


@then("the pool should be not be imported")
def _(pool):
    """the pool should be not be imported."""
    for i in range(2):
        human_sleep(RECONCILE_PERIOD)
    pool = PoolOps.update(pool)
    assert pool.state is None


@then("all pool resources should be Online")
def _(pool):
    """all pool resources should be Online."""
    pool = PoolOps.update(pool, False)
    assert pool.state
    assert pool.state.status == PoolStatus("Online")


@then("new resources can be scheduled on the cordoned pool")
def _(pool):
    """new resources can be scheduled on the cordoned pool."""
    schedule_repl(pool, False)


@then("new resources cannot be scheduled on the cordoned pool")
def _(pool):
    """new resources cannot be scheduled on the cordoned pool."""
    schedule_repl(pool, True)


@then("the command will succeed")
def _():
    """the command will succeed."""


@then("the pool should be imported successfully")
def _(pool):
    """the pool should be imported successfully."""
    wait_pool_online(pool)


@then("the pool should be uncordoned")
def _(pool):
    """the pool should be uncordoned."""
    pool = PoolOps.update(pool)
    assert not is_cordoned(pool)


@then("the pool should remain cordoned")
def _(pool):
    """the pool should remain cordoned."""
    pool = PoolOps.update(pool)
    assert is_cordoned(pool)


@then("the resources should be deleted")
def _(pool_resources):
    """the resources should be deleted."""
    pool = PoolOps.update(pool_resources["pool"])
    assert pool.state.used == 0


@then("the volume shall eventually rebuild become healthy")
def _(volume):
    """the volume shall eventually rebuild become healthy."""
    wait_volume_status(volume, VolumeStatus("Online"))


@then("the volume will remain in a degraded state")
def _(volume):
    """the volume will remain in a degraded state."""
    Cluster.wait_cache_update(0.1)
    volume = VolumeOps.update(volume)
    assert volume.state.status == VolumeStatus("Degraded")


def is_cordoned(pool):
    pool = ApiClient.pools_api().get_pool(pool.id)
    return cordon_resources(pool) is not None


def cordon_resources(pool):
    cordon = pool.spec.cordon_drain
    if cordon is None:
        return None
    # cast to a dic to get around openapi bug with AllOf
    cordoned = cordon.cordoned
    if cordoned is None:
        return None
    return cordoned


def remove_all_cordons(pool):
    if is_cordoned(pool):
        PoolOps.uncordon(pool)


def schedule_repl(pool, cordoned, cleanup=True):
    try:
        key = "openebs.io/name"
        ApiClient.pools_api().put_pool_label(pool.id, "openebs.io/name", pool.id)
        volume = ApiClient.volumes_api().put_volume(
            str(uuid.uuid4()),
            CreateVolumeBody(
                policy=VolumePolicy(self_heal=True),
                replicas=1,
                size=VOLUME_SIZE,
                thin=True,
                encrypted=False,
                topology=Topology(
                    pool_topology=PoolTopology(
                        labelled=LabelledTopology(
                            exclusion={},
                            inclusion={key: pool.id},
                        )
                    )
                ),
            ),
        )
        if cleanup:
            VolumeOps.cleanup(volume)
        assert not cordoned
        return volume

    except openapi.exceptions.ApiException as exception:
        assert cordoned
        assert exception.status == http.HTTPStatus.INSUFFICIENT_STORAGE
        assert ApiClient.exception_to_error(exception).kind == "ResourceExhausted"
        return None


def schedule_snap(pool, cordoned):
    try:
        snapshot = ApiClient.snapshots_api().put_volume_snapshot(
            pool.additional_properties["volume"], str(uuid.uuid4())
        )
        SnapshotOps.cleanup(snapshot)
        assert not cordoned

    except openapi.exceptions.ApiException as exception:
        assert cordoned
        assert exception.status == http.HTTPStatus.PRECONDITION_FAILED
        assert ApiClient.exception_to_error(exception).kind == "FailedPrecondition"


def schedule_restore(pool, cordoned):
    try:
        body = CreateVolumeBody(
            policy=VolumePolicy(self_heal=True),
            replicas=1,
            size=VOLUME_SIZE,
            thin=True,
            encrypted=False,
        )
        volume = ApiClient.volumes_api().put_snapshot_volume(
            pool.additional_properties["snapshot"], str(uuid.uuid4()), body
        )
        VolumeOps.cleanup(volume)
        assert not cordoned

    except openapi.exceptions.ApiException as exception:
        assert cordoned
        assert exception.status == http.HTTPStatus.PRECONDITION_FAILED
        assert ApiClient.exception_to_error(exception).kind == "FailedPrecondition"
