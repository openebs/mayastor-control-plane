"""Volume Topology feature feature tests."""

import docker
import pytest
import requests

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker

from openapi.model.volume_policy import VolumePolicy
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.protocol import Protocol
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_spec import VolumeSpec
from openapi.model.topology import Topology
from openapi.model.pool_topology import PoolTopology
from openapi.model.labelled_topology import LabelledTopology
from openapi.model.publish_volume_body import PublishVolumeBody

VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 1
CREATE_REQUEST_KEY = "create_request"
POOL_1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_1_NAME = "io-engine-1"
POOL_2_UUID = "24d36c1a-3e6c-4e05-893d-917ec9f4c1bb"
NODE_2_NAME = "io-engine-2"
NUM_IO_ENGINES = 2
REPLICA_CONTEXT_KEY = "replica"
REPLICA_ERROR = "replica_error"


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool is created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    Deployer.start(NUM_IO_ENGINES)
    ApiClient.pools_api().put_node_pool(
        NODE_1_NAME,
        POOL_1_UUID,
        CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "pool1-specific-key": "pool1-specific-value",
                "openebs.io/created-by": "operator-diskpool",
            },
        ),
    )
    ApiClient.pools_api().put_node_pool(
        NODE_2_NAME,
        POOL_2_UUID,
        CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "pool2-specific-key": "pool2-specific-value",
                "openebs.io/created-by": "operator-diskpool",
            },
        ),
    )
    yield
    Deployer.stop()


# Fixture used to pass the volume create request between test steps.
@pytest.fixture(scope="function")
def create_request():
    return {}


# Fixture used to pass the replica context between test steps.
@pytest.fixture(scope="function")
def replica_ctx():
    return {}


@scenario(
    "feature.feature",
    "cannot add a replica to a volume with pool topology labels",
)
def test_cannot_add_a_replica_to_a_volume_with_pool_topology_labels():
    """cannot add a replica to a volume with pool topology labels."""


@scenario("feature.feature", "desired number of replicas cannot be created")
def test_desired_number_of_replicas_cannot_be_created():
    """desired number of replicas cannot be created."""


@scenario(
    "feature.feature",
    "successfully adding a replica to a volume with pool topology",
)
def test_successfully_adding_a_replica_to_a_volume_with_pool_topology():
    """successfully adding a replica to a volume with pool topology."""


@scenario(
    "feature.feature",
    "successfully adding a replica to a volume without pool topology",
)
def test_successfully_adding_a_replica_to_a_volume_without_pool_topology():
    """successfully adding a replica to a volume without pool topology."""


@scenario(
    "feature.feature",
    "sufficient suitable pools which contain volume topology labels",
)
def test_sufficient_suitable_pools_which_contain_volume_topology_labels():
    """sufficient suitable pools which contain volume topology labels."""


@scenario(
    "feature.feature",
    "sufficient suitable pools which do not contain volume pool topology labels",
)
def test_sufficient_suitable_pools_which_do_not_contain_volume_topology_labels():
    """sufficient suitable pools which do not contain volume topology labels."""


@given("a control plane, two Io-Engine instances, two pools")
def a_control_plane_two_io_engine_instances_two_pools():
    """a control plane, two Io-Engine instances, two pools."""
    docker_client = docker.from_env()

    # The control plane comprises the core agents, rest server and etcd instance.
    for component in ["core", "rest", "etcd"]:
        Docker.check_container_running(component)

    # Check all Io-Engine instances are running
    try:
        io_engines = docker_client.containers.list(
            all=True, filters={"name": "io-engine"}
        )

    except docker.errors.NotFound:
        raise Exception("No Io-Engine instances")

    for io_engine in io_engines:
        Docker.check_container_running(io_engine.attrs["Name"])

    # Check for a pools
    pools = ApiClient.pools_api().get_pools()
    assert len(pools) == 2


@given("a request for a volume with topology different from pools")
def a_request_for_a_volume_with_topology_different_from_pools(create_request):
    """a request for a volume with topology different from pools."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={}, inclusion={"fake-label-key": "fake-label-value"}
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request


@given("a request for a volume with topology same as pool labels")
def a_request_for_a_volume_with_topology_same_as_pool_labels(create_request):
    """a request for a volume with topology same as pool labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool"},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request


@given("a request for a volume without pool topology")
def a_request_for_a_volume_without_pool_topology(create_request):
    """a request for a volume without pool topology."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
    )
    create_request[CREATE_REQUEST_KEY] = request


@given("an existing published volume without pool topology")
def an_existing_published_volume_without_pool_topology():
    """an existing published volume without pool topology"""
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            VolumePolicy(False),
            1,
            VOLUME_SIZE,
            False,
        ),
    )
    # Publish volume so that there is a nexus to add a replica to.
    ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, Protocol("nvmf"), node=NODE_1_NAME, frontend_node=""
        ),
    )


@given("suitable available pools with labels")
def suitable_available_pools_with_labels():
    """suitable available pools with labels."""
    # Since the volume does not contain any topology,
    # all the pools are suitable candidates for selection
    assert len(ApiClient.pools_api().get_pools()) != 0


@given("an existing published volume with a topology matching pool labels")
def an_existing_published_volume_with_a_topology_matching_pool_labels():
    """an existing published volume with a topology matching pool labels"""
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            VolumePolicy(False),
            1,
            VOLUME_SIZE,
            False,
            topology=Topology(
                pool_topology=PoolTopology(
                    labelled=LabelledTopology(
                        exclusion={},
                        inclusion={"openebs.io/created-by": "operator-diskpool"},
                    )
                )
            ),
        ),
    )
    # Publish volume so that there is a nexus to add a replica to.
    ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, Protocol("nvmf"), node=NODE_1_NAME, frontend_node=""
        ),
    )


@given("an existing published volume with a topology not matching pool labels")
def an_existing_published_volume_with_a_topology_not_matching_pool_labels():
    """an existing published volume with a topology not matching pool labels"""
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            VolumePolicy(False),
            1,
            VOLUME_SIZE,
            False,
            topology=Topology(
                pool_topology=PoolTopology(
                    labelled=LabelledTopology(
                        exclusion={},
                        inclusion={"pool1-specific-key": "pool1-specific-value"},
                    )
                )
            ),
        ),
    )
    # Publish volume so that there is a nexus to add a replica to.
    ApiClient.volumes_api().put_volume_target(
        VOLUME_UUID,
        publish_volume_body=PublishVolumeBody(
            {}, Protocol("nvmf"), node=NODE_1_NAME, frontend_node=""
        ),
    )


@given("a pool which does not contain the volume topology label")
def a_pool_which_does_not_contain_the_volume_topology_label():
    """a pool which does not contain the volume topology label."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert (
        # From the no of suitable pools we get one would be already occupied, thus reduce the count by 1.
        # Since in this scenario, the only pool having topology labels is being used up, we are left with
        # 0 pools having topology labels.
        no_of_suitable_pools(
            volume["spec"]["topology"]["pool_topology"]["labelled"]["inclusion"]
        )
        - 1
        == 0
    )


@when("a user attempts to increase the number of volume replicas")
def a_user_attempts_to_increase_the_number_of_volume_replicas(replica_ctx):
    """a user attempts to increase the number of volume replicas."""
    volumes_api = ApiClient.volumes_api()
    volume = volumes_api.get_volume(VOLUME_UUID)
    num_replicas = volume.spec.num_replicas
    try:
        volume = volumes_api.put_volume_replica_count(VOLUME_UUID, num_replicas + 1)
        replica_ctx[REPLICA_CONTEXT_KEY] = volume.spec.num_replicas
    except Exception as e:
        replica_ctx[REPLICA_ERROR] = e


@when("the number of suitable pools is less than the number of desired volume replicas")
def the_number_of_suitable_pools_is_less_than_the_number_of_desired_volume_replicas(
    create_request,
):
    """the number of suitable pools is less than the number of desired volume replicas."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    no_of_pools = no_of_suitable_pools(
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "inclusion"
        ]
    )
    assert num_volume_replicas > no_of_pools


@when(
    "the number of volume replicas is less than or equal to the number of suitable pools"
)
def the_number_of_volume_replicas_is_less_than_or_equal_to_the_number_of_suitable_pools(
    create_request,
):
    """the number of volume replicas is less than or equal to the number of suitable pools."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    if (
        hasattr(create_request[CREATE_REQUEST_KEY], "topology")
        and "pool_topology" in create_request[CREATE_REQUEST_KEY]["topology"]
    ):
        no_of_pools = no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ]
        )
    else:
        # Here we are fetching all pools and comparing its length, because if we reach this part of code
        # it signifies the volume request has no pool topology labels, thus all pools are suitable
        no_of_pools = len(ApiClient.pools_api().get_pools())
    assert num_volume_replicas <= no_of_pools


@given("additional unused pools with labels containing volume topology")
def additional_unused_pools_with_labels_containing_volume_topology():
    """additional unused pools with labels containing volume topology."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert (
        no_of_suitable_pools(
            volume["spec"]["topology"]["pool_topology"]["labelled"]["inclusion"]
        )
        > volume["spec"]["num_replicas"]
    )


@then("an additional replica should be added to the volume")
def an_additional_replica_should_be_added_to_the_volume(replica_ctx):
    """an additional replica should be added to the volume."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert hasattr(volume.state, "target")
    nexus = volume.state.target
    assert replica_ctx[REPLICA_CONTEXT_KEY] == len(nexus["children"])
    assert REPLICA_ERROR not in replica_ctx


@then("pool labels must contain all the volume request topology labels")
def pool_labels_must_contain_all_the_volume_request_topology_labels(create_request):
    """pool labels must contain all the volume request topology labels."""
    assert (
        common_labels(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ],
            ApiClient.pools_api().get_pool(POOL_1_UUID),
        )
        or common_labels(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ],
            ApiClient.pools_api().get_pool(POOL_2_UUID),
        )
    ) == len(
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "inclusion"
        ]
    )


@then("pool labels must contain all the volume topology labels")
def pool_labels_must_contain_all_the_volume_topology_labels():
    """pool labels must contain all the volume topology labels."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert (
        common_labels(
            volume["spec"]["topology"]["pool_topology"]["labelled"]["inclusion"],
            ApiClient.pools_api().get_pool(POOL_1_UUID),
        )
        or common_labels(
            volume["spec"]["topology"]["pool_topology"]["labelled"]["inclusion"],
            ApiClient.pools_api().get_pool(POOL_2_UUID),
        )
    ) == len(volume["spec"]["topology"]["pool_topology"]["labelled"]["inclusion"])


@then("pool labels must not contain the volume topology labels")
def pool_labels_must_not_contain_the_volume_topology_labels():
    """pool labels must not contain the volume topology labels."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert (
        common_labels(
            volume["spec"]["topology"]["pool_topology"]["labelled"]["inclusion"],
            ApiClient.pools_api().get_pool(POOL_2_UUID),
        )
        == 0
    )


@then("pool labels must not contain the volume request topology labels")
def pool_labels_must_not_contain_the_volume_request_topology_labels(create_request):
    """pool labels must not contain the volume request topology labels."""
    assert (
        no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ]
        )
    ) == 0


@then("replica addition should fail with an insufficient storage error")
def replica_addition_should_fail_with_an_insufficient_storage_error(replica_ctx):
    """replica addition should fail with an insufficient storage error."""
    assert replica_ctx[REPLICA_ERROR] is not None
    exception_info = replica_ctx[REPLICA_ERROR].__dict__
    assert exception_info["status"] == requests.codes["insufficient_storage"]


@then("volume creation should fail with an insufficient storage error")
def volume_creation_should_fail_with_an_insufficient_storage_error(create_request):
    """volume creation should fail with an insufficient storage error."""
    request = create_request[CREATE_REQUEST_KEY]
    try:
        ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["insufficient_storage"]

    # Check that the volume wasn't created.
    volumes = ApiClient.volumes_api().get_volumes().entries
    assert len(volumes) == 0


@then("volume creation should succeed with a returned volume object with topology")
def volume_creation_should_succeed_with_a_returned_volume_object_with_topology(
    create_request,
):
    """volume creation should succeed with a returned volume object with topology."""
    expected_spec = VolumeSpec(
        1,
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool"},
                )
            )
        ),
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state["status"]) == "Online"


@then(
    "volume creation should succeed with a returned volume object without pool topology"
)
def volume_creation_should_succeed_with_a_returned_volume_object_without_pool_topology(
    create_request,
):
    """volume creation should succeed with a returned volume object without pool topology."""
    expected_spec = VolumeSpec(
        1, VOLUME_SIZE, SpecStatus("Created"), VOLUME_UUID, VolumePolicy(False), False
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state["status"]) == "Online"


@then("volume request should not contain any pool topology labels")
def volume_request_should_not_contain_any_pool_topology_labels(create_request):
    """volume request should not contain any pool topology labels."""
    assert (
        not hasattr(create_request[CREATE_REQUEST_KEY], "topology")
        or "pool_topology" not in create_request[CREATE_REQUEST_KEY]["topology"]
    )


@then("volume should not contain any pool topology labels")
def volume_should_not_contain_any_pool_topology_labels():
    """volume should not contain any pool topology labels."""
    volume = ApiClient.volumes_api().get_volume(VOLUME_UUID)
    assert (
        not hasattr(volume["spec"], "topology")
        or "pool_topology" not in volume["spec"]["topology"]
    )


def no_of_suitable_pools(volume_pool_topology_labels):
    pool_labels = [
        ApiClient.pools_api().get_pool(POOL_1_UUID)["spec"]["labels"],
        ApiClient.pools_api().get_pool(POOL_2_UUID)["spec"]["labels"],
    ]
    count = 0
    for labels in pool_labels:
        f = True
        for key in volume_pool_topology_labels:
            if not (key in labels and volume_pool_topology_labels[key] == labels[key]):
                f = False
        if f:
            count += 1
    return count


def common_labels(volume_pool_topology_labels, pool):
    pool_labels = pool["spec"]["labels"]
    count = 0
    for key in volume_pool_topology_labels:
        if key in pool_labels and volume_pool_topology_labels[key] == pool_labels[key]:
            count += 1
    return count
