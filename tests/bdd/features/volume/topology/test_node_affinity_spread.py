"""Volume Topology feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)
import requests
import docker
import pytest
from retrying import retry

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.nvme import nvme_connect, nvme_disconnect
from time import sleep
from common.fio import Fio
from common.operations import Cluster

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_policy import VolumePolicy
from openapi.model.topology import Topology
from openapi.model.node_topology import NodeTopology
from openapi.model.labelled_topology import LabelledTopology
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_spec import VolumeSpec



NODE_1_NAME = "io-engine-1"
NODE_2_NAME = "io-engine-2"
NODE_3_NAME = "io-engine-3"
NODE_1_LABLES = "node1-specific-key=node1-specific-value",
NODE_2_LABLES = "node2-specific-key=node2-specific-value",
NODE_3_LABLES = "node3-specific-key=node3-specific-value",
POOL_1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_2_UUID = "91a60318-bcfe-4e36-92cb-ddc7abf212ea"
POOL_3_UUID = "4d471e62-ca17-44d1-a6d3-8820f6156c1a"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_UUID_1 = "5cd5378e-3f05-47f1-a830-a0f5873a1441"
VOLUME_SIZE = 10485761
POOL_SIZE = 734003200
NUM_VOLUME_REPLICAS = 1
MULTIPLE_VOLUME_REPLICA = 2
FAULTED_CHILD_WAIT_SECS = 2
FIO_RUN = 7
NUM_IO_ENGINES = 3
SLEEP_BEFORE_START = 1
CREATE_REQUEST_KEY = "create_request"


POOL_CONFIGURATIONS = [
    {
        "node_name": NODE_1_NAME,
        "pool_uuid": POOL_1_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-1",
            },
        ),
    },
    {
        "node_name": NODE_2_NAME,
        "pool_uuid": POOL_2_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-2",
            },
        ),
    },
    {
        "node_name": NODE_3_NAME,
        "pool_uuid": POOL_3_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-3",
            },
        ),
    },
]

NODE_LABELS = [
        ("node1-specific-key=node1-specific-value", NODE_1_NAME),
        ("node2-specific-key=node2-specific-value", NODE_2_NAME),
        ("node3-specific-key=node3-specific-value", NODE_3_NAME),
]


# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool is created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True)
def init():
    Deployer.start(NUM_IO_ENGINES)
    # Create the nodes with labels.
    for label, node_name in NODE_LABELS:
        [key, value] = label.split("=")
        ApiClient.nodes_api().put_node_label(node_name, key, value, overwrite="false")


    # Create the pools.
    for config in POOL_CONFIGURATIONS:
        ApiClient.pools_api().put_node_pool(
            config["node_name"],
            config["pool_uuid"],
            config["pool_body"],
        )

    yield
    Deployer.stop()


# Fixture used to pass the volume create request between test steps.
@pytest.fixture(scope="function")
def create_request():
    return {}

# Fixture used to pass the volume create request between test steps.
@pytest.fixture(scope="function")
def create_request():
    return {}


# Fixture used to pass the replica context between test steps.
@pytest.fixture(scope="function")
def replica_ctx():
    return {}

@scenario('node-affinity-spread.feature', 'suitable pool on node  whose labels contain volume topology labels')
def test_suitable_pool_on_node__whose_labels_contain_volume_topology_labels():
    """suitable pool on node  whose labels contain volume topology labels."""

@scenario('node-affinity-spread.feature', 'unsuitable pool on node whose labels contain volume topology labels')
def test_unsuitable_pool_on_node_whose_labels_contain_volume_topology_labels():
    """unsuitable pool on node whose labels contain volume topology labels."""

@scenario('node-affinity-spread.feature', 'suitable pools which caters volume exclusion topology labels')
def test_suitable_pools_which_caters_volume_exclusion_topology_labels():
    """suitable pools which caters volume exclusion topology labels."""

@scenario('node-affinity-spread.feature', 'desired number of replicas cannot be created')
def test_desired_number_of_replicas_cannot_be_created():
    """desired number of replicas cannot be created."""

@scenario(
    "pool-affinity-spread.feature",
    "desired number of replicas cannot be created if topology inclusion labels and exclusion label as same",
)
def test_desired_number_of_replicas_cannot_be_created_if_topology_inclusion_labels_and_exclusion_label_as_same():
    """desired number of replicas cannot be created if topology inclusion labels and exclusion label as same."""

@given('a control plane, three Io-Engine instances, three pools')
def a_control_plane_three_ioengine_instances_three_pools():
    """a control plane, three Io-Engine instances, three pools."""
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
    assert len(pools) == 3

@given(
    "a request for a volume with its topology inclusion labels and exclusion label as same"
)
def a_request_for_a_volume_with_its_topology_inclusion_labels_and_exclusion_label_as_same(
    create_request,
):
    """a request for a volume with its topology inclusion labels and exclusion label as same."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={"node1-specific-key": "node1-specific-value"},
                    inclusion={"node1-specific-key": "node1-specific-value"},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request


@given('a request for a volume with its topology node inclusion labels not matching with any node with labels')
def a_request_for_a_volume_with_its_topology_node_inclusion_labels_not_matching_with_any_node_with_labels(create_request):
    """a request for a volume with its topology node inclusion labels not matching with any node with labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"fake-label-key": "fake-label-value"},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request

@given('a request for a volume with its node topology exclusion labels key matching with any node with labels')
def a_request_for_a_volume_with_its_node_topology_exclusion_labels_key_matching_with_any_node_with_labels(create_request):
    """a request for a volume with its node topology exclusion labels key matching with any node with labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={"node1-specific-key": ""},
                    inclusion={},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request

@given('a request for a volume with its node topology inclusion labels matching certain nodes with same labels')
def a_request_for_a_volume_with_its_node_topology_inclusion_labels_matching_certain_nodes_with_same_labels(create_request):
    """a request for a volume with its node topology inclusion labels matching certain nodes with same labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"node1-specific-key": "node1-specific-value",},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request

@given('a request for a volume with its node topology inclusion labels matching lesser number of nodes with same labels')
def a_request_for_a_volume_with_its_node_topology_inclusion_labels_matching_lesser_number_of_nodes_with_same_labels(create_request):
    """a request for a volume with its node topology inclusion labels matching lesser number of nodes with same labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        MULTIPLE_VOLUME_REPLICA,
        VOLUME_SIZE,
        False,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"node1-specific-key": "node1-specific-value",},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request

@when('the volume replicas has node topology inclusion labels matching to the labels as that of certain nodes')
def the_volume_replicas_has_node_topology_inclusion_labels_matching_to_the_labels_as_that_of_certain_nodes(create_request):
    """the volume replicas has node topology inclusion labels matching to the labels as that of certain nodes."""

@when('the volume replicas has node topology inclusion labels not matching with any node with labels')
def the_volume_replicas_has_node_topology_inclusion_labels_not_matching_with_any_node_with_labels():
    """the volume replicas has node topology inclusion labels not matching with any node with labels."""

@when('the volume replicas count is more than the node topology inclusion labels matching to the labels as that of certain nodes')
def the_volume_replicas_count_is_more_than_the_node_topology_inclusion_labels_matching_to_the_labels_as_that_of_certain_nodes():
    """the volume replicas count is more than the node topology inclusion labels matching to the labels as that of certain nodes."""

@when("the volume replicas has topology inclusion labels and exclusion labels same")
def the_volume_replicas_has_topology_inclusion_labels_and_exclusion_labels_same(
    create_request,
):
    """the volume replicas has topology inclusion labels and exclusion labels same."""

@when('the volume replicas has node topology exclusion labels key matching with any node with labels key and not value')
def the_volume_replicas_has_node_topology_exclusion_labels_key_matching_with_any_node_with_labels_key_and_not_value():
    """the volume replicas has node topology exclusion labels key matching with any node with labels key and not value."""


@then('volume creation should succeed with a returned volume object with node topology')
def volume_creation_should_succeed_with_a_returned_volume_object_with_node_topology(create_request):
    """volume creation should succeed with a returned volume object with node topology."""
    expected_spec = VolumeSpec(
        1,
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        0,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"node1-specific-key": "node1-specific-value"},
                )
            ),
        ),
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state["status"]) == "Online"
    for key, value in volume.state.replica_topology.items():
            assert str( value["pool"] == POOL_1_UUID)


@then('node labels must contain all the volume request topology labels')
def node_labels_must_contain_all_the_volume_request_topology_labels(create_request):
    """node labels must contain all the volume request topology labels."""
    assert (
        common_labels(
            create_request[CREATE_REQUEST_KEY]["topology"]["node_topology"]["labelled"][
                "inclusion"
            ],
            ApiClient.nodes_api().get_node(NODE_1_NAME),
        )
    ) == len(
        create_request[CREATE_REQUEST_KEY]["topology"]["node_topology"]["labelled"][
            "inclusion"
        ]
    )

@then('replica creation should fail with an insufficient storage error')
def replica_creation_should_fail_with_an_insufficient_storage_error(create_request):
    """replica creation should fail with an insufficient storage error."""
    request = create_request[CREATE_REQUEST_KEY]
    try:
        ApiClient.volumes_api().put_volume(VOLUME_UUID_1, request)
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["insufficient_storage"]

    # Check that the volume wasn't created.
    volumes = ApiClient.volumes_api().get_volumes().entries
    assert len(volumes) == 0


@then('volume creation should succeed with a returned volume object with exclusion topology key same as pool key and different value')
def volume_creation_should_succeed_with_a_returned_volume_object_with_exclusion_topology_key_same_as_pool_key_and_different_value(create_request):
    """volume creation should succeed with a returned volume object with exclusion topology key same as pool key and different value."""
    expected_spec = VolumeSpec(
        1,
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        0,
        topology=Topology(
            node_topology=NodeTopology(
                labelled=LabelledTopology(
                    exclusion={"node1-specific-key": ""},
                    inclusion={},
                )
            )
        ),
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    # assert str(volume.spec) == str(expected_spec)
    assert str(volume.state["status"]) == "Online"
    for key, value in volume.state.replica_topology.items():
        assert str(value["pool"] == POOL_2_UUID) or str(value["pool"] == POOL_3_UUID)

def common_labels(volume_node_topology_labels, node):
    node_labels = node["spec"]["labels"]
    print(node_labels)
    count = 0
    for key in volume_node_topology_labels:
        if key in node_labels and volume_node_topology_labels[key] == node_labels[key]:
            count += 1
    return count
