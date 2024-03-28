"""Volume Node Topology feature tests."""

from pytest_bdd import given, scenario, then, when, parsers

import pytest
import docker
import requests

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

NUM_IO_ENGINES = 4
NODE_1_NAME = "io-engine-1"
NODE_2_NAME = "io-engine-2"
NODE_3_NAME = "io-engine-3"
NODE_4_NAME = "io-engine-4"
NODE_1_POOL_1_UUID = "node1pool1"
NODE_1_POOL_2_UUID = "node1pool2"
NODE_1_POOL_3_UUID = "node1pool3"
NODE_2_POOL_1_UUID = "node2pool1"
NODE_2_POOL_2_UUID = "node2pool2"
NODE_2_POOL_3_UUID = "node2pool3"
NODE_3_POOL_1_UUID = "node3pool1"
NODE_3_POOL_2_UUID = "node3pool2"
NODE_3_POOL_3_UUID = "node3pool3"
NODE_4_POOL_1_UUID = "node4pool1"
NODE_4_POOL_2_UUID = "node4pool2"
NODE_4_POOL_3_UUID = "node4pool3"
CREATE_REQUEST_KEY = "create_request"
VOLUME_SIZE = 10485761
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1441"

# The labels to be applied to the nodes.
################################################################################################################
#         Description                    ||     Node Name          ||    Pool Name   ||     Node Label         ||
# ===============================================================================================================
#     "io-engine-1" has                  ||   node1 (io-engine-1)  ||    node1pool1  ||                       ||
#  label "zone-us=us-west-1"             ||   node1 (io-engine-1)  ||    node1pool2  ||   zone-us=us-west-1   ||
#                                        ||   node1 (io-engine-1)  ||    node1pool3  ||                       ||
# ===============================================================================================================
#     "io-engine-2" has                  ||   node2 (io-engine-2)  ||    node2pool1  ||                       ||
#  label "zone-ap=ap-south-1"            ||   node2 (io-engine-2)  ||    node2pool2  ||   zone-ap=ap-south-1  ||
#                                        ||   node2 (io-engine-2)  ||    node2pool3  ||                       ||
# ===============================================================================================================
#     "io-engine-3" has                  ||   node3 (io-engine-3)  ||    node3pool1  ||                       ||
#  label "zone-eu=eu-west-3 "            ||   node3 (io-engine-3)  ||    node3pool2  ||   zone-eu=eu-west-3   ||
#                                        ||   node3 (io-engine-3)  ||    node3pool3  ||                       ||
# ===============================================================================================================
#     "io-engine-4" has                  ||   node4 (io-engine-4)  ||    node4pool1  ||   zone-us=us-west-1   ||
#  label "zone-us=us-west-1,             ||   node4 (io-engine-4)  ||    node4pool2  ||   zone-ap=ap-south-1  ||
# zone-ap=ap-south-1,zone-eu=eu-west-3"  ||   node4 (io-engine-4)  ||    node4pool3  ||   zone-eu=eu-west-3   ||
# ===============================================================================================================

POOL_CONFIGURATIONS = [
    # Pool node1pool1 is on node "io-engine-1"
    {
        "node_name": NODE_1_NAME,
        "pool_uuid": NODE_1_POOL_1_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node1pool1?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-1",
            },
        ),
    },
    # Pool node2pool1 is on node "io-engine-2"
    {
        "node_name": NODE_2_NAME,
        "pool_uuid": NODE_2_POOL_1_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node2pool1?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-2",
            },
        ),
    },
    #  Pool node3pool1 is on node "io-engine-3"
    {
        "node_name": NODE_3_NAME,
        "pool_uuid": NODE_3_POOL_1_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node3pool1?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-3",
            },
        ),
    },
    #  Pool node4pool1 is on node "io-engine-4"
    {
        "node_name": NODE_4_NAME,
        "pool_uuid": NODE_4_POOL_1_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node4pool1?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-4",
            },
        ),
    },
    # Pool node1pool2 is on node "io-engine-1"
    {
        "node_name": NODE_1_NAME,
        "pool_uuid": NODE_1_POOL_2_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node1pool2?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-1",
            },
        ),
    },
    # Pool node2pool2 is on node "io-engine-2"
    {
        "node_name": NODE_2_NAME,
        "pool_uuid": NODE_2_POOL_2_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node2pool2?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-2",
            },
        ),
    },
    # Pool node3pool2 is on node "io-engine-3"
    {
        "node_name": NODE_3_NAME,
        "pool_uuid": NODE_3_POOL_2_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node3pool2?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-3",
            },
        ),
    },
    # Pool node4pool2 is on node "io-engine-4"
    {
        "node_name": NODE_4_NAME,
        "pool_uuid": NODE_4_POOL_2_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node4pool2?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-4",
            },
        ),
    },
    # Pool node1pool3 is on node "io-engine-1"
    {
        "node_name": NODE_1_NAME,
        "pool_uuid": NODE_1_POOL_3_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node1pool3?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-1",
            },
        ),
    },
    # Pool node2pool3 is on node "io-engine-2"
    {
        "node_name": NODE_2_NAME,
        "pool_uuid": NODE_2_POOL_3_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node2pool3?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-2",
            },
        ),
    },
    # Pool node3pool3 is on node "io-engine-3"
    {
        "node_name": NODE_3_NAME,
        "pool_uuid": NODE_3_POOL_3_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node3pool3?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-3",
            },
        ),
    },
    # Pool node4pool3 is on node "io-engine-4"
    {
        "node_name": NODE_4_NAME,
        "pool_uuid": NODE_4_POOL_3_UUID,
        "pool_body": CreatePoolBody(
            ["malloc:///node4pool3?size_mb=50"],
            labels={
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-4",
            },
        ),
    },
]

NODE_LABELS = [
    ("zone-us=us-west-1", NODE_1_NAME),
    ("zone-ap=ap-south-1", NODE_2_NAME),
    ("zone-eu=eu-west-3", NODE_3_NAME),
    ("zone-us=us-west-1", NODE_4_NAME),
    ("zone-ap=ap-south-1", NODE_4_NAME),
    ("zone-eu=eu-west-3", NODE_4_NAME),
]


@pytest.fixture(scope="module")
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


# Fixture used to pass the replica context between test steps.
@pytest.fixture(scope="function")
def replica_ctx():
    return {}


@scenario(
    "node-topology.feature", "Suitable nodes which contain volume topology labels"
)
def test_suitable_nodes_which_contain_volume_topology_labels():
    """Suitable nodes which contain volume topology labels."""


@scenario(
    "node-topology.feature",
    "Suitable nodes which contain volume node topology keys only",
)
def test_suitable_nodes_which_contain_volume_node_topology_keys_only():
    """Suitable nodes which contain volume node topology keys only."""


@given("a control plane, four Io-Engine instances, twelve pools")
def a_control_plane_four_ioengine_instances_twelve_pools(init):
    """a control plane, four Io-Engine instances, twelve pools."""
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
    assert len(pools) == 12
    # Check for a nodes
    nodes = ApiClient.nodes_api().get_nodes()
    assert len(nodes) == 4
    node1 = ApiClient.nodes_api().get_node(NODE_1_NAME)
    assert node1["spec"]["labels"] == {"zone-us": "us-west-1"}
    node2 = ApiClient.nodes_api().get_node(NODE_2_NAME)
    assert node2["spec"]["labels"] == {"zone-ap": "ap-south-1"}
    node3 = ApiClient.nodes_api().get_node(NODE_3_NAME)
    assert node3["spec"]["labels"] == {"zone-eu": "eu-west-3"}
    node4 = ApiClient.nodes_api().get_node(NODE_4_NAME)
    assert node4["spec"]["labels"] == {
        "zone-us": "us-west-1",
        "zone-ap": "ap-south-1",
        "zone-eu": "eu-west-3",
    }
    yield
    Cluster.cleanup(pools=False)


@given(
    parsers.parse(
        "a request for a {replica} replica volume with nodeAffinityTopologyLabel as {node_affinity_topology_label} and node topology inclusion as {volume_node_topology_inclusion_label}"
    )
)
def a_request_for_a_replica_replica_volume_with_nodeaffinitytopologylabel_as_node_affinity_topology_label_and_node_topology_inclusion_as_volume_node_topology_inclusion_label(
    create_request,
    replica,
    node_affinity_topology_label,
    volume_node_topology_inclusion_label,
):
    """a request for a <replica> replica volume with nodeAffinityTopologyLabel as <node_affinity_topology_label> and node topology inclusion as <volume_node_topology_inclusion_label>."""
    if node_affinity_topology_label == "True":
        request = create_volume_body(replica, volume_node_topology_inclusion_label)
        create_request[CREATE_REQUEST_KEY] = request


@given(
    parsers.parse(
        "a request for a {replica} replica volume with nodeHasTopologyKey as {has_topology_key} and node topology inclusion as {volume_node_topology_inclusion_label}"
    )
)
def a_request_for_a_replica_replica_volume_with_nodehastopologykey_as_has_topology_key_and_node_topology_inclusion_as_volume_node_topology_inclusion_label(
    create_request, replica, has_topology_key, volume_node_topology_inclusion_label
):
    """a request for a <replica> replica volume with nodeHasTopologyKey as <has_topology_key> and node topology inclusion as <volume_node_topology_inclusion_label>."""
    if has_topology_key == "True":
        request = create_volume_body(replica, volume_node_topology_inclusion_label)
        create_request[CREATE_REQUEST_KEY] = request


@when(
    parsers.parse(
        "the desired number of replica of volume i.e. {replica} here; is {expression} number of the nodes containing the label {volume_node_topology_inclusion_label}"
    )
)
def the_desired_number_of_replica_of_volume_ie_replica_here_is_expression_number_of_the_nodes_containing_the_label_volume_node_topology_inclusion_label(
    create_request, replica, expression, volume_node_topology_inclusion_label
):
    """the desired number of replica of volume i.e. <replica> here; is <expression> number of the nodes containing the label <volume_node_topology_inclusion_label>."""
    no_of_eligible_nodes = no_of_suitable_nodes(
        create_request[CREATE_REQUEST_KEY]["topology"]["node_topology"]["labelled"][
            "inclusion"
        ],
    )
    if expression == "<=":
        assert int(replica) <= no_of_eligible_nodes
    elif expression == ">":
        assert int(replica) > no_of_eligible_nodes


@then(
    parsers.parse(
        "the {replica} replica volume creation should {result} and {provisioned} provisioned on pools with its corresponding node labels {node_label}"
    )
)
def the_replica_replica_volume_creation_should_result_and_provisioned_provisioned_on_pools_with_its_corresponding_node_labels_node_label(
    create_request, replica, result, provisioned, node_label
):
    """the <replica> replica volume creation should <result> and <provisioned> provisioned on pools with its corresponding node labels <node_label>."""
    if result == "succeed":
        # Check the volume object returned is as expected
        request = create_request[CREATE_REQUEST_KEY]
        volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
        expected_spec = expected_volume_spec(
            replica, create_request[CREATE_REQUEST_KEY]["topology"]
        )
        assert str(volume.spec) == str(expected_spec)
        node_name_whose_node_has_given_labels = get_node_names_with_given_labels(
            node_label
        )
        nodes_on_which_volume_provisioned = list()
        for replica_id, replica_details in volume.state["replica_topology"].items():
            nodes_on_which_volume_provisioned.append(replica_details["node"])
        assert set(nodes_on_which_volume_provisioned).issubset(
            set(node_name_whose_node_has_given_labels)
        )
    elif result == "fail":
        """volume creation should fail with an insufficient storage error."""
        request = create_request[CREATE_REQUEST_KEY]
        try:
            ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
        except Exception as e:
            exception_info = e.__dict__
            assert exception_info["status"] == requests.codes["precondition_failed"]

        # Check that the volume wasn't created.
        volumes = ApiClient.volumes_api().get_volumes().entries
        assert len(volumes) == 0


def get_node_names_with_given_labels(node_label):
    """Return the pool names whose node has the given labels."""
    node_with_node_labels = get_node_names_with_its_corresponding_node_labels()
    qualified_nodes = list()
    if "=" in node_label:
        # Splitting the pool_label into parts
        split_parts = node_label.split("=")
        # Accessing the parts after splitting
        node_key = split_parts[0]
        node_value = split_parts[1]
        for pool_id, pool_node_labels in node_with_node_labels.items():
            for key, value in pool_node_labels.items():
                if key == node_key and value == node_value:
                    qualified_nodes.append(pool_id)
    else:
        node_key = node_label
        for pool_id, pool_labels in node_with_node_labels.items():
            for key, value in pool_labels.items():
                if key == node_key:
                    qualified_nodes.append(pool_id)
    return qualified_nodes


# Return the create volume request body based on the input parameters.
def create_volume_body(replica, volume_node_topology_inclusion_label):
    """Create a volume body."""
    key, _, value = volume_node_topology_inclusion_label.partition("=")
    topology = Topology(
        node_topology=NodeTopology(
            labelled=LabelledTopology(
                exclusion={},
                inclusion={key.strip(): value.strip()},
            )
        )
    )

    return CreateVolumeBody(
        VolumePolicy(False),
        int(replica),
        VOLUME_SIZE,
        False,
        topology=topology,
    )


# Return the number of nodes that qualify based on the volume node topology inclusion labels.
def no_of_suitable_nodes(volume_node_topology_inclusion_labels):
    """Return the number of nodes that qualify based on the volume node topology inclusion labels."""
    node_with_its_node_labels = get_node_names_with_its_corresponding_node_labels()
    qualified_nodes = list()
    for node_id, node_labels in node_with_its_node_labels.items():
        if does_node_qualify_inclusion_labels(
            volume_node_topology_inclusion_labels, node_labels
        ):
            qualified_nodes.append(node_id)
    return len(qualified_nodes)


# Return the node names with its corresponding node labels.
def get_node_names_with_its_corresponding_node_labels():
    """Return the pool names with the corresponding node labels."""
    nodes_with_its_corresponding_node_labels = {}
    nodes = ApiClient.nodes_api().get_nodes()
    for node in nodes:
        nodes_with_its_corresponding_node_labels[node["id"]] = node["spec"]["labels"]
    return nodes_with_its_corresponding_node_labels


# Return whether the node qualifies based on the volume node topology inclusion labels.
def does_node_qualify_inclusion_labels(
    volume_node_topology_inclusion_labels, node_with_its_node_labels
):
    """Return whether the node qualifies based on the volume node topology inclusion labels."""
    inc_match = True
    for key, value in volume_node_topology_inclusion_labels.items():
        if key in node_with_its_node_labels:
            if value == "":
                inc_match = True
                break
            if (
                volume_node_topology_inclusion_labels[key]
                != node_with_its_node_labels[key]
            ):
                inc_match = False
                break
        else:
            inc_match = False
    return inc_match


# Return the expected volume spec based on the input parameters.
def expected_volume_spec(replica, toplogy):
    """Return the expected volume spec."""
    return VolumeSpec(
        int(replica),
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        0,
        topology=toplogy,
    )
