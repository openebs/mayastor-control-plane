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
from openapi.model.pool_topology import PoolTopology
from openapi.model.labelled_topology import LabelledTopology
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_spec import VolumeSpec



NODE_1_NAME = "io-engine-1"
NODE_2_NAME = "io-engine-2"
NODE_3_NAME = "io-engine-3"
POOL_1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
POOL_2_UUID = "91a60318-bcfe-4e36-92cb-ddc7abf212ea"
POOL_3_UUID = "4d471e62-ca17-44d1-a6d3-8820f6156c1a"
POOL_4_UUID = "609j67e9-kb19-65d3-a1d2-8820f61alpha"
VOLUME_UUID = "5cd5378e-3f05-47f1-a830-a0f5873a1449"
VOLUME_UUID_1 = "5cd5378e-3f05-47f1-a830-a0f5873a1441"
VOLUME_SIZE = 10485761
POOL_SIZE = 734003200
NUM_VOLUME_REPLICAS = 1
FAULTED_CHILD_WAIT_SECS = 2
FIO_RUN = 7
NUM_IO_ENGINES = 3
SLEEP_BEFORE_START = 1
CREATE_REQUEST_KEY = "create_request"


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
                "node": "io-engine-1",
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
                "node": "io-engine-2",
            },
        ),
    )
    ApiClient.pools_api().put_node_pool(
        NODE_3_NAME,
        POOL_3_UUID,
        CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "pool3-specific-key": "pool3-specific-value",
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-3",
            },
        ),
    )
    ApiClient.pools_api().put_node_pool(
        NODE_3_NAME,
        POOL_4_UUID,
        CreatePoolBody(
            ["malloc:///disk-alpha?size_mb=50"],
            labels={
                "pool3-specific-key": "pool3-specific-alpha",
                "openebs.io/created-by": "operator-diskpool",
                "node": "io-engine-3",
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

@scenario('pool-affinity-spread.feature', 'suitable pools which caters volume exclusion topology labels key with empty value')
def test_suitable_pools_which_caters_volume_exclusion_topology_labels_key_with_empty_value():
    """suitable pools which caters volume exclusion topology labels key with empty value."""

@scenario('pool-affinity-spread.feature', 'desired number of replicas cannot be created if topology inclusion labels and exclusion label as same')
def test_desired_number_of_replicas_cannot_be_created_if_topology_inclusion_labels_and_exclusion_label_as_same():
    """desired number of replicas cannot be created if topology inclusion labels and exclusion label as same."""

@scenario('pool-affinity-spread.feature', 'suitable pools which caters volume exclusion topology labels')
def test_suitable_pools_which_caters_volume_exclusion_topology_labels():
    """suitable pools which caters volume exclusion topology labels."""

@scenario('pool-affinity-spread.feature', 'desired number of replicas cannot be created')
def test_desired_number_of_replicas_cannot_be_created():
    """desired number of replicas cannot be created."""

@scenario('pool-affinity-spread.feature', 'suitable pools which contain volume topology labels')
def test_suitable_pools_which_contain_volume_topology_labels():
    """suitable pools which contain volume topology labels."""

@scenario('pool-affinity-spread.feature', 'suitable pools which contain volume topology labels key with empty value')
def test_suitable_pools_which_contain_volume_topology_labels_key_with_empty_value():
    """suitable pools which contain volume topology labels key with empty value."""


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
    assert len(pools) == 4

@given('a request for a volume with its topology exclusion labels key with empty value matching with any pools with labels')
def a_request_for_a_volume_with_its_topology_exclusion_labels_key_with_empty_value_matching_with_any_pools_with_labels(create_request):
    """a request for a volume with its topology exclusion labels key with empty value matching with any pools with labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={"pool3-specific-key": ""},
                    inclusion={"openebs.io/created-by": "operator-diskpool"},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request


@given('a request for a volume with its topology inclusion labels and exclusion label as same')
def a_request_for_a_volume_with_its_topology_inclusion_labels_and_exclusion_label_as_same(create_request):
    """a request for a volume with its topology inclusion labels and exclusion label as same."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={"pool1-specific-key": "pool1-specific-value"},
                    inclusion={"openebs.io/created-by": "operator-diskpool", "pool1-specific-key": "pool1-specific-value"},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request


@given('a request for a volume with its topology exclusion labels key matching with any pools with labels')
def a_request_for_a_volume_with_its_topology_exclusion_labels_key_matching_with_any_pools_with_labels(create_request):
    """a request for a volume with its topology exclusion labels key matching with any pools with labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={"pool3-specific-key": "pool3-specific-value"},
                    inclusion={"openebs.io/created-by": "operator-diskpool"},
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request


@given('a request for a volume with its topology inclusion labels not matching with any pools with labels')
def a_request_for_a_volume_with_its_topology_inclusion_labels_not_matching_with_any_pools_with_labels(create_request):
    """a request for a volume with its topology inclusion labels not matching with any pools with labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool",
                               "fake-label-key": "fake-label-value",
                               },
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request

@given('a request for a volume with its topology inclusion labels matching certain pools with same labels')
def a_request_for_a_volume_with_its_topology_inclusion_labels_matching_certain_pools_with_same_labels(create_request):
    """a request for a volume with its topology inclusion labels matching certain pools with same labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool",
                               "pool1-specific-key": "pool1-specific-value",
                               },
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request

@given('a request for a volume with its topology inclusion labels key with empty value matching certain pools with same labels')
def a_request_for_a_volume_with_its_topology_inclusion_labels_key_with_empty_value_matching_certain_pools_with_same_labels(create_request):
    """a request for a volume with its topology inclusion labels key with empty value matching certain pools with same labels."""
    request = CreateVolumeBody(
        VolumePolicy(False),
        NUM_VOLUME_REPLICAS,
        VOLUME_SIZE,
        False,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool",
                               "pool3-specific-key": "",
                               },
                )
            )
        ),
    )
    create_request[CREATE_REQUEST_KEY] = request



@when('the volume replicas has topology inclusion labels and exclusion labels same')
def the_volume_replicas_has_topology_inclusion_labels_and_exclusion_labels_same(create_request):
    """the volume replicas has topology inclusion labels and exclusion labels same."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    no_of_pools = no_of_suitable_pools(
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "inclusion"
        ],
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "exclusion"
        ]
    )
    assert num_volume_replicas > no_of_pools

@when('the volume replicas has topology exclusion labels key with empty value not matching with any pools with labels key')
def the_volume_replicas_has_topology_exclusion_labels_key_with_empty_value_not_matching_with_any_pools_with_labels_key(create_request):
    """the volume replicas has topology exclusion labels key with empty value not matching with any pools with labels key."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    if (
        hasattr(create_request[CREATE_REQUEST_KEY], "topology")
        and "pool_topology" in create_request[CREATE_REQUEST_KEY]["topology"]
    ):
        no_of_pools = no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ], 
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "exclusion"
            ]
        )
    else:
        # Here we are fetching all pools and comparing its length, because if we reach this part of code
        # it signifies the volume request has no pool topology labels, thus all pools are suitable
        no_of_pools = len(ApiClient.pools_api().get_pools())
    assert num_volume_replicas < no_of_pools

@when('the volume replicas has topology exclusion labels key matching with any pools with labels key and not value')
def the_volume_replicas_has_topology_exclusion_labels_key_matching_with_any_pools_with_labels_key_and_not_value(create_request):
    """the volume replicas has topology exclusion labels key matching with any pools with labels key and not value."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    if (
        hasattr(create_request[CREATE_REQUEST_KEY], "topology")
        and "pool_topology" in create_request[CREATE_REQUEST_KEY]["topology"]
    ):
        no_of_pools = no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ], 
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "exclusion"
            ]
        )
    else:
        # Here we are fetching all pools and comparing its length, because if we reach this part of code
        # it signifies the volume request has no pool topology labels, thus all pools are suitable
        no_of_pools = len(ApiClient.pools_api().get_pools())
    assert num_volume_replicas == no_of_pools


@when('the volume replicas has topology inclusion labels not matching with any pools with labels')
def the_volume_replicas_has_topology_inclusion_labels_not_matching_with_any_pools_with_labels(create_request):
    """the volume replicas has topology inclusion labels not matching with any pools with labels."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    no_of_pools = no_of_suitable_pools(
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "inclusion"
        ], 
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "exclusion"
        ]
    )
    assert num_volume_replicas > no_of_pools


@when('the volume replicas has topology inclusion labels key with empty value matching to the labels as that of certain pools')
def the_volume_replicas_has_topology_inclusion_labels_key_with_empty_value_matching_to_the_labels_as_that_of_certain_pools(create_request):
    """the volume replicas has topology inclusion labels key with empty value matching to the labels as that of certain pools."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    if (
        hasattr(create_request[CREATE_REQUEST_KEY], "topology")
        and "pool_topology" in create_request[CREATE_REQUEST_KEY]["topology"]
    ):
        no_of_pools = no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ], 
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "exclusion"
            ]
        )
    else:
        # Here we are fetching all pools and comparing its length, because if we reach this part of code
        # it signifies the volume request has no pool topology labels, thus all pools are suitable
        no_of_pools = len(ApiClient.pools_api().get_pools())
    assert num_volume_replicas < no_of_pools


@when('the volume replicas has topology inclusion labels matching to the labels as that of certain pools')
def the_volume_replicas_has_topology_inclusion_labels_matching_to_the_labels_as_that_of_certain_pools(create_request):
    """the volume replicas has topology inclusion labels matching to the labels as that of certain pools."""
    num_volume_replicas = create_request[CREATE_REQUEST_KEY]["replicas"]
    if (
        hasattr(create_request[CREATE_REQUEST_KEY], "topology")
        and "pool_topology" in create_request[CREATE_REQUEST_KEY]["topology"]
    ):
        no_of_pools = no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ], 
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "exclusion"
            ]
        )
    else:
        # Here we are fetching all pools and comparing its length, because if we reach this part of code
        # it signifies the volume request has no pool topology labels, thus all pools are suitable
        no_of_pools = len(ApiClient.pools_api().get_pools())
    assert num_volume_replicas == no_of_pools


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


@then('pool labels must contain all the volume request topology labels')
def pool_labels_must_contain_all_the_volume_request_topology_labels(create_request):
    """pool labels must contain all the volume request topology labels."""
    assert (
        common_labels(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ],
            ApiClient.pools_api().get_pool(POOL_1_UUID),
        )
    ) == len(
        create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
            "inclusion"
        ]
    )

@then('volume creation should fail with an insufficient storage error')
def volume_creation_should_fail_with_an_insufficient_storage_error(create_request):
    """volume creation should fail with an insufficient storage error."""
    request = create_request[CREATE_REQUEST_KEY]
    try:
        ApiClient.volumes_api().put_volume(VOLUME_UUID_1, request)
    except Exception as e:
        exception_info = e.__dict__
        assert exception_info["status"] == requests.codes["insufficient_storage"]

    # Check that the volume wasn't created.
    volumes = ApiClient.volumes_api().get_volumes().entries
    assert len(volumes) == 0



@then('pool labels must not contain the volume request topology labels')
def pool_labels_must_not_contain_the_volume_request_topology_labels(create_request):
    """pool labels must not contain the volume request topology labels."""
    assert (
        no_of_suitable_pools(
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "inclusion"
            ],
            create_request[CREATE_REQUEST_KEY]["topology"]["pool_topology"]["labelled"][
                "exclusion"
            ]
        )
    ) == 0


@then('volume creation should succeed with a returned volume object with topology')
def volume_creation_should_succeed_with_a_returned_volume_object_with_topology(create_request):
    """volume creation should succeed with a returned volume object with topology."""
    expected_spec = VolumeSpec(
        1,
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        0,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool", 
                               "pool1-specific-key": "pool1-specific-value"
                               },
                )
            )
        ),
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state["status"]) == "Online"
    for key, value in volume.state.replica_topology.items():
            assert str( value["pool"] == POOL_1_UUID)

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
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={"pool3-specific-key": "pool3-specific-value"},
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
    for key, value in volume.state.replica_topology.items():
            assert str( value["pool"] == POOL_4_UUID)


@then('volume creation should succeed with a returned volume object with topology label key with empty value')
def volume_creation_should_succeed_with_a_returned_volume_object_with_topology_label_key_with_empty_value(create_request):
    """volume creation should succeed with a returned volume object with topology label key with empty value."""
    expected_spec = VolumeSpec(
        1,
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        0,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={},
                    inclusion={"openebs.io/created-by": "operator-diskpool", "pool3-specific-key": ""},
                )
            )
        ),
    )

    # Check the volume object returned is as expected
    request = create_request[CREATE_REQUEST_KEY]
    volume = ApiClient.volumes_api().put_volume(VOLUME_UUID, request)
    assert str(volume.spec) == str(expected_spec)
    assert str(volume.state["status"]) == "Online"
    for key, value in volume.state.replica_topology.items():
            assert str( value["pool"] == POOL_4_UUID) or str( value["pool"] == POOL_3_UUID)


@then('volume creation should succeed with a returned volume object with exclusion topology key different as pool key')
def volume_creation_should_succeed_with_a_returned_volume_object_with_exclusion_topology_key_different_as_pool_key(create_request):
    """volume creation should succeed with a returned volume object with exclusion topology key different as pool key."""
    expected_spec = VolumeSpec(
        1,
        VOLUME_SIZE,
        SpecStatus("Created"),
        VOLUME_UUID,
        VolumePolicy(False),
        False,
        0,
        topology=Topology(
            pool_topology=PoolTopology(
                labelled=LabelledTopology(
                    exclusion={"pool3-specific-key": ""},
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
    for key, value in volume.state.replica_topology.items():
            assert str( value["pool"] == POOL_1_UUID) or str( value["pool"] == POOL_2_UUID)

def no_of_suitable_pools(volume_pool_topology_inclusion_labels, volume_pool_topology_exclusion_labels):
    pool_with_labels = {
        POOL_1_UUID: ApiClient.pools_api().get_pool(POOL_1_UUID)["spec"]["labels"],
        POOL_2_UUID: ApiClient.pools_api().get_pool(POOL_2_UUID)["spec"]["labels"],
        POOL_3_UUID: ApiClient.pools_api().get_pool(POOL_3_UUID)["spec"]["labels"],
        POOL_4_UUID: ApiClient.pools_api().get_pool(POOL_4_UUID)["spec"]["labels"],
    }
    qualified_pools = list()
    for pool_id, labels in pool_with_labels.items():
        if does_pool_qualify_inclusion_labels(volume_pool_topology_inclusion_labels, labels ) and does_pool_qualify_exclusion_labels(volume_pool_topology_exclusion_labels, labels ):
            qualified_pools.append(pool_id)
    return  len(qualified_pools) 


def does_pool_qualify_inclusion_labels(volume_pool_topology_inclusion_labels, pool_labels):
    inc_match = True
    for key, value in volume_pool_topology_inclusion_labels.items():
        if key in pool_labels:
            if value == "":
                inc_match = True
                break
            if volume_pool_topology_inclusion_labels[key] != pool_labels[key]:
                inc_match = False
                break
        else:
           inc_match = False
    return inc_match

def does_pool_qualify_exclusion_labels(volume_pool_topology_exclusion_labels, pool_labels):
    if len(volume_pool_topology_exclusion_labels) == 0:
        return True
    for key in volume_pool_topology_exclusion_labels:
        if key in pool_labels:
            if volume_pool_topology_exclusion_labels[key] == pool_labels[key]:
                return False
        else:
            return False
    return True

def common_labels(volume_pool_topology_labels, pool):
    pool_labels = pool["spec"]["labels"]
    count = 0
    for key in volume_pool_topology_labels:
        if key in pool_labels and volume_pool_topology_labels[key] == pool_labels[key]:
            count += 1
    return count
