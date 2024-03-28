"""Label a node, this will be used while scheduling replica of volume considering the feature tests."""

import docker
import pytest
import sys
import http

from pytest_bdd import given, scenario, then, when
from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker
from common.etcd import Etcd
from common.operations import Cluster, wait_node_online
from openapi.model.node import Node
from openapi.exceptions import ApiException

NUM_IO_ENGINES = 2
LABEL1 = "KEY1=VALUE1"
LABEL1_NEW = "KEY1=NEW_LABEL"
LABEL1_MAP = "{'KEY1': 'VALUE1'}"
LABEL2 = "KEY2=VALUE2"
LABEL_1_NEW = "KEY1=NEW_LABEL"
LABEL_KEY_TO_DELETE = "KEY1"
LABEL_KEY_TO_DELETE_ABSENT = "ABSENT_KEY"
NODE_ID_1 = "io-engine-1"


# Fixtures
@pytest.fixture(scope="module")
def init():
    Deployer.start(NUM_IO_ENGINES, io_engine_env="MAYASTOR_HB_INTERVAL_SEC=0")
    yield
    Deployer.stop()


@pytest.fixture(scope="function")
def context():
    return {}


@pytest.fixture(scope="function")
def node(context):
    yield context["node"]


@scenario("feature.feature", "Label a node")
def test_label_a_node():
    """Label a node."""


@scenario(
    "feature.feature",
    "Label a node when label key already exist and overwrite is false",
)
def test_label_a_node_when_label_key_already_exist_and_overwrite_is_false():
    """Label a node when label key already exist and overwrite is false."""


@scenario(
    "feature.feature", "Label a node when label key already exist and overwrite is true"
)
def test_label_a_node_when_label_key_already_exist_and_overwrite_is_true():
    """Label a node when label key already exist and overwrite is true."""


@scenario("feature.feature", "Unlabel a node")
def test_unlabel_a_node():
    """Unlabel a node."""


@scenario("feature.feature", "Unlabel a node when the label key is not present")
def test_unlabel_a_node_when_the_label_key_is_not_present():
    """Unlabel a node when the label key is not present."""


@given("a control plane, two Io-Engine instances, two pools")
def a_control_plane_two_ioengine_instances_two_pools(init):
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

    # Check for a nodes
    nodes = ApiClient.nodes_api().get_nodes()
    assert len(nodes) == 2
    yield
    for node in ApiClient.nodes_api().get_nodes():
        if hasattr(node.spec, "labels"):
            for label in node.spec.labels.keys():
                ApiClient.nodes_api().delete_node_label(node.id, label)


@given("an unlabeled node")
def an_unlabeled_node():
    """an unlabeled node."""
    node = ApiClient.nodes_api().get_node(NODE_ID_1)
    assert not "labels" in node.spec


@given("a labeled node")
def a_labeled_node(a_node_labeled_1):
    """a labeled node."""


@when("the user issues a label command with a label to the node")
def the_user_issues_a_label_command_with_a_label_to_the_node(attempt_add_label_one):
    """the user issues a label command with a label to the node."""


@when(
    "the user attempts to label the same node with the same label key with overwrite as false"
)
def the_user_attempts_to_label_the_same_node_with_the_same_label_key_with_overwrite_as_false(
    context, node
):
    """the user attempts to label the same node with the same label key with overwrite as false."""
    attempt_add_label(node.id, LABEL1_NEW, False, context)


@when(
    "the user attempts to label the same node with the same label key and overwrite as true"
)
def the_user_attempts_to_label_the_same_node_with_the_same_label_key_and_overwrite_as_true(
    context, node
):
    """the user attempts to label the same node with the same label key and overwrite as true."""
    attempt_add_label(node.id, LABEL1_NEW, True, context)


@when(
    "the user issues a unlabel command with a label key present as label for the node"
)
def the_user_issues_a_unlabel_command_with_a_label_key_present_as_label_for_the_node(
    context, node
):
    """the user issues a unlabel command with a label key present as label for the node."""


@when(
    "the user issues an unlabel command with a label key that is not currently associated with the node"
)
def the_user_issues_an_unlabel_command_with_a_label_key_that_is_not_currently_associated_with_the_node(
    context, node
):
    """the user issues an unlabel command with a label key that is not currently associated with the node."""
    attempt_delete_label(node.id, LABEL_KEY_TO_DELETE_ABSENT, context)


@then("the given node should be labeled with the given label")
def the_given_node_should_be_labeled_with_the_given_label(
    attempt_add_label_one, context
):
    """the given node should be labeled with the given label."""
    labelling_succeeds(attempt_add_label_one, context)


@then('the node label should fail with error "PRECONDITION_FAILED"')
def the_node_label_should_fail_with_error_precondition_failed(node_attempt):
    """the node label should fail with error "PRECONDITION_FAILED"."""
    assert node_attempt.status == http.HTTPStatus.PRECONDITION_FAILED
    assert ApiClient.exception_to_error(node_attempt).kind == "FailedPrecondition"


@then("the given node should be labeled with the new given label")
def the_given_node_should_be_labeled_with_the_new_given_label(
    attempt_add_label_one_with_overwrite, context
):
    """the given node should be labeled with the new given label."""
    labelling_succeeds(attempt_add_label_one_with_overwrite, context)


@then("the given node should remove the label with the given key")
def the_given_node_should_remove_the_label_with_the_given_key(
    attempt_delete_label_of_node, context
):
    """the given node should remove the label with the given key."""
    labelling_succeeds(attempt_delete_label_of_node, context)


@then("the unlabel operation for the node should fail with error PRECONDITION_FAILED")
def the_unlabel_operation_for_the_node_should_fail_with_error_precondition_failed(
    node_attempt,
):
    """the unlabel operation for the node should fail with error PRECONDITION_FAILED."""
    assert node_attempt.status == http.HTTPStatus.PRECONDITION_FAILED
    assert ApiClient.exception_to_error(node_attempt).kind == "FailedPrecondition"


@pytest.fixture(scope="function")
def node_attempt(context):
    return context["attempt_result"]


@pytest.fixture
def a_node_labeled_1(context):
    node = attempt_add_label(NODE_ID_1, LABEL1, False, context)
    labelling_succeeds(node, context)
    yield node


@pytest.fixture
def attempt_add_label_one(context):
    yield attempt_add_label(NODE_ID_1, LABEL1, False, context)


@pytest.fixture
def attempt_add_label_one_with_overwrite(context):
    yield attempt_add_label(NODE_ID_1, LABEL1_NEW, True, context)


def attempt_add_label(node_name, label, overwrite, context):
    try:
        [key, value] = label.split("=")
        overwrite = "true" if overwrite else "false"
        node = ApiClient.nodes_api().put_node_label(
            node_name, key, value, overwrite=overwrite
        )
        context["node"] = node
        return node
    except ApiException as exception:
        context["attempt_result"] = exception
        return exception


@pytest.fixture
def attempt_delete_label_of_node(context):
    yield attempt_delete_label(NODE_ID_1, LABEL_KEY_TO_DELETE, context)


def attempt_delete_label(node_name, label, context):
    try:
        node = ApiClient.nodes_api().delete_node_label(node_name, label)
        context["node"] = node
        return node
    except ApiException as exception:
        context["attempt_result"] = exception
        return exception


def labelling_succeeds(result, context):
    # raise result for exception information
    assert isinstance(result, Node)
    ApiClient.nodes_api().get_node(result.id)
    context["node"] = result
