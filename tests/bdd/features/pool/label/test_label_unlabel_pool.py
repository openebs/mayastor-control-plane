"""Label a pool, this will be used while scheduling replica of volume considering the feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)


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
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.pool import Pool


NUM_IO_ENGINES = 2
LABEL1 = "KEY1=VALUE1"
LABEL1_NEW = "KEY1=NEW_LABEL"
LABEL2_NEW = "KEY2=NEW_LABEL"
LABEL1_MAP = "{'KEY1': 'VALUE1'}"
LABEL2 = "KEY2=VALUE2"
LABEL_1_NEW = "KEY1=NEW_LABEL"
LABEL_KEY_TO_DELETE = "KEY2"
LABEL_KEY_TO_DELETE_ABSENT = "ABSENT_KEY"


VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 1
CREATE_REQUEST_KEY = "create_request"
POOL_1_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_1_NAME = "io-engine-1"
POOL_2_UUID = "24d36c1a-3e6c-4e05-893d-917ec9f4c1bb"
NODE_2_NAME = "io-engine-2"
REPLICA_CONTEXT_KEY = "replica"
REPLICA_ERROR = "replica_error"


@pytest.fixture(scope="module")
def init():
    Deployer.start(NUM_IO_ENGINES, io_engine_env="MAYASTOR_HB_INTERVAL_SEC=0")
    ApiClient.pools_api().put_node_pool(
        NODE_1_NAME,
        POOL_1_UUID,
        CreatePoolBody(["malloc:///disk?size_mb=50"]),
    )
    ApiClient.pools_api().put_node_pool(
        NODE_2_NAME,
        POOL_2_UUID,
        CreatePoolBody(
            ["malloc:///disk?size_mb=50"],
            labels={
                "KEY2": "VALUE2",
            },
        ),
    )
    yield
    Deployer.stop()


@pytest.fixture(scope="function")
def context():
    return {}


@pytest.fixture(scope="function")
def pool(context):
    yield context["pool"]


@scenario("pool-label.feature", "Label a pool")
def test_label_a_pool():
    """Label a pool."""


@scenario(
    "pool-label.feature",
    "Label a pool when label key already exist and overwrite is false",
)
def test_label_a_pool_when_label_key_already_exist_and_overwrite_is_false():
    """Label a pool when label key already exist and overwrite is false."""


@scenario(
    "pool-label.feature",
    "Label a pool when label key already exist and overwrite is true",
)
def test_label_a_pool_when_label_key_already_exist_and_overwrite_is_true():
    """Label a pool when label key already exist and overwrite is true."""


@scenario("pool-label.feature", "Unlabel a pool")
def test_unlabel_a_pool():
    """Unlabel a pool."""


@scenario("pool-label.feature", "Unlabel a pool when the label key is not present")
def test_unlabel_a_pool_when_the_label_key_is_not_present():
    """Unlabel a pool when the label key is not present."""


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

    # Check for a pools
    pools = ApiClient.pools_api().get_pools()
    assert len(pools) == 2
    Cluster.cleanup(pools=False)


@given("an unlabeled pool")
def an_unlabeled_pool():
    """an unlabeled pool."""
    pool = ApiClient.pools_api().get_pool(POOL_1_UUID)
    assert not "labels" in pool.spec


@given("a labeled pool")
def a_labeled_pool(context):
    """a labeled pool."""
    pool = ApiClient.pools_api().get_pool(POOL_2_UUID)
    assert "labels" in pool.spec
    context["pool"] = pool


@when("the user issues a label command with a label to the pool")
def the_user_issues_a_label_command_with_a_label_to_the_pool(attempt_add_label_pool):
    """the user issues a label command with a label to the pool."""


@when(
    "the user attempts to label the same pool with the same label key with overwrite as false"
)
def the_user_attempts_to_label_the_same_pool_with_the_same_label_key_with_overwrite_as_false(
    context, pool
):
    """the user attempts to label the same pool with the same label key with overwrite as false."""
    attempt_add_label(pool.id, LABEL2_NEW, False, context)


@when(
    "the user attempts to label the same pool with the same label key and overwrite as true"
)
def the_user_attempts_to_label_the_same_pool_with_the_same_label_key_and_overwrite_as_true(
    context, pool
):
    """the user attempts to label the same pool with the same label key and overwrite as true."""
    attempt_add_label(pool.id, LABEL2_NEW, True, context)


@when(
    "the user issues a unlabel command with a label key present as label for the pool"
)
def the_user_issues_a_unlabel_command_with_a_label_key_present_as_label_for_the_pool(
    context, pool
):
    """the user issues a unlabel command with a label key present as label for the pool."""


@when(
    "the user issues an unlabel command with a label key that is not currently associated with the pool"
)
def the_user_issues_an_unlabel_command_with_a_label_key_that_is_not_currently_associated_with_the_pool(
    context, pool
):
    """the user issues an unlabel command with a label key that is not currently associated with the pool."""
    attempt_delete_label(pool.id, LABEL_KEY_TO_DELETE_ABSENT, context)


@then("the given pool should be labeled with the given label")
def the_given_pool_should_be_labeled_with_the_given_label(
    attempt_add_label_pool, context
):
    """the given pool should be labeled with the given label."""
    labelling_succeeds(attempt_add_label_pool, context)


@then('the pool label should fail with error "PRECONDITION_FAILED"')
def the_pool_label_should_fail_with_error_precondition_failed(pool_attempt):
    """the pool label should fail with error "PRECONDITION_FAILED"."""
    assert pool_attempt.status == http.HTTPStatus.PRECONDITION_FAILED
    assert ApiClient.exception_to_error(pool_attempt).kind == "FailedPrecondition"


@then("the given pool should be labeled with the new given label")
def the_given_pool_should_be_labeled_with_the_new_given_label(
    attempt_add_label_one_with_overwrite, context
):
    """the given pool should be labeled with the new given label."""
    labelling_succeeds(attempt_add_label_one_with_overwrite, context)


@then("the given pool should remove the label with the given key")
def the_given_pool_should_remove_the_label_with_the_given_key(
    attempt_delete_label_of_pool, context
):
    """the given pool should remove the label with the given key."""
    labelling_succeeds(attempt_delete_label_of_pool, context)


@then("the unlabel operation for the pool should fail with error PRECONDITION_FAILED")
def the_unlabel_operation_for_the_pool_should_fail_with_error_precondition_failed(
    pool_attempt,
):
    """the unlabel operation for the pool should fail with error PRECONDITION_FAILED."""
    assert pool_attempt.status == http.HTTPStatus.PRECONDITION_FAILED
    assert ApiClient.exception_to_error(pool_attempt).kind == "FailedPrecondition"


@pytest.fixture(scope="function")
def pool_attempt(context):
    return context["attempt_result"]


@pytest.fixture
def attempt_add_label_pool(context):
    yield attempt_add_label(POOL_1_UUID, LABEL1, False, context)


@pytest.fixture
def attempt_add_label_one_with_overwrite(context):
    yield attempt_add_label(POOL_2_UUID, LABEL2_NEW, True, context)


@pytest.fixture
def attempt_delete_label_of_pool(context):
    yield attempt_delete_label(POOL_2_UUID, LABEL_KEY_TO_DELETE, context)


def attempt_add_label(pool_name, label, overwrite, context):
    try:
        [key, value] = label.split("=")
        overwrite = "true" if overwrite else "false"
        pool = ApiClient.pools_api().put_pool_label(
            pool_name, key, value, overwrite=overwrite
        )
        context["pool"] = pool
        return pool
    except ApiException as exception:
        context["attempt_result"] = exception
        return exception


def attempt_delete_label(pool_name, label, context):
    try:
        pool = ApiClient.pools_api().del_pool_label(pool_name, label)
        context["pool"] = pool
        return pool
    except ApiException as exception:
        context["attempt_result"] = exception
        return exception


def labelling_succeeds(result, context):
    # raise result for exception information
    assert isinstance(result, Pool)
    ApiClient.pools_api().get_pool(result.id)
    context["pool"] = result
