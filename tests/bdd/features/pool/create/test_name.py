"""Pool creation feature tests."""
import http

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest

from common.deployer import Deployer
from common.apiclient import ApiClient
from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.pool import Pool
from openapi.exceptions import ApiException
from openapi.exceptions import NotFoundException


@scenario(
    "name.feature",
    "creating a pool whose name is already used by an existing pool on a different node",
)
def test_creating_a_pool_whose_name_is_already_used_by_an_existing_pool_on_a_different_node():
    """creating a pool whose name is already used by an existing pool on a different node."""


@scenario(
    "name.feature",
    "creating a pool whose name is already used by an existing pool on the same node",
)
def test_creating_a_pool_whose_name_is_already_used_by_an_existing_pool_on_the_same_node():
    """creating a pool whose name is already used by an existing pool on the same node."""


@scenario("name.feature", "creating a pool with a valid name")
def test_creating_a_pool_with_a_valid_name():
    """creating a pool with a valid name."""


@scenario("name.feature", "recreating a pool with a different name and the same disk")
def test_recreating_a_pool_with_a_different_name_and_the_same_disk():
    """recreating a pool with a different name and the same disk."""


@scenario(
    "name.feature",
    "recreating an unexported pool with a different name and the same disk",
)
def test_recreating_an_unexported_pool_with_a_different_name_and_the_same_disk():
    """recreating a pool with a different name and the same disk."""


@given("a control plane and Io-Engine instances")
def a_control_plane_and_io_engine_instances(background):
    """a control plane and Io-Engine instances."""


@given('a pool named "p0"')
def a_pool_named_p0(a_pool_named_p0):
    """a pool "p0"."""


@given('an unexported pool named "p0"')
def an_unexported_pool_named_p0(an_unexported_pool_named_p0):
    """an unexported pool named "p0"."""


@when('the user attempts to create a pool with a valid name "p0"')
def the_user_attempts_to_create_a_pool_with_a_valid_name_p0(
    attempt_create_valid_pool_p0,
):
    """the user attempts to create a pool with a valid name "p0"."""


@when("the user attempts to create another pool with the same name on the same node")
def the_user_attempts_to_create_another_pool_with_the_same_name_on_the_same_node(
    context, pool
):
    """the user attempts to create another pool with the same name on the same node."""
    attempt_create_valid_pool(pool.id, pool.spec.node, context)


@when("the user attempts to create another pool with the same name on a different node")
def the_user_attempts_to_create_another_pool_with_the_same_name_on_a_different_node(
    context, pool
):
    """the user attempts to create another pool with the same name on a different node."""
    attempt_create_valid_pool(pool.id, different_node(pool.spec.node), context)


@when('the user attempts to recreate the pool using a different name "p1"')
def the_user_attempts_to_recreate_a_pool_using_a_different_name_p1(context, pool):
    """the user attempts to recreate a pool using a different name."""
    attempt_create_valid_pool(POOL_NAME_P1, pool.spec.node, context)


@then('the pool creation should fail with error "AlreadyExists"')
def the_pool_creation_should_fail_with_error_already_exists(pool_attempt):
    """the pool creation should fail with error "AlreadyExists"."""
    assert pool_attempt.status == http.HTTPStatus.UNPROCESSABLE_ENTITY
    assert ApiClient.exception_to_error(pool_attempt).kind == "AlreadyExists"


@then('the pool creation should fail with error "InvalidArgument"')
def the_pool_creation_should_fail_with_error_internal_error(pool_attempt):
    """the pool creation should fail with error InvalidArgument"."""
    assert pool_attempt.status == http.HTTPStatus.BAD_REQUEST
    assert ApiClient.exception_to_error(pool_attempt).kind == "InvalidArgument"


@then("the pool creation should succeed")
def the_pool_creation_should_succeed(context, attempt_create_valid_pool_p0):
    """the pool creation should succeed."""
    pool_creation_succeeds(attempt_create_valid_pool_p0, context)


"""" Implementations """

POOL_NODE = "io-engine-1"
POOL_NAME_P0 = "p0"
POOL_NAME_P1 = "p1"


@pytest.fixture(scope="module")
def background():
    Deployer.start(2)
    yield
    Deployer.stop()


@pytest.fixture(scope="function")
def context():
    return {}


@pytest.fixture(scope="function")
def pool_attempt(context):
    return context["attempt_result"]


@pytest.fixture(scope="function")
def pool(context):
    yield context["pool"]


@pytest.fixture
def attempt_create_valid_pool_p0(context):
    yield attempt_create_valid_pool(POOL_NAME_P0, POOL_NODE, context)
    try:
        ApiClient.pools_api().del_pool(POOL_NAME_P0)
    except NotFoundException:
        pass


def attempt_create_valid_pool(name, node, context):
    try:
        pool = ApiClient.pools_api().put_node_pool(
            node, name, CreatePoolBody(["malloc:///disk?size_mb=100"])
        )
        context["attempt_result"] = pool
        return pool
    except ApiException as exception:
        context["attempt_result"] = exception
        return exception


@pytest.fixture
def a_pool_named_p0(context):
    pool = attempt_create_valid_pool(POOL_NAME_P0, POOL_NODE, context)
    pool_creation_succeeds(pool, context)
    yield pool
    ApiClient.pools_api().del_pool(POOL_NAME_P0)


@pytest.fixture
def an_unexported_pool_named_p0(context):
    # todo: Unexport pool first, just in case that affects the return code from the io-engine
    pool = attempt_create_valid_pool(POOL_NAME_P0, POOL_NODE, context)
    pool_creation_succeeds(pool, context)
    yield pool
    ApiClient.pools_api().del_pool(POOL_NAME_P0)


def pool_creation_succeeds(result, context):
    # raise result for exception information
    assert isinstance(result, Pool)
    ApiClient.pools_api().get_pool(result.id)
    context["pool"] = result


def pool_creation_fails(result, expected_error_type):
    # raise result for exception information
    assert isinstance(result, expected_error_type)


def different_node(node_id):
    nodes = ApiClient.nodes_api().get_nodes()
    nodes = list(filter(lambda node: node.id != node_id, nodes))
    return nodes[0].id
