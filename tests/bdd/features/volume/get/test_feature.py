"""Issuing GET requests for volumes feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import docker
import requests

from common.deployer import Deployer
from common.apiclient import ApiClient
from common.docker import Docker

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.volume_spec import VolumeSpec
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_state import VolumeState
from openapi.model.volume_status import VolumeStatus
from openapi.model.volume_policy import VolumePolicy
from openapi.model.replica_state import ReplicaState
from openapi.model.replica_topology import ReplicaTopology

VOLUMES = [
    "00000000-0000-0000-0000-000000000001",
    "00000000-0000-0000-0000-000000000002",
    "00000000-0000-0000-0000-000000000003",
    "00000000-0000-0000-0000-000000000004",
    "00000000-0000-0000-0000-000000000005",
]
VOLUME_SIZE = 10485761
NUM_VOLUME_REPLICAS = 1
NUM_VOLUMES_KEY = "num_volumes"
VOLUME_RESULTS_KEY = "volume_results"
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME = "io-engine-1"
PAGINATED_MAX_ENTRIES_KEY = "max_entries"
PAGINATED_STARTING_TOKEN = "starting_token"

# This fixture will be automatically used by all tests.
# It starts the deployer which launches all the necessary containers.
# A pool is created for convenience such that it is available for use by the tests.
@pytest.fixture(autouse=True, scope="module")
def init(num_volumes):
    Deployer.start(1)
    ApiClient.pools_api().put_node_pool(
        NODE_NAME, POOL_UUID, CreatePoolBody(["malloc:///disk?size_mb=100"])
    )

    # Create the desired number of volumes
    for volume_uuid in VOLUMES:
        ApiClient.volumes_api().put_volume(
            volume_uuid,
            CreateVolumeBody(
                VolumePolicy(False), NUM_VOLUME_REPLICAS, VOLUME_SIZE, False
            ),
        )
    num_volumes[NUM_VOLUMES_KEY] = len(ApiClient.volumes_api().get_volumes().entries)

    yield
    Deployer.stop()


@pytest.fixture(scope="module")
def num_volumes():
    return {}


@pytest.fixture(scope="function")
def volume_results():
    return {}


@pytest.fixture(scope="function")
def paginated_variables():
    return {}


@scenario(
    "feature.feature",
    "issuing a GET request with pagination max entries larger than number of volumes",
)
def test_issuing_a_get_request_with_pagination_max_entries_larger_than_number_of_volumes():
    """issuing a GET request with pagination max entries larger than number of volumes."""


@scenario(
    "feature.feature",
    "issuing a GET request with pagination starting token set to greater than the number of volumes",
)
def test_issuing_a_get_request_with_pagination_starting_token_set_to_greater_than_the_number_of_volumes():
    """issuing a GET request with pagination starting token set to greater than the number of volumes."""


@scenario(
    "feature.feature",
    "issuing a GET request with pagination starting token set to less than the number of volumes",
)
def test_issuing_a_get_request_with_pagination_starting_token_set_to_less_than_the_number_of_volumes():
    """issuing a GET request with pagination starting token set to less than the number of volumes."""


@scenario(
    "feature.feature", "issuing a GET request with pagination variables within range"
)
def test_issuing_a_get_request_with_pagination_variables_within_range():
    """issuing a GET request with pagination variables within range."""


@scenario("feature.feature", "issuing a GET request without pagination")
def test_issuing_a_get_request_without_pagination():
    """issuing a GET request without pagination."""


@given("five existing volumes")
def five_existing_volumes(num_volumes):
    """five existing volumes."""
    assert num_volumes[NUM_VOLUMES_KEY] == len(VOLUMES)


@given("a number of volumes greater than one")
def a_number_of_volumes_greater_than_one(num_volumes):
    """a number of volumes greater than one."""
    assert num_volumes[NUM_VOLUMES_KEY] > 1


@when("a user issues a GET request with max entries greater than the number of volumes")
def a_user_issues_a_get_request_with_max_entries_greater_than_the_number_of_volumes(
    paginated_variables, num_volumes, volume_results
):
    """a user issues a GET request with max entries greater than the number of volumes."""
    paginated_variables[PAGINATED_MAX_ENTRIES_KEY] = 10
    paginated_variables[PAGINATED_STARTING_TOKEN] = 0
    assert paginated_variables[PAGINATED_MAX_ENTRIES_KEY] > num_volumes[NUM_VOLUMES_KEY]
    volume_results[VOLUME_RESULTS_KEY] = ApiClient.volumes_api().get_volumes(
        max_entries=paginated_variables[PAGINATED_MAX_ENTRIES_KEY],
        starting_token=paginated_variables[PAGINATED_STARTING_TOKEN],
    )


@when("a user issues a GET request with pagination")
def a_user_issues_a_get_request_with_pagination(paginated_variables, volume_results):
    """a user issues a GET request with pagination."""
    paginated_variables[PAGINATED_MAX_ENTRIES_KEY] = 2
    paginated_variables[PAGINATED_STARTING_TOKEN] = 2
    volume_results[VOLUME_RESULTS_KEY] = ApiClient.volumes_api().get_volumes(
        max_entries=paginated_variables[PAGINATED_MAX_ENTRIES_KEY],
        starting_token=paginated_variables[PAGINATED_STARTING_TOKEN],
    )


@when(
    "a user issues a GET request with the starting token set to a value greater than the number of volumes"
)
def a_user_issues_a_get_request_with_the_starting_token_set_to_a_value_greater_than_the_number_of_volumes(
    paginated_variables, volume_results, num_volumes
):
    """a user issues a GET request with the starting token set to a value greater than the number of volumes."""
    paginated_variables[PAGINATED_MAX_ENTRIES_KEY] = 2
    paginated_variables[PAGINATED_STARTING_TOKEN] = 10
    assert paginated_variables[PAGINATED_STARTING_TOKEN] > num_volumes[NUM_VOLUMES_KEY]
    volume_results[VOLUME_RESULTS_KEY] = ApiClient.volumes_api().get_volumes(
        max_entries=paginated_variables[PAGINATED_MAX_ENTRIES_KEY],
        starting_token=paginated_variables[PAGINATED_STARTING_TOKEN],
    )


@when(
    "a user issues a GET request with the starting token set to a value less than the number of volumes"
)
def a_user_issues_a_get_request_with_the_starting_token_set_to_a_value_less_than_the_number_of_volumes(
    paginated_variables, volume_results, num_volumes
):
    """a user issues a GET request with the starting token set to a value less than the number of volumes."""
    paginated_variables[PAGINATED_MAX_ENTRIES_KEY] = 2
    paginated_variables[PAGINATED_STARTING_TOKEN] = 2
    assert paginated_variables[PAGINATED_STARTING_TOKEN] < num_volumes[NUM_VOLUMES_KEY]
    volume_results[VOLUME_RESULTS_KEY] = ApiClient.volumes_api().get_volumes(
        max_entries=paginated_variables[PAGINATED_MAX_ENTRIES_KEY],
        starting_token=paginated_variables[PAGINATED_STARTING_TOKEN],
    )


@when("a user issues a GET request without pagination")
def a_user_issues_a_get_request_without_pagination(volume_results):
    """a user issues a GET request without pagination."""
    volume_results[VOLUME_RESULTS_KEY] = ApiClient.volumes_api().get_volumes()


@then("all of the volumes should be returned")
def all_of_the_volumes_should_be_returned(volume_results, num_volumes):
    """all of the volumes should be returned."""
    assert (
        len(volume_results[VOLUME_RESULTS_KEY].entries) == num_volumes[NUM_VOLUMES_KEY]
    )


@then("an empty list should be returned")
def an_empty_list_should_be_returned(volume_results):
    """an empty list should be returned."""
    assert len(volume_results[VOLUME_RESULTS_KEY].entries) == 0


@then("only a subset of the volumes should be returned")
def only_a_subset_of_the_volumes_should_be_returned(volume_results, num_volumes):
    """only a subset of the volumes should be returned."""
    assert (
        len(volume_results[VOLUME_RESULTS_KEY].entries) < num_volumes[NUM_VOLUMES_KEY]
    )


@then("the first result back should be for the given starting token offset")
def the_first_result_back_should_be_for_the_given_starting_token_offset(
    paginated_variables, volume_results
):
    """the first result back should be for the given starting token offset."""
    first_volume_uuid = volume_results[VOLUME_RESULTS_KEY].entries[0].spec.uuid
    assert first_volume_uuid == VOLUMES[paginated_variables[PAGINATED_STARTING_TOKEN]]


@then("the next token should be returned")
def the_next_token_should_be_returned(paginated_variables, volume_results):
    """the next token should be returned."""
    assert hasattr(volume_results[VOLUME_RESULTS_KEY], "next_token")
    expected_next_token = (
        paginated_variables[PAGINATED_STARTING_TOKEN]
        + paginated_variables[PAGINATED_MAX_ENTRIES_KEY]
    )
    assert volume_results[VOLUME_RESULTS_KEY].next_token == expected_next_token


@then("the next token should not be returned")
def the_next_token_should_not_be_returned(volume_results):
    """the next token should not be returned."""
    assert not hasattr(volume_results[VOLUME_RESULTS_KEY], "next_token")


@then("the number of returned volumes should equal the paginated request")
def the_number_of_returned_volumes_should_equal_the_paginated_request(
    volume_results, paginated_variables
):
    """the number of returned volumes should equal the paginated request."""
    assert (
        len(volume_results[VOLUME_RESULTS_KEY].entries)
        == paginated_variables[PAGINATED_MAX_ENTRIES_KEY]
    )
