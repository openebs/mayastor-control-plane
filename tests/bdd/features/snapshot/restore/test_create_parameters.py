"""Create Volume From Snapshot - Parameter Validation feature tests."""
import http
import json

import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import uuid
import openapi
from common.deployer import Deployer
from common.apiclient import ApiClient

from openapi.model.create_pool_body import CreatePoolBody
from openapi.model.create_volume_body import CreateVolumeBody
from openapi.model.spec_status import SpecStatus
from openapi.model.volume_policy import VolumePolicy

VOLUME_UUID = "8d305974-43a3-484b-8e2c-c74afe2f4400"
SNAPSHOT_UUID = "8d305974-43a3-484b-8e2c-c74afe2f4401"
RESTORE_UUID = "8d305974-43a3-484b-8e2c-c74afe2f4402"


@pytest.fixture(scope="module")
def disks():
    yield Deployer.create_disks(1)
    Deployer.delete_disks(1)


@pytest.fixture(scope="module")
def deployer_cluster(disks):
    Deployer.start(1, cache_period="100ms", reconcile_period="150ms")
    ApiClient.pools_api().put_node_pool(
        Deployer.node_name(0), "pool", CreatePoolBody([disks[0]])
    )
    yield
    Deployer.stop()


@scenario("create_parameters.feature", "Capacity greater than the snapshot")
def test_capacity_greater_than_the_snapshot():
    """Capacity greater than the snapshot."""


@scenario("create_parameters.feature", "Capacity smaller than the snapshot")
def test_capacity_smaller_than_the_snapshot():
    """Capacity smaller than the snapshot."""


@scenario("create_parameters.feature", "Multi-replica restore")
def test_multireplica_restore():
    """Multi-replica restore."""


@scenario("create_parameters.feature", "Thick provisioning")
def test_thick_provisioning():
    """Thick provisioning."""


@given("a deployer cluster")
def a_deployer_cluster(deployer_cluster):
    """a deployer cluster."""


@given(
    "a request to create a new volume with the snapshot as its source",
    target_fixture="base_request",
)
def a_request_to_create_a_new_volume_with_the_snapshot_as_its_source():
    """a request to create a new volume with the snapshot as its source."""
    yield CreateVolumeBody(
        VolumePolicy(True),
        replicas=1,
        size=20 * 1024 * 1024,
        thin=True,
    )


@given("a valid snapshot of a single replica volume")
def a_valid_snapshot_of_a_single_replica_volume():
    """a valid snapshot of a single replica volume."""
    ApiClient.volumes_api().put_volume(
        VOLUME_UUID,
        CreateVolumeBody(
            VolumePolicy(True),
            replicas=1,
            size=20 * 1024 * 1024,
            thin=False,
        ),
    )
    ApiClient.snapshots_api().put_volume_snapshot(VOLUME_UUID, SNAPSHOT_UUID)
    yield
    ApiClient.snapshots_api().del_snapshot(SNAPSHOT_UUID)
    ApiClient.volumes_api().del_volume(VOLUME_UUID)


@when("the request is for a multi-replica volume", target_fixture="request")
def the_request_is_for_a_multireplica_volume(base_request):
    """the request is for a multi-replica volume."""
    base_request.replicas = 2
    yield base_request


@when("the request is for a thick volume", target_fixture="request")
def the_request_is_for_a_thick_volume(base_request):
    """the request is for a thick volume."""
    base_request.thin = False
    yield base_request


@when("the requested capacity is greater than the snapshot", target_fixture="request")
def the_requested_capacity_is_greater_than_the_snapshot(base_request):
    """the requested capacity is greater than the snapshot."""
    base_request.size = base_request.size + 512
    yield base_request


@when("the requested capacity is smaller than the snapshot", target_fixture="request")
def the_requested_capacity_is_smaller_than_the_snapshot(base_request):
    """the requested capacity is smaller than the snapshot."""
    base_request.size = base_request.size - 512
    yield base_request


@then("the request should fail with InvalidArguments")
def the_request_should_fail_with_invalidarguments(request):
    """the request should fail with InvalidArguments."""
    restore_expect(request, http.HTTPStatus.BAD_REQUEST, "InvalidArgument")


@then("the request should fail with OutOfRange")
def the_request_should_fail_with_outofrange(request):
    """the request should fail with OutOfRange."""
    restore_expect(
        request, http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, "OutOfRange"
    )


def restore_expect(request, status, kind):
    with pytest.raises(openapi.exceptions.ApiException) as exception:
        ApiClient.volumes_api().put_snapshot_volume(
            SNAPSHOT_UUID, RESTORE_UUID, request
        )
    assert exception.value.status == status
    error_body = json.loads(exception.value.body)
    assert error_body["kind"] == kind
