"""Snapshot capability gRPC API for CSI Controller feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import csi_pb2 as pb
import grpc

from common.csi import CsiHandle
from common.deployer import Deployer


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1, csi_controller=True)
    yield
    Deployer.stop()


@scenario("operations.feature", "Create Snapshot Operation is not implemented")
def test_create_snapshot_operation_is_not_implemented():
    """Create Snapshot Operation."""


@scenario("operations.feature", "Delete Snapshot Operation is not implemented")
def test_delete_snapshot_operation_is_not_implemented():
    """Delete Snapshot Operation."""


@scenario("operations.feature", "List Snapshot Operation is not implemented")
def test_list_snapshot_operation_is_not_implemented():
    """List Snapshot Operation is not implemented."""


@given("a running CSI controller plugin", target_fixture="csi_instance")
def a_running_csi_controller_plugin(setup):
    """a running CSI controller plugin."""
    return csi_rpc_handle()


@when(
    "a CreateSnapshotRequest request is sent to the CSI controller",
    target_fixture="grpc_error",
)
def a_createsnapshotrequest_request_is_sent_to_the_csi_controller(csi_instance):
    """a CreateSnapshotRequest request is sent to the CSI controller."""
    with pytest.raises(grpc.RpcError) as grpc_error:
        request = pb.CreateSnapshotRequest(
            source_volume_id="1d447a4d-bca6-4ab9-82cc-dea1652b37e7", name="snapshot-1"
        )
        csi_instance.controller.CreateSnapshot(request)
    return grpc_error.value


@when(
    "a DeleteSnapshotRequest request is sent to the CSI controller",
    target_fixture="grpc_error",
)
def a_deletesnapshotrequest_request_is_sent_to_the_csi_controller(csi_instance):
    """a DeleteSnapshotRequest request is sent to the CSI controller."""
    with pytest.raises(grpc.RpcError) as grpc_error:
        request = pb.DeleteSnapshotRequest(
            snapshot_id="1d447a4d-bca6-4ab9-82cc-dea1652b37e7",
        )
        csi_instance.controller.DeleteSnapshot(request)
    return grpc_error.value


@when(
    "a ListSnapshotRequest request is sent to the CSI controller",
    target_fixture="grpc_error",
)
def a_listsnapshotrequest_request_is_sent_to_the_csi_controller(csi_instance):
    """a ListSnapshotRequest request is sent to the CSI controller."""
    with pytest.raises(grpc.RpcError) as grpc_error:
        request = pb.ListSnapshotsRequest()
        csi_instance.controller.ListSnapshots(request)
    return grpc_error.value


@then("it should fail with status NOT_IMPLEMENTED")
def it_should_fail_with_status_not_implemented(grpc_error):
    """it should fail with status NOT_IMPLEMENTED."""
    assert (
        grpc_error.code() is grpc.StatusCode.UNIMPLEMENTED
    ), "Unexpected gRPC Error Code"


def csi_rpc_handle():
    return CsiHandle("unix:///var/tmp/csi-controller.sock")
