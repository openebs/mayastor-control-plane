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
from common.apiclient import ApiClient
from openapi.model.create_pool_body import CreatePoolBody

from common.csi import CsiHandle
from common.deployer import Deployer

VOLUME1_UUID = "d01b8bfb-0116-47b0-a03a-447fcbdc0e99"
PVC_VOLUME1_NAME = "pvc-%s" % VOLUME1_UUID
POOL1_NAME = "pool-1"
NODE1 = "io-engine-1"
VOLUME1_SIZE = 1024 * 1024 * 32
SNAP1_NAME = "snapshot-3f49d30d-a446-4b40-b3f6-f439345f1ce9"
SNAP1_UUID = "3f49d30d-a446-4b40-b3f6-f439345f1ce9"


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1, csi_controller=True)
    pool_labels = {"openebs.io/created-by": "operator-diskpool"}
    pool_api = ApiClient.pools_api()
    pool_api.put_node_pool(
        NODE1,
        POOL1_NAME,
        CreatePoolBody(["malloc:///disk?size_mb=128"], labels=pool_labels),
    )
    yield
    # Seems to crash, maybe because we don't delete the snapshot?
    # try:
    #     pool_api.del_pool(POOL1_NAME)
    # except grpc.RpcError:
    #     pass
    Deployer.stop()


@scenario("operations.feature", "Create Snapshot Operation is implemented")
def test_create_snapshot_operation_is_implemented():
    """Create Snapshot Operation is implemented."""


# @pytest.mark.skip("This would be fixed in the upcoming changes")
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


@given("a single replica volume", target_fixture="volume")
def a_single_replica_volume():
    """a single replica volume."""
    yield csi_create_1_replica_nvmf_volume1()
    csi_delete_1_replica_nvmf_volume1()


@then("it should succeed")
def it_should_succeed(snapshot_response):
    """it should succeed."""
    assert snapshot_response.snapshot.source_volume_id == VOLUME1_UUID


@when(
    "a CreateSnapshotRequest request is sent to the CSI controller",
    target_fixture="snapshot_response",
)
def a_createsnapshotrequest_request_is_sent_to_the_csi_controller(csi_instance, volume):
    """a CreateSnapshotRequest request is sent to the CSI controller."""
    request = pb.CreateSnapshotRequest(
        source_volume_id=volume.volume_id, name=SNAP1_NAME
    )
    return csi_instance.controller.CreateSnapshot(request)


@when(
    "a DeleteSnapshotRequest request is sent to the CSI controller",
    target_fixture="grpc_error",
)
def a_deletesnapshotrequest_request_is_sent_to_the_csi_controller(csi_instance):
    """a DeleteSnapshotRequest request is sent to the CSI controller."""
    try:
        request = pb.DeleteSnapshotRequest(
            snapshot_id=SNAP1_UUID,
        )
        csi_instance.controller.DeleteSnapshot(request)
    except grpc.RpcError as grpc_error:
        return grpc_error.value


@when(
    "a ListSnapshotRequest request is sent to the CSI controller",
    target_fixture="grpc_error",
)
def a_listsnapshotrequest_request_is_sent_to_the_csi_controller(csi_instance):
    """a ListSnapshotRequest request is sent to the CSI controller."""
    with pytest.raises(grpc.RpcError) as grpc_error:
        request = pb.ListSnapshotsRequest(snapshot_id=SNAP1_NAME)
        csi_instance.controller.ListSnapshots(request)
    return grpc_error.value


@then("the deletion should succeed")
def the_deletion_should_succeed(grpc_error):
    assert grpc_error is None


@then("it should fail with status NOT_IMPLEMENTED")
def it_should_fail_with_status_not_implemented(grpc_error):
    """it should fail with status NOT_IMPLEMENTED."""
    assert (
        grpc_error.code() is grpc.StatusCode.UNIMPLEMENTED
    ), "Unexpected gRPC Error Code"


def csi_rpc_handle():
    return CsiHandle("unix:///var/tmp/csi-controller.sock")


def csi_create_1_replica_nvmf_volume1():
    capacity = pb.CapacityRange(required_bytes=VOLUME1_SIZE, limit_bytes=0)
    parameters = {
        "protocol": "nvmf",
        "ioTimeout": "30",
        "repl": "1",
    }

    req = pb.CreateVolumeRequest(
        name=PVC_VOLUME1_NAME, capacity_range=capacity, parameters=parameters
    )

    return csi_rpc_handle().controller.CreateVolume(req).volume


def csi_delete_1_replica_nvmf_volume1():
    csi_rpc_handle().controller.DeleteVolume(
        pb.DeleteVolumeRequest(volume_id=VOLUME1_UUID)
    )
