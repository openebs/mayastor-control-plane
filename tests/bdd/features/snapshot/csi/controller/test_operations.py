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

import openapi.exceptions
from common.apiclient import ApiClient
from common.operations import Snapshot, Volume, Cluster
from openapi.model.create_pool_body import CreatePoolBody

from common.csi import CsiHandle
from common.deployer import Deployer

VOLUME1_UUID = "d01b8bfb-0116-47b0-a03a-447fcbdc0e99"
PVC_VOLUME1_NAME = "pvc-%s" % VOLUME1_UUID
VOLUME2_UUID = "415c27ae-6b4d-48c1-a769-b08e63eff18c"
PVC_VOLUME2_NAME = "pvc-%s" % VOLUME2_UUID
POOL1_NAME = "pool-1"
NODE1 = "io-engine-1"
VOLUME1_SIZE = 1024 * 1024 * 32
SNAP1_NAME = "snapshot-3f49d30d-a446-4b40-b3f6-f439345f1ce9"
SNAP1_UUID = SNAP1_NAME.strip("snapshot-")


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
    try:
        pool_api.del_pool(POOL1_NAME)
    except grpc.RpcError:
        pass
    Deployer.stop()


@scenario("operations.feature", "Create Snapshot Operation is implemented")
def test_create_snapshot_operation_is_implemented():
    """Create Snapshot Operation is implemented."""


@scenario("operations.feature", "Delete Snapshot Operation is implemented")
def test_delete_snapshot_operation_is_implemented():
    """Delete Snapshot Operation."""


@scenario("operations.feature", "List Snapshot Operation is implemented")
def test_list_snapshot_operation_is_implemented():
    """List Snapshot Operation is implemented."""


@scenario("operations.feature", "Create Volume Operation with snapshot source")
def test_create_Volume_operation_with_snapshot_source():
    """Create Volume Operation with snapshot source."""


@given("a running CSI controller plugin", target_fixture="csi_instance")
def a_running_csi_controller_plugin(setup):
    """a running CSI controller plugin."""
    return csi_rpc_handle()


@given("we have a single replica volume", target_fixture="volume")
def we_have_a_single_replica_volume():
    """we have a single replica volume."""
    yield csi_create_1_replica_nvmf_volume1()
    if Cluster.fixture_cleanup():
        csi_delete_1_replica_nvmf_volume1()


@given("a snapshot is created for that volume")
def a_snapshot_is_created_for_that_volume():
    ApiClient.snapshots_api().put_volume_snapshot(VOLUME1_UUID, SNAP1_UUID)
    yield
    Snapshot.cleanup(SNAP1_UUID)


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
    try:
        request = pb.ListSnapshotsRequest(snapshot_id=SNAP1_UUID)
        csi_instance.controller.ListSnapshots(request)
    except grpc.RpcError as grpc_error:
        return grpc_error.value


@then("the creation should succeed")
def the_creation_should_succeed(snapshot_response):
    """the creation should succeed."""
    assert snapshot_response.snapshot.source_volume_id == VOLUME1_UUID
    yield
    Snapshot.cleanup(SNAP1_UUID)


@then("the deletion should succeed")
def the_deletion_should_succeed(grpc_error):
    """the deletion should succeed."""
    assert grpc_error is None


@then("the list should succeed")
def the_list_should_succeed(grpc_error):
    """the list should succeed."""
    assert grpc_error is None


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


@when(
    "a CreateVolumeRequest request with snapshot as source is sent to the CSI controller",
    target_fixture="grpc_error",
)
def a_CreateVolumeRequest_request_with_snapshot_as_source_is_sent_to_the_CSI_controller():
    capacity = pb.CapacityRange(required_bytes=VOLUME1_SIZE, limit_bytes=0)
    parameters = {
        "protocol": "nvmf",
        "ioTimeout": "30",
        "repl": "1",
        "thin": "true",
    }

    req = pb.CreateVolumeRequest(
        name=PVC_VOLUME2_NAME,
        capacity_range=capacity,
        parameters=parameters,
        volume_content_source=pb.VolumeContentSource(
            snapshot=pb.VolumeContentSource.SnapshotSource(snapshot_id=SNAP1_UUID)
        ),
    )

    try:
        csi_rpc_handle().controller.CreateVolume(req)
    except grpc.RpcError as grpc_error:
        return grpc_error
    yield
    Volume.cleanup(VOLUME2_UUID)


@then("the volume creation should succeed")
def the_volume_creation_should_fail(grpc_error):
    assert grpc_error is None
