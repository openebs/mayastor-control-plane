"""Snapshot capability gRPC API for CSI Controller feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import csi_pb2 as pb

from common.csi import CsiHandle
from common.deployer import Deployer


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1, csi_controller=True)
    yield
    Deployer.stop()


@scenario("capabilities.feature", "Snapshot CreateDelete capabilities")
def test_snapshot_createdelete_capabilities():
    """Snapshot CreateDelete capabilities."""


@scenario("capabilities.feature", "Snapshot List capabilities")
def test_snapshot_list_capabilities():
    """Snapshot List capabilities."""


@given("a running CSI controller plugin", target_fixture="csi_instance")
def a_running_csi_controller_plugin(setup):
    """a running CSI controller plugin."""
    return csi_rpc_handle()


@when(
    "a ControllerGetCapabilities request is sent to the CSI controller",
    target_fixture="get_caps_request",
)
def a_getcontrollergetcapabilities_request_is_sent_to_the_csi_controller(csi_instance):
    """a ControllerGetCapabilities request is sent to the CSI controller."""
    return csi_instance.controller.ControllerGetCapabilities(
        pb.ControllerGetCapabilitiesRequest()
    )


@then("the response capabilities should include CREATE_DELETE_SNAPSHOT")
def the_response_capabilities_should_include_create_delete_snapshot(get_caps_request):
    """the response capabilities should include CREATE_DELETE_SNAPSHOT."""
    check_controller_capabilities(
        get_caps_request,
        [pb.ControllerServiceCapability.RPC.Type.CREATE_DELETE_SNAPSHOT],
    )


@then("the response capabilities should include LIST_SNAPSHOTS")
def the_response_capabilities_should_include_list_snapshots(get_caps_request):
    """the response capabilities should include LIST_SNAPSHOTS."""
    check_controller_capabilities(
        get_caps_request, [pb.ControllerServiceCapability.RPC.Type.LIST_SNAPSHOTS]
    )


def csi_rpc_handle():
    return CsiHandle("unix:///var/tmp/csi-controller.sock")


def check_controller_capabilities(get_caps_request, check_capabilities):
    reported_capabilities = [c.rpc.type for c in get_caps_request.capabilities]

    missing_capabilities = list(
        filter(lambda c: c not in reported_capabilities, check_capabilities)
    )

    assert (
        len(missing_capabilities) is 0
    ), f"Missing capabilities: {missing_capabilities}"
