"""CSI node Identity RPC tests."""
import os
import threading
import time
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import docker
import subprocess
import csi_pb2 as pb

from common.csi import CsiHandle
from common.deployer import Deployer


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1, csi_node=True)
    yield
    Deployer.stop()


@scenario("identity.feature", "get plugin information")
def test_plugin_info(setup):
    """get plugin information"""


@scenario("identity.feature", "get plugin capabilities")
def test_plugin_capabilities():
    """get plugin capabilities"""


@pytest.fixture(scope="module")
def fix_socket_permissions(setup):
    subprocess.run(
        ["sudo", "chmod", "go+rw", "/var/tmp/csi-app-node-1.sock"], check=True
    )
    yield


def csi_rpc_handle():
    return CsiHandle("unix:///var/tmp/csi-app-node-1.sock")


@given("a running CSI node plugin", target_fixture="csi_instance")
def a_csi_plugin(fix_socket_permissions):
    return csi_rpc_handle()


@when("a GetPluginInfo request is sent to CSI node", target_fixture="info_request")
def plugin_information_info_request(csi_instance):
    return csi_instance.identity.GetPluginInfo(pb.GetPluginInfoRequest())


@then("CSI node should report its name and version")
def check_csi_node_info(info_request):
    assert info_request.name == "io.openebs.csi-mayastor"
    assert info_request.vendor_version == "1.0.0"


@when(
    "a GetPluginCapabilities request is sent to CSI node",
    target_fixture="caps_request",
)
def plugin_information_info_request(csi_instance):
    return csi_instance.identity.GetPluginCapabilities(
        pb.GetPluginCapabilitiesRequest()
    )


@then("CSI node should report its capabilities")
def check_csi_node_info(caps_request):
    all_capabilities = [
        pb.PluginCapability.Service.Type.CONTROLLER_SERVICE,
        pb.PluginCapability.Service.Type.VOLUME_ACCESSIBILITY_CONSTRAINTS,
    ]

    assert len(caps_request.capabilities) == len(
        all_capabilities
    ), "Wrong amount of plugin capabilities reported"

    for c in caps_request.capabilities:
        ct = c.service.type
        assert ct in all_capabilities, "Unexpected capability reported: %s" % str(ct)
