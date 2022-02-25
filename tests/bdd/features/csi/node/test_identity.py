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
from common.apiclient import ApiClient


@pytest.fixture(scope="module")
def setup():
    def monitor(proc, result):
        stdout, stderr = proc.communicate()
        result["stdout"] = stdout.decode()
        result["stderr"] = stderr.decode()
        result["status"] = proc.returncode

    try:
        subprocess.run(["sudo", "rm", "/var/tmp/csi.sock"], check=True)
    except:
        pass
    proc = subprocess.Popen(
        args=[
            os.environ["MCP_SRC"] + "/target/debug/csi-node",
            "--csi-socket=/var/tmp/csi.sock",
            "--grpc-endpoint=0.0.0.0",
            "--node-name=msn-test",
            "-v",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    result = {}
    handler = threading.Thread(target=monitor, args=[proc, result])
    handler.start()
    time.sleep(1)
    yield
    subprocess.run(["sudo", "pkill", "csi-node"], check=True)
    handler.join()
    print("[CSI] exit status: %d" % (result["status"]))
    print(result["stdout"])
    print(result["stderr"])


@scenario("identity.feature", "get plugin information")
def test_plugin_info(setup):
    """get plugin information"""


@scenario("identity.feature", "get plugin capabilities")
def test_plugin_capabilities():
    """get plugin capabilities"""


def csi_rpc_handle():
    return CsiHandle("unix:///var/tmp/csi.sock")


@given("a running CSI node plugin", target_fixture="csi_instance")
def a_csi_plugin():
    return csi_rpc_handle()


@when("a GetPluginInfo request is sent to CSI node", target_fixture="info_request")
def plugin_information_info_request(csi_instance):
    return csi_instance.identity.GetPluginInfo(pb.GetPluginInfoRequest())


@then("CSI node should report its name and version")
def check_csi_node_info(info_request):
    assert info_request.name == "io.openebs.csi-mayastor"
    assert info_request.vendor_version == "0.2"


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
