"""CSI Controller Identity RPC tests."""
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
    Deployer.start(1, csi=True)
    subprocess.run(["sudo", "chmod", "go+rw", "/var/tmp/csi.sock"], check=True)
    yield
    Deployer.stop()


@scenario("feature.feature", "get plugin information")
def test_plugin_info(setup):
    """get plugin information"""


@scenario("feature.feature", "get plugin capabilities")
def test_plugin_capabilities(setup):
    """get plugin capabilities"""


@scenario(
    "feature.feature",
    "probe CSI controller when REST API endpoint is accessible",
)
def test_probe_rest_accessible(setup):
    """probe when REST is accessible"""


@scenario(
    "feature.feature",
    "probe CSI controller when REST API endpoint is not accessible",
)
def test_probe_rest_not_accessible(setup):
    """probe when REST is not accessible"""


def csi_rpc_handle():
    return CsiHandle("unix:///var/tmp/csi.sock")


@given("a running CSI controller plugin", target_fixture="csi_instance")
def a_csi_plugin():
    return csi_rpc_handle()


@given(
    "a running CSI controller plugin with accessible REST API endpoint",
    target_fixture="csi_plugin",
)
def csi_plugin_and_rest_api():
    # Check REST APi accessibility by listing pools.
    ApiClient.pools_api().get_pools()
    return csi_rpc_handle()


@pytest.fixture(scope="function")
def stop_start_rest():
    docker_client = docker.from_env()

    try:
        rest_server = docker_client.containers.list(all=True, filters={"name": "rest"})[
            0
        ]
    except docker.errors.NotFound:
        raise Exception("No REST server instance found")

    rest_server.stop()
    yield
    rest_server.start()


@given(
    "a running CSI controller plugin without REST API server running",
    target_fixture="csi_plugin_partial",
)
def csi_plugin_without_rest_api(stop_start_rest):
    # Make sure REST API is not accessible anymore.
    with pytest.raises(Exception) as e:
        ApiClient.pools_api().get_pools()
    return csi_rpc_handle()


@when(
    "a GetPluginInfo request is sent to CSI controller", target_fixture="info_request"
)
def plugin_information_info_request(csi_instance):
    return csi_instance.identity.GetPluginInfo(pb.GetPluginInfoRequest())


@then("CSI controller should report its name and version")
def check_csi_controller_info(info_request):
    assert info_request.name == "io.openebs.csi-mayastor"
    assert info_request.vendor_version == "0.5"


@when(
    "a GetPluginCapabilities request is sent to CSI controller",
    target_fixture="caps_request",
)
def plugin_information_info_request(csi_instance):
    return csi_instance.identity.GetPluginCapabilities(
        pb.GetPluginCapabilitiesRequest()
    )


@then("CSI controller should report its capabilities")
def check_csi_controller_info(caps_request):
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


@when("a Probe request is sent to CSI controller", target_fixture="probe_available")
def probe_request_api_accessible(csi_plugin):
    return csi_plugin.identity.Probe(pb.ProbeRequest())


@when(
    "a Probe request is sent to CSI controller which can not access REST API endpoint",
    target_fixture="probe_not_available",
)
def probe_request_api_not_accessible(csi_plugin_partial):
    return csi_plugin_partial.identity.Probe(pb.ProbeRequest())


@then("CSI controller should report itself as being ready")
def check_probe_api_accessible(probe_available):
    assert probe_available.ready.value, "CSI Plugin is not ready"


@then("CSI controller should report itself as being ready")
def check_probe_request_api_accessible(probe_available):
    assert probe_available.ready.value, "CSI Plugin is not ready"


@then("CSI controller should report itself as being not ready")
def check_probe_request_api_not_accessible(probe_not_available):
    assert (
        probe_not_available.ready.value == False
    ), "CSI controller is ready when REST server is not reachable"
