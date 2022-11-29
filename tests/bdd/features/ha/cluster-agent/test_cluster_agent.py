"""gRPC API for cluster-agent feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import pytest
import v1.ha.cluster_agent_pb2 as pb
import grpc
from common.deployer import Deployer
from common.cluster_agent import ClusterAgentHandle


@scenario("cluster_agent.feature", "register node-agent")
def test_register_nodeagent():
    """register node-agent."""


@scenario("cluster_agent.feature", "register node-agent with empty nodeAgent details")
def test_register_nodeagent_with_empty_nodeagent_details():
    """register node-agent with empty nodeAgent details."""


@given("a running cluster agent")
def a_running_cluster_agent(setup):
    """a running cluster agent."""


@when("the RegisterNodeAgent request is sent to cluster-agent")
def the_registernodeagent_request_is_sent_to_clusteragent(
    register_node_agent,
):
    """the RegisterNodeAgent request is sent to cluster-agent."""


@when("the RegisterNodeAgent request is sent to cluster-agent with empty data")
def the_registernodeagent_request_is_sent_to_clusteragent_with_empty_data(
    register_node_agent_with_empty_data,
):
    """the RegisterNodeAgent request is sent to cluster-agent with empty data."""


@then("the request should be completed successfully")
def the_request_should_be_completed_successfully(context):
    """the request should be completed successfully."""
    assert context["attempt"]


@then("the request should be failed")
def the_request_should_be_failed(context):
    """the request should be failed."""
    e = context["attempt"]
    assert (
        e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    ), "Unexpected error code: %s" + str(e.value.code())


def cluster_agent_rpc_handle():
    return ClusterAgentHandle("0.0.0.0:11500")


@pytest.fixture(scope="module")
def setup():
    Deployer.start(1, cluster_agent=True)
    yield
    Deployer.stop()


@pytest.fixture(scope="function")
def context():
    return {}


@pytest.fixture
def register_node_agent(context):
    hdl = cluster_agent_rpc_handle()
    req = pb.HaNodeInfo(nodename="test", endpoint="0.0.0.0:1111")

    context["attempt"] = hdl.api.RegisterNodeAgent(req)


@pytest.fixture
def register_node_agent_with_empty_data(context):
    hdl = cluster_agent_rpc_handle()
    req = pb.HaNodeInfo()
    with pytest.raises(grpc.RpcError) as error:
        hdl.api.RegisterNodeAgent(req)
    context["attempt"] = error
