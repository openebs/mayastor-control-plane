import os
import subprocess

from openapi.api.volumes_api import VolumesApi
from openapi.api.pools_api import PoolsApi
from openapi.api.specs_api import SpecsApi
from openapi.api.replicas_api import ReplicasApi
from openapi.api.nodes_api import NodesApi
from openapi import api_client
from openapi import configuration
import docker

import grpc
import csi_pb2_grpc as rpc

REST_SERVER = "http://localhost:8081/v0"
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME = "mayastor-1"


# Return a configuration which can be used for API calls.
# This is necessary for the API calls so that parameter type conversions can be performed. If the
# configuration is not passed, a type error is raised.
def get_cfg():
    return configuration.Configuration(host=REST_SERVER, discard_unknown_keys=True)


# Return an API client
def get_api_client():
    return api_client.ApiClient(get_cfg())


# Return a VolumesApi object which can be used for performing volume related REST calls.
def get_volumes_api():
    return VolumesApi(get_api_client())


# Return a PoolsApi object which can be used for performing pool related REST calls.
def get_pools_api():
    return PoolsApi(get_api_client())


# Return a SpecsApi object which can be used for performing spec related REST calls.
def get_specs_api():
    return SpecsApi(get_api_client())


# Return a NodesApi object which can be used for performing node related REST calls.
def get_nodes_api():
    return NodesApi(get_api_client())


# Return a ReplicasApi object which can be used for performing replica related REST calls.
def get_replicas_api():
    return ReplicasApi(get_api_client())


# Start containers with the default arguments.
def deployer_start(num_mayastors):
    deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
    # Start containers and wait for them to become active.
    subprocess.run(
        [deployer_path, "start", "--csi", "-j", "-m", str(num_mayastors), "-w", "10s"]
    )


# Start containers with the provided arguments.
def deployer_start_with_args(args):
    deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
    subprocess.run([deployer_path, "start"] + args)


# Stop containers
def deployer_stop():
    deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
    subprocess.run([deployer_path, "stop"])


# Determines if a container with the given name is running.
def check_container_running(container_name):
    docker_client = docker.from_env()
    try:
        container = docker_client.containers.get(container_name)
    except docker.errors.NotFound as exc:
        raise Exception("{} container not found", container_name)
    else:
        container_state = container.attrs["State"]
        if container_state["Status"] != "running":
            raise Exception("{} container not running", container_name)


# Kill a container with the given name.
def kill_container(name):
    docker_client = docker.from_env()
    container = docker_client.containers.get(name)
    container.kill()


# Restart a container with the given name.
def restart_container(name):
    docker_client = docker.from_env()
    container = docker_client.containers.get(name)
    container.restart()


"""
Wrapper arount gRPC handle to communicate with CSI controller.
"""


class CsiHandle(object):
    def __init__(self, csi_socket):
        self.channel = grpc.insecure_channel(csi_socket)
        self.controller = rpc.ControllerStub(self.channel)
        self.identity = rpc.IdentityStub(self.channel)

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()
