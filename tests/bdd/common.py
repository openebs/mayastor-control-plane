import os
import subprocess

from openapi.openapi_client.api.volumes_api import VolumesApi
from openapi.openapi_client.api.pools_api import PoolsApi
from openapi.openapi_client.api.specs_api import SpecsApi
from openapi.openapi_client import api_client
from openapi.openapi_client import configuration
import docker

import grpc
import csi_pb2 as pb
import csi_pb2_grpc as rpc

REST_SERVER = "http://localhost:8081/v0"
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME = "mayastor-1"


# Return a configuration which can be used for API calls.
# This is necessary for the API calls so that parameter type conversions can be performed. If the
# configuration is not passed, a type error is raised.
def get_cfg():
    return configuration.Configuration(host=REST_SERVER, discard_unknown_keys=True)


# Return a VolumesApi object which can be used for performing volume related REST calls.
def get_volumes_api():
    api = api_client.ApiClient(get_cfg())
    return VolumesApi(api)


# Return a PoolsApi object which can be used for performing pool related REST calls.
def get_pools_api():
    api = api_client.ApiClient(get_cfg())
    return PoolsApi(api)


# Return a SpecsApi object which can be used for performing spec related REST calls.
def get_specs_api():
    api = api_client.ApiClient(get_cfg())
    return SpecsApi(api)


# Start containers
def deployer_start(num_mayastors):
    deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
    # Start containers and wait for them to become active.
    subprocess.run(
        [deployer_path, "start", "--csi", "-j", "-m", str(num_mayastors), "-w", "10s"]
    )


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
