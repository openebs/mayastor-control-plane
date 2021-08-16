import os
import subprocess
import time

from openapi.openapi_client.api.volumes_api import VolumesApi
from openapi.openapi_client.model.create_pool_body import CreatePoolBody
from openapi.openapi_client.api.pools_api import PoolsApi
from openapi.openapi_client import api_client
from openapi.openapi_client import configuration

REST_SERVER = "http://localhost:8081/v0"
POOL_UUID = "4cc6ee64-7232-497d-a26f-38284a444980"
NODE_NAME = "mayastor"


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


# Start containers
def deployer_start(num_mayastors):
    deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
    # Start containers and wait for them to become active.
    subprocess.run([deployer_path, "start", "-m", str(num_mayastors), "-w", "10s"])


# Stop containers
def deployer_stop():
    deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
    subprocess.run([deployer_path, "stop"])
    # Allow time for containers to stop
    time.sleep(2)
