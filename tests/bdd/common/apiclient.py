from openapi.api.volumes_api import VolumesApi
from openapi.api.pools_api import PoolsApi
from openapi.api.specs_api import SpecsApi
from openapi.api.replicas_api import ReplicasApi
from openapi.api.nodes_api import NodesApi
from openapi import api_client
from openapi import configuration

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


class ApiClient(object):
    # Return a VolumesApi object which can be used for performing volume related REST calls.
    @staticmethod
    def volumes_api():
        return VolumesApi(get_api_client())

    # Return a PoolsApi object which can be used for performing pool related REST calls.
    @staticmethod
    def pools_api():
        return PoolsApi(get_api_client())

    # Return a SpecsApi object which can be used for performing spec related REST calls.
    @staticmethod
    def specs_api():
        return SpecsApi(get_api_client())

    # Return a NodesApi object which can be used for performing node related REST calls.
    @staticmethod
    def nodes_api():
        return NodesApi(get_api_client())

    # Return a ReplicasApi object which can be used for performing replica related REST calls.
    @staticmethod
    def replicas_api():
        return ReplicasApi(get_api_client())
