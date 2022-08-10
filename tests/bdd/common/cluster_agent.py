import grpc
import cluster_agent_pb2_grpc as rpc


class ClusterAgentHandle(object):
    def __init__(self, endpoint):
        self.channel = grpc.insecure_channel(endpoint)
        self.api = rpc.HaRpcStub(self.channel)

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()
