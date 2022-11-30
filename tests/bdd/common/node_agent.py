"""
Wrapper around gRPC handle to communicate with HA node agents.
"""

import grpc
import v1.ha.node_agent_pb2_grpc as rpc


class HaNodeHandle(object):
    def __init__(self, csi_socket):
        self.channel = grpc.insecure_channel(csi_socket)
        self.api = rpc.HaNodeRpcStub(self.channel)

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()
