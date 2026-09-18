"""
Wrapper around gRPC handle to communicate with CSI controller.
"""

import csi_pb2_grpc as rpc
import grpc


class CsiHandle(object):
    def __init__(self, csi_socket):
        # gRPC derives the HTTP/2 :authority pseudo-header from the socket path
        # for unix:/// targets, which is not RFC-compliant and is rejected by a
        # spec-conformant h2 server. Override it with a valid authority so the
        # request is accepted (this used to be tolerated by our h2 fork).
        self.channel = grpc.insecure_channel(
            csi_socket, options=[("grpc.default_authority", "localhost")]
        )
        self.controller = rpc.ControllerStub(self.channel)
        self.identity = rpc.IdentityStub(self.channel)
        self.node = rpc.NodeStub(self.channel)

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()
