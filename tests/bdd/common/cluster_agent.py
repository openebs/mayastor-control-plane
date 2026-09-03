import ssl

import grpc
import v1.ha.cluster_agent_pb2_grpc as rpc


class ClusterAgentHandle(object):
    def __init__(self, endpoint):
        # The cluster-agent serves gRPC over TLS using an ephemeral, in-memory
        # self-signed certificate (SAN "localhost"). Since the certificate is
        # generated at start-up and not available on disk, fetch it from the
        # running server and pin it as the trust root, overriding the target name
        # so hostname verification passes regardless of the dialled address.
        host, _, port = endpoint.rpartition(":")
        server_cert = ssl.get_server_certificate((host, int(port))).encode()
        credentials = grpc.ssl_channel_credentials(root_certificates=server_cert)
        options = (("grpc.ssl_target_name_override", "localhost"),)
        self.channel = grpc.secure_channel(endpoint, credentials, options)
        self.api = rpc.HaClusterRpcStub(self.channel)

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()
