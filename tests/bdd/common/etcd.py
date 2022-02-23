import etcd3


class Etcd(object):
    def __init__(self):
        self.client = etcd3.client(host="localhost", port=2379)

    # Get the NexusInfo structure.
    def get_nexus_info(self, volume_id, nexus_id):
        # This key must match that used by the control plane.
        key = "/namespace/{}/volume/{}/nexus/{}/info".format(
            "default", volume_id, nexus_id
        )
        # Getting the entry returns a tuple of the value and metadata.
        # Return the NexusInfo value only.
        return self.client.get(key)[0]
