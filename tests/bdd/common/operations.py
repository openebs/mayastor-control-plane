from retrying import retry

import openapi.exceptions
from common.apiclient import ApiClient
from common.docker import Docker
from openapi.exceptions import NotFoundException
from openapi.model.node_status import NodeStatus


class Pool(object):
    @staticmethod
    def __pools_api():
        return ApiClient.pools_api()

    # Delete all the pools in the cluster
    @staticmethod
    def delete_all():
        all_deleted = True
        for pool in Pool.__pools_api().get_pools():
            try:
                Pool.__pools_api().del_pool(pool.id)
            except NotFoundException:
                pass
            except openapi.exceptions.ApiException:
                all_deleted = False
                pass
        return all_deleted


class Volume(object):
    @staticmethod
    def __api():
        return ApiClient.volumes_api()

    # Delete all the volumes in the cluster
    @staticmethod
    def delete_all():
        for volume in Volume.__api().get_volumes().entries:
            try:
                Volume.__api().del_volume(volume.spec.uuid)
            except NotFoundException:
                pass


class Snapshot(object):
    @staticmethod
    def __api():
        return ApiClient.snapshots_api()

    # Delete all the snapshots in the cluster
    @staticmethod
    def delete_all():
        for snapshot in Snapshot.__api().get_volumes_snapshots().entries:
            try:
                Snapshot.__api().del_snapshot(snapshot.spec.uuid)
            except NotFoundException:
                pass


class Cluster(object):
    # Cleanup the cluster in preparation for another test scenario
    @staticmethod
    def cleanup(snapshots=True, volumes=True, pools=True, waitPools=True):
        # ensure core agent is up
        wait_core_online()

        # ensure nodes are all online
        for node in ApiClient.nodes_api().get_nodes():
            if node.state.status != NodeStatus("Online"):
                Docker.restart_container(node.id)
        for node in ApiClient.nodes_api().get_nodes():
            wait_node_online(node.id)

        # ensure snapshots are all delete
        if snapshots:
            Snapshot.delete_all()
        # ensure volumes are all deleted
        if volumes:
            Volume.delete_all()
        # ensure pools are all deleted
        if pools:
            if waitPools and not Pool.delete_all():
                wait_pools_deleted()


@retry(wait_fixed=10, stop_max_attempt_number=200)
def wait_node_online(node_id):
    assert ApiClient.nodes_api().get_node(node_id).state.status == NodeStatus("Online")


@retry(wait_fixed=10, stop_max_attempt_number=100)
def wait_core_online():
    assert ApiClient.specs_api().get_specs()


@retry(wait_fixed=100, stop_max_attempt_number=100)
def wait_pools_deleted():
    assert Pool.delete_all()
