import http
import time
from urllib.parse import urlparse
from retrying import retry
import sys

import common
import openapi.exceptions
from openapi.exceptions import ApiException
from common.apiclient import ApiClient
from common.deployer import Deployer
from common.docker import Docker
from common.fio import Fio
from openapi.exceptions import NotFoundException
from openapi.model.node_status import NodeStatus
from openapi.model.publish_volume_body import PublishVolumeBody
from openapi.model.protocol import Protocol


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

    @staticmethod
    def cleanup(volume):
        if Cluster.fixture_cleanup():
            try:
                if hasattr(volume, "spec"):
                    Volume.__api().del_volume(volume.spec.uuid)
                else:
                    Volume.__api().del_volume(volume)
            except NotFoundException:
                pass

    @staticmethod
    def update(volume, cached=True):
        # this should be built-in the rest server
        if not cached:
            Cluster.wait_cache_update()
        return ApiClient.volumes_api().get_volume(volume.spec.uuid)

    @staticmethod
    def fio(volume, offset="0", size="4M", rw="write"):
        try:
            volume = ApiClient.volumes_api().put_volume_target(
                volume.spec.uuid,
                publish_volume_body=PublishVolumeBody({}, Protocol("nvmf")),
            )
        except ApiException as e:
            assert e.status == http.HTTPStatus.PRECONDITION_FAILED
            volume = Volume.update(volume)
        uri = urlparse(volume.state.target["deviceUri"])
        fio = Fio(name="job", rw=rw, uri=uri, offset=offset, size=size)
        fio.run()
        volume = Volume.update(volume)
        return volume


class Snapshot(object):
    @staticmethod
    def __api():
        return ApiClient.snapshots_api()

    # Delete all the snapshots in the cluster
    @staticmethod
    def delete_all():
        for snapshot in Snapshot.__api().get_volumes_snapshots().entries:
            try:
                Snapshot.__api().del_snapshot(snapshot.definition.spec.uuid)
            except NotFoundException:
                pass

    @staticmethod
    def cleanup(snapshot):
        if Cluster.fixture_cleanup():
            try:
                if hasattr(snapshot, "definition"):
                    Snapshot.__api().del_snapshot(snapshot.definition.spec.uuid)
                else:
                    Snapshot.__api().del_snapshot(snapshot)
            except NotFoundException:
                pass

    @staticmethod
    def update(snapshot, volume=None, cached=True):
        if not cached:
            Cluster.wait_cache_update()
        if volume is not None:
            return ApiClient.snapshots_api().get_volume_snapshot(
                volume.spec.uuid, snapshot.definition.spec.uuid
            )
        return ApiClient.snapshots_api().get_volumes_snapshot(
            snapshot.definition.spec.uuid
        )


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

    @staticmethod
    def env_cleanup():
        return common.env_cleanup()

    @staticmethod
    def fixture_cleanup():
        return common.env_cleanup() or not hasattr(sys, "last_traceback")

    @staticmethod
    def wait_cache_update(slack=0.1):
        cache = common.human_time_to_float(Deployer.cache_period())
        time.sleep(cache + slack)

    @staticmethod
    def restart_node(node_name):
        Deployer.restart_node(node_name)


@retry(wait_fixed=10, stop_max_attempt_number=200)
def wait_node_online(node_id):
    assert ApiClient.nodes_api().get_node(node_id).state.status == NodeStatus("Online")


@retry(wait_fixed=10, stop_max_attempt_number=100)
def wait_core_online():
    assert ApiClient.specs_api().get_specs()


@retry(wait_fixed=100, stop_max_attempt_number=100)
def wait_pools_deleted():
    assert Pool.delete_all()
