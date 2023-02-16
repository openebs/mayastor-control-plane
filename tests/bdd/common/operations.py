from common.apiclient import ApiClient
from openapi.exceptions import NotFoundException


class Pool(object):
    @staticmethod
    def __pools_api():
        return ApiClient.pools_api()

    # Delete all the pools in the cluster
    @staticmethod
    def delete_all():
        for pool in Pool.__pools_api().get_pools():
            try:
                Pool.__pools_api().del_pool(pool.id)
            except NotFoundException:
                pass


class Volume(object):
    @staticmethod
    def __api():
        return ApiClient.volumes_api()

    # Delete all the pools in the cluster
    @staticmethod
    def delete_all():
        for volume in Volume.__api().get_volumes().entries:
            try:
                Volume.__api().del_volume(volume.spec.uuid)
            except NotFoundException:
                pass
