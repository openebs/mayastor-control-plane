from common.apiclient import ApiClient


class Pool(object):
    @staticmethod
    def __pools_api():
        return ApiClient.pools_api()

    # Delete all the pools in the cluster
    @staticmethod
    def delete_all():
        for pool in Pool.__pools_api().get_pools():
            Pool.__pools_api().del_pool(pool.id)
