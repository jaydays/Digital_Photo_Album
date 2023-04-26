import requests

import app.boto_utils
from app.common import CacheConfig, AutoScalerConfig, Resizingpolicy, ReplacementPolicy
import jsonpickle

USE_LOCAL_IP = False
LOCAL_HOST_IP = "127.0.0.1"
#TEMP_IP = app.boto_utils.get__ec2_instance_ip_addr()
DEFAULT_IP = LOCAL_HOST_IP if USE_LOCAL_IP else app.common.THIS_IP_ADDR

# All these apps can probably run on the same EC2 instance, thus running on local host
FRONTEND_APP_URL = "http://" + DEFAULT_IP + ":5000/"
MANAGER_APP_URL = "http://" + DEFAULT_IP + ":5001/"
AUTOSCALER_APP_URL = "http://" + DEFAULT_IP + ":5002/"
STORAGE_APP_URL = "http://" + DEFAULT_IP + ":5003/"
MEMCACHE_APP_PORT = "5004"

# THIS CLASS DEFINES THE API ENDPOINTS OF ALL THE FLASK APPS
# APP API SHOULD BE PROGRAMMED TO CONFORM TO THE API SPECIFIED HERE


class FrontEndApi:
    @staticmethod
    def notify_cache_pool_size_change(timestamp):
        num_active_nodes = ManagerApi.get_num_active_nodes() # Number of Active Nodes

        cache_config      = StorageApi.get_most_recent_cache_config()
        autoscaler_config = StorageApi.get_most_recent_autoscaler_config()

        capacity = cache_config.max_size_mb
        replacement_policy = cache_config.replacement_policy
        autoscaler_config = autoscaler_config.resizing_policy
        response = requests.post(FRONTEND_APP_URL + "/api/notify_pool_size_change",
                                 data={'timestamp': timestamp,
                                       'capacity': capacity,
                                       'replacement_policy': replacement_policy.value,
                                       'autoscaler_config': autoscaler_config.value,
                                       'pool_size': num_active_nodes})
        json_response = response.json()
        return json_response['success'] is True


class StorageApi:
    @staticmethod
    def store_img(key, img_filename, img_file):
        print("key = ", key)
        print("img_filename = ", img_filename)
        print("img_file = ", img_file)

        print("MADE IT HERE 3")
        print(STORAGE_APP_URL + "/api/store_image")
        response = requests.post(STORAGE_APP_URL + "api/store_image",
                                 data={'key': key,
                                       'img_filename': img_filename
                                       },
                                 files = {'img_file': img_file})
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def get_img_url(key):
        response = requests.post(STORAGE_APP_URL + "/api/get_image_url",
                                 data={'key': key})
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        print("Response at API", json_response['img_url'])
        return json_response['img_url']

    @staticmethod
    def delete_all():
        response = requests.post(STORAGE_APP_URL + "/api/delete_all")
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def save_cache_config(cache_config: CacheConfig):
        headers = {'Content-type': 'application/json'}
        data = jsonpickle.encode(cache_config)
        response = requests.post(STORAGE_APP_URL + "/api/save_cache", headers=headers, data=data)
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def get_most_recent_cache_config():
        response = requests.get(STORAGE_APP_URL + "/api/get_cache")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        cache_config = jsonpickle.decode(json_response['data'])
        print("Response at API", cache_config)
        return cache_config

    @staticmethod
    def save_autoscaler_config(scaler_config: AutoScalerConfig):
        headers = {'Content-type': 'application/json'}
        data = jsonpickle.encode(scaler_config)
        response = requests.post(STORAGE_APP_URL + "/api/save_autoscaler",headers=headers, data=data)
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def get_most_recent_autoscaler_config():
        response = requests.post(STORAGE_APP_URL + "/api/get_autoscaler")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        autoscaler_config = jsonpickle.decode(json_response['data'])
        print("Response at API", autoscaler_config)
        return autoscaler_config

    @staticmethod
    def get_keys():
        response = requests.post(STORAGE_APP_URL + "/api/get_all_keys")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        print("Response at API", json_response['keys'])
        return json_response['keys']

class AutoScalerApi:
    @staticmethod
    def refresh_config():
        response = requests.get(AUTOSCALER_APP_URL + "/refresh_configuration")
        json_response = response.json()
        return json_response['success'] is True


class ManagerApi:
    @staticmethod
    def set_configuration(cache_config: CacheConfig):
        replacement_policy = cache_config.replacement_policy.value
        max_size_mb = cache_config.max_size_mb
        max_num_items = cache_config.max_num_items
        if max_num_items is not None:
            response = requests.post(MANAGER_APP_URL + "/set_configuration", data={'replacement_policy': replacement_policy,
                                                                            'max_size_mb': max_size_mb,
                                                                            'max_num_items': max_num_items})
        else:
            response = requests.post(MANAGER_APP_URL + "/set_configuration", data={'replacement_policy': replacement_policy,
                                                                            'max_size_mb': max_size_mb})

        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def put(key, img_data):
        response = requests.post(MANAGER_APP_URL + "/put", data={'key': key, 'img_data': img_data})
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def get(key):
        response = requests.get(MANAGER_APP_URL + "/get", data={'key': key})
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        return json_response['img_data']

    @staticmethod
    def get_rate(rate_type):
        response = requests.get(MANAGER_APP_URL + "/getRate",  data={'type': rate_type})
        if response is None:
            return None
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        return json_response['rate']

    @staticmethod
    def expand_nodes(growth_factor):
        """ Expand number of nodes by provided factor, should only be called from Autoscaler app. """
        response = requests.post(MANAGER_APP_URL + "/expand_nodes", data={'growth_factor': growth_factor})
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def shrink_nodes(shrink_factor):
        """ Shrink number of nodes by provided factor, should only be called from Autoscaler app. """
        response = requests.post(MANAGER_APP_URL + "/shrink_nodes", data={'shrink_factor': shrink_factor})
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def get_stat_ids():
        """ Get the stat ids of all nodes in the cache pool. """
        response = requests.get(MANAGER_APP_URL + "/get_stat_ids")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        return json_response['stat_ids']

    @staticmethod
    def get_num_active_nodes():
        response = requests.get(MANAGER_APP_URL + "/get_num_active_nodes")
        json_response = response.json()
        return json_response['num_active_nodes']

    @staticmethod
    def set_num_active_nodes(num_desired):
        response = requests.post(MANAGER_APP_URL + "/set_num_active_nodes", data={'num_desired': num_desired})
        json_response = response.json()
        return json_response['success'] is True


    @staticmethod
    def clear():
        response = requests.delete(MANAGER_APP_URL + "/clear_all_nodes")
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def invalidate(key):
        response = requests.delete(MANAGER_APP_URL + "/invalidate", data={'key': key})
        json_response = response.json()
        return json_response['success'] is True

    @staticmethod
    def get_all_keys():
        response = requests.get(MANAGER_APP_URL + "/get_all_keys")
        if response is None:
            return []
        json_response = response.json()
        if json_response['success'] is not True:
            return []
        return json_response['keys']


class MemcacheApi:
    """  """
    url: str

    def __init__(self, ip_addr):
        self.url = "http://" + ip_addr + ":" + MEMCACHE_APP_PORT

    def get_url(self):
        return self.url

    def get(self, key):
        response = requests.get(self.url + "/get", data={'key': key})
        json_response = response.json()
        img_data = None
        if json_response['success'] is True:
            img_data = json_response['img_data']
        return img_data

    def put(self, key, img_data):
        response = requests.post(self.url + "/put", data={'key': key, 'img_data': img_data})
        json_response = response.json()
        return json_response['success'] is True

    def clear(self):
        response = requests.delete(self.url + "/clear")
        json_response = response.json()
        return json_response['success'] is True

    def invalidate(self, key):
        response = requests.delete(self.url + "/invalidate", data={'key': key})
        json_response = response.json()
        return json_response['success'] is True

    def get_all_keys(self):
        response = requests.get(self.url + "/get_keys")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        return json_response['keys']

    def set_configuration(self, cache_config: CacheConfig):
        replacement_policy = cache_config.replacement_policy.value
        max_size_mb = cache_config.max_size_mb
        max_num_items = cache_config.max_num_items
        if max_num_items is not None:
            response = requests.post(self.url + "/set_configuration", data={'replacement_policy': replacement_policy,
                                                                            'max_size_mb': max_size_mb,
                                                                            'max_num_items': max_num_items})
        else:
            response = requests.post(self.url + "/set_configuration", data={'replacement_policy': replacement_policy,
                                                                            'max_size_mb': max_size_mb})

        json_response = response.json()
        return json_response['success'] is True

    def get_is_active(self):
        response = requests.get(self.url + "/get_is_active")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        return json_response['is_active']

    def set_is_active(self, is_active):
        response = requests.post(self.url + "/set_is_active", data={'is_active': is_active})
        json_response = response.json()
        return json_response['success'] is True

    def activate(self):
        return self.set_is_active(True)

    def deactivate(self):
        return self.set_is_active(False)

    def get_stat_id(self):
        response = requests.get(self.url + "/get_stat_id")
        json_response = response.json()
        if json_response['success'] is not True:
            return None
        return json_response['stat_id']

    def set_stat_id(self, stat_id):
        response = requests.post(self.url + "/set_stat_id", data={'stat_id': stat_id})
        json_response = response.json()
        return json_response['success'] is True

