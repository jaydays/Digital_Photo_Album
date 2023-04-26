from sys import getsizeof
import random
import logging
import time
from threading import Thread
from collections import OrderedDict
from app.memcache.stats import RunningCacheStats
from app.rw_lock import ReadWriteLock
from app.common import CacheConfig, ReplacementPolicy
from random import choice
from string import ascii_uppercase
import requests

logger = logging.getLogger(__name__)


def generate_random_stat_id():
    first_half = ''.join(choice(ascii_uppercase) for i in range(4))
    second_half = ''.join(choice(ascii_uppercase) for i in range(4))
    return first_half + '_' + second_half


class Memcache:
    """ Maintains cache data structure and associated structures. """
    is_active = True
    cache: OrderedDict
    cache_config: CacheConfig
    stat_tracker: RunningCacheStats
    stat_save_thread: Thread
    rw_lock: ReadWriteLock
    cache_size = 0
    stat_id: str

    def __init__(self):
        """Create a new memcache class instance."""
        logger.info("Starting a new Memcache instance.")
        self.stat_id = None #generate_random_stat_id()
        self.cache = OrderedDict()
        self.rw_lock = ReadWriteLock()

        cache_config = None #db_instance.get_most_recent_cache_config() # TODO: get from RDS
        if cache_config is None:
            logger.warning("Couldn't load cache config from DB, creating default.")
            self.cache_config = CacheConfig(ReplacementPolicy.LRU, 10, None)
            #db_instance.add_cache_config(self.cache_config)
        # else:
        #     self.cache_config = cache_config

        self.stat_tracker = RunningCacheStats()
        self.stat_save_thread = Thread(target=self.stat_polling_loop)
        self.stat_save_thread.start()

    def set_stat_id(self, new_id):
        if new_id is not None:
            self.stat_id = new_id

    def get_stat_id(self):
        return self.stat_id

    def get_is_active(self):
        return self.is_active

    def set_is_active(self, is_active):
        if is_active is not None and self.is_active != is_active:
            if is_active:
                self.stat_tracker.reset(time.time())
            self.is_active = is_active

    def print_keys(self):
        print(self.get_all_keys())

    def get(self, key):
        """ Retrieve a value based on the key. """
        if not self.is_active:
            logger.warning("Attempting to get from deactivated cache, continuing anyways.")

        value = self.get_value_internal(key)
        if value is not None:
            # Remove and place back on top of queue
            self.invalidate(key)
            self.add_key_value_internal(key, value)
        self.stat_tracker.add_req_served(is_get=True, is_miss=(value is None))
        return value

    def put(self, key, value):
        """ Place key-value pair in cache. Returns True if successful, false otherwise. """
        if not self.is_active:
            logger.warning("Attempting to put to deactivated cache, ignoring.")
            return False
        self.stat_tracker.add_req_served(is_get=False, is_miss=False)

        # Invalidate the key no matter what
        self.invalidate(key)

        # Check to make sure value isn't too large
        if getsizeof(value) > self.get_max_cache_size_bytes():
            # key/value too large to place in cache
            logger.warning("Cache to small for data entry: bytes="
                           + str(getsizeof(value))
                           + " (key=" + key + ")")
            return False

        # Add key/value to cache
        self.add_key_value_internal(key, value)
        # Clear cache using replacement policy until enough space is free
        self.clear_space_as_necessary(skip_top_entry=True)

        return True
    
    def clear_space_as_necessary(self, skip_top_entry):
        """ Remove elements from cache using replacement policy until it is under the max size limit. """
        while (self.get_cache_size_bytes() > self.get_max_cache_size_bytes()
                or self.get_num_items_in_cache() > self.cache_config.max_num_items) and self.get_num_items_in_cache() > 0:
            self.invalidate_by_policy(skip_top_entry=skip_top_entry)

    def get_value_internal(self, key):
        """ Get a value stored in the cache. """
        self.rw_lock.acquire_read()
        value = None
        if key in self.cache:
            value = self.cache[key]
        self.rw_lock.release_read()
        return value

    def get_all_keys(self):
        """ Get all keys stored in the cache. """
        self.rw_lock.acquire_read()
        keys = list(self.cache.keys())
        self.rw_lock.release_read()
        return keys

    def clear(self):
        if not self.is_active:
            logger.info("Attempting to clear deactivated cache, proceeding anyways.")

        """ Empty the entire cache. """
        self.rw_lock.acquire_write()
        self.cache.clear()
        self.cache_size = 0
        self.rw_lock.release_write()
        return True

    def invalidate(self, key):
        if not self.is_active:
            logger.warning("Attempting to invalidate entry in deactivated cache, continuing anyways.")

        """ Remove a specified key-value pair from the cache based on provided key. """
        self.rw_lock.acquire_write()
        if key in self.cache:
            value = self.cache.pop(key)
            self.cache_size -= getsizeof(value)
            if self.cache_size < 0:
                self.cache_size = 0
        self.rw_lock.release_write()
        return True

    def add_key_value_internal(self, key, value):
        self.rw_lock.acquire_write()
        self.cache[key] = value
        self.cache_size += getsizeof(value)
        self.rw_lock.release_write()

    def invalidate_by_policy(self, skip_top_entry):
        """ Remove an element from the cache based on the configured replacement policy. """
        if self.cache_config.replacement_policy == ReplacementPolicy.RANDOM:
            return self.invalidate_random(skip_top_entry)
        elif self.cache_config.replacement_policy == ReplacementPolicy.LRU:
            return self.invalidate_lru()
        else:
            return False

    def invalidate_lru(self):
        """ Remove an element from the cache based on which was last recently used. """
        self.rw_lock.acquire_write()
        (key, value) = self.cache.popitem(last=False)
        self.cache_size -= getsizeof(value)
        if self.cache_size < 0:
            self.cache_size = 0
        self.rw_lock.release_write()
        return True

    def invalidate_random(self, skip_top_entry):
        """ Remove a random element from the cache.
        If we just added a key we don't want to remove then skip_top_entry should be true. """
        keys = self.get_all_keys()
        if skip_top_entry:
            rand_key = random.choice(keys[:-1])
        else:
            rand_key = random.choice(keys)
        return self.invalidate(rand_key)

    def get_max_cache_size_bytes(self):
        """ Get the max size of the cache in bytes. """
        return self.cache_config.max_size_mb * 1000000

    def get_cache_size_bytes(self):
        """ Get the current size of the cache in bytes. """
        return self.cache_size

    def get_num_items_in_cache(self):
        """ Get the current number of items in the cache. """
        return len(self.get_all_keys())

    # def refresh_cache_config(self):
    #     """ Refresh the current cache configuration based on most recent one saved in DB. """
    #     new_config = self.cache_config #TODO: get from api from manager
    #     return self.set_cache_config(new_config)

    def set_configuration(self, new_config):
        """ Set cache config of this memcache to be one provided. """
        if new_config is None:
            logger.error("New cache config is None, skipping update.")
            return False
        if self.cache_config.is_equivalent_to(new_config):
            logger.warning("Ignoring cache config update due to lack of changes.")
            return True
        else:
            self.cache_config = new_config
            self.clear_space_as_necessary(skip_top_entry=False)
            return True

    def get_cache_config(self):
        """ Get the current cache config. """
        return self.cache_config

    def stat_polling_loop(self):
        while True:
            time.sleep(5)
            #self.save_stats()
            response = requests.get("http://127.0.0.1:5004" + "/save_stats")

    def save_stats(self):
        if self.is_active and self.stat_id is not None:
            self.stat_tracker.save_time_boxed_stat(self.stat_id,
                                                   self.get_num_items_in_cache(),
                                                   self.get_cache_size_bytes())

        return True
