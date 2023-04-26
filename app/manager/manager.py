import hashlib
import logging
import math
import time

from app.rw_lock import ReadWriteLock
from app.common import CacheConfig, ReplacementPolicy, MAX_NUM_NODES, EXPECTED_NUM_NODES
from app.apis import FrontEndApi, MemcacheApi, StorageApi
from app.boto_utils import get_memcache_ip_addresses, get_aggregated_cache_stats_at_time

logger = logging.getLogger(__name__)
TOTAL_NUM_PARTITIONS = 16

DEFAULT_CACHE_CONFIG = CacheConfig(replacement_policy=ReplacementPolicy.LRU, max_size_mb=10, max_num_items=None)

def md5_hash(string):
    # encode the string into bytes
    encoded_string = string.encode('utf-8')

    # create an MD5 hash object
    md5_hash_object = hashlib.md5()

    # update the hash object with the encoded string
    md5_hash_object.update(encoded_string)

    # get the hexadecimal digest of the hash
    hex_digest = md5_hash_object.hexdigest()

    return hex_digest


class Manager:
    """ Maintains pool of memcache nodes and contains operations to interact with them. """
    rw_lock: ReadWriteLock
    cache_config: CacheConfig
    max_available_nodes: int
    cache_pool = []  # List of all available nodes whether active or not
    active_nodes = []
    stat_ids = []
    should_notify_pool_size_change = False  # TODO: make this configurable in front end app?
    active_nodes_history = []

    def __init__(self):
        logger.info("Starting a new Manager instance.")
        self.rw_lock = ReadWriteLock()

        run_remote = 1

        if(run_remote):
            # Load config
            config = StorageApi.get_most_recent_cache_config()
            if config is None:
                logger.warning("No cache config saved in RDS, creating default and saving.")
                config = DEFAULT_CACHE_CONFIG
                StorageApi.save_cache_config(config)
            self.set_configuration(config)

            # Setup cache pool
            self.rw_lock.acquire_write()
            self.load_cache_pool()
            #self.load_cache_pool_debug()
            self.max_available_nodes = len(self.cache_pool)
            self.reload_stat_ids()
            self.active_nodes_history.append((time.time(), len(self.active_nodes)))
            self.rw_lock.release_write()

    def load_cache_pool(self):
        # Load ip addresses of EC2 instances w caches running
        memcache_ip_addresses = get_memcache_ip_addresses()
        if len(memcache_ip_addresses) > MAX_NUM_NODES:
            logger.info("Found more memcache nodes than needed, ignoring extra nodes.")
        if len(memcache_ip_addresses) < EXPECTED_NUM_NODES:
            raise ValueError("Found less memcache nodes than expected.")

        for ip_addr in memcache_ip_addresses:
            if len(self.cache_pool) == EXPECTED_NUM_NODES:
                break
            cache_api = MemcacheApi(ip_addr)
            cache_api.clear()  # Clear caches for new run
            cache_api.set_configuration(self.cache_config)
            self.cache_pool.append(MemcacheApi(ip_addr))
            is_active = cache_api.get_is_active()
            if is_active is not None and is_active is True:
                self.active_nodes.append(cache_api)

    def load_cache_pool_debug(self):
        from app.memcache.memcache import Memcache
        """ Load a local version of the memcache instance for debugging purposes. """
        # Create locally for debugging
        for cache_num in range(EXPECTED_NUM_NODES):
            cache = Memcache()
            cache.clear()
            cache.set_configuration(self.cache_config)
            # Start with only 1 active cache
            if cache_num == 0:
                cache.set_is_active(True)
                self.active_nodes.append(cache)
            else:
                cache.set_is_active(False)
            self.cache_pool.append(cache)

    def set_configuration(self, cache_config):
        if cache_config is None:
            return False
        self.rw_lock.acquire_write()
        self.cache_config = cache_config
        for node in self.cache_pool:
            node.set_configuration(cache_config)
        self.rw_lock.release_write()
        return True

    def generate_node_state_id(self, node_index):
        return "NODE_" + str(node_index)

    def reload_stat_ids(self):
        self.stat_ids = []

        should_set_ids = len(self.cache_pool) == EXPECTED_NUM_NODES
        node_idx = 0
        for node in self.cache_pool:
            node_idx += 1
            new_stat_id = self.generate_node_state_id(node_idx)
            if should_set_ids:
                if node.set_stat_id(new_stat_id):
                    self.stat_ids.append(new_stat_id)
                else:
                    logger.error("Couldn't set or get stat_id of node: " + node.get_url)
            # else:
            #     stat_id = node.get_stat_id()
            #     if stat_id is None:
            #         logger.error("Couldn't set or get stat_id of cache node: " + node.get_url)
            #     else:
            #         self.stat_ids.append(stat_id)

    def get_num_active_nodes(self):
        """ Get the number of active nodes. """
        self.rw_lock.acquire_read()
        num = len(self.active_nodes)
        self.rw_lock.release_read()
        return num

    def get_hash_partition_from_key(self, key):
        """ Get the hash partition (1-16) based on the provided key. """
        hashed_key = md5_hash(key)
        if len(hashed_key) < 32:
            return 1
        first_char = hashed_key[0]
        return int(first_char, TOTAL_NUM_PARTITIONS) + 1

    def get_active_node_for_key(self, key):
        """ Get an active cache for the provided key. """
        partition = self.get_hash_partition_from_key(key)
        active_cache_idx = (partition - 1) % len(self.active_nodes)
        return self.active_nodes[active_cache_idx]

    def put(self, key, value):
        """ Place key/value pair into cache pool. """
        self.rw_lock.acquire_read()
        cache = self.get_active_node_for_key(key)
        result = cache.put(key, value)
        self.rw_lock.release_read()
        return result

    def get(self, key):
        """ Get key/value pair into cache pool. """
        self.rw_lock.acquire_read()
        print("manager.get1")
        cache = self.get_active_node_for_key(key)
        print("manager.get2")
        value = cache.get(key)
        print("manager.get3")
        self.rw_lock.release_read()
        print("manager.get4")
        return value

    def invalidate(self, key):
        """ Get key/value pair into cache pool. """
        self.rw_lock.acquire_read()
        cache = self.get_active_node_for_key(key)
        result = cache.invalidate(key)
        self.rw_lock.release_read()
        return result

    def clear_all_nodes(self):
        """ Clear all the data in all the nodes. """
        self.rw_lock.acquire_read()
        result = True
        for cache in self.cache_pool:
            result = result and cache.clear()
        self.rw_lock.release_read()
        return result

    def notify_pool_size_change(self, old_size, new_size):
        """ Notify front end app that pool size has changed. """

        # Record size change in history
        if len(self.active_nodes_history) == 30:
            self.active_nodes_history.pop(0)
            self.active_nodes_history.append((time.time(), new_size))

        if self.should_notify_pool_size_change and old_size != new_size:
            try:
                FrontEndApi.notify_cache_pool_size_change(time.time())
            except:
                logger.warning("Failed to notify front end of pool size change.")

    def grow_nodes_by_factor(self, growth_factor):
        """ Increase number of active nodes by some growth factor (if applicable). """
        num_active_nodes = self.get_num_active_nodes()
        if num_active_nodes >= self.max_available_nodes:
            logger.warning("Attempting to grow number of active nodes but already at max value, ignoring.")
            return True

        new_num_active_nodes = min(self.max_available_nodes, math.ceil(num_active_nodes * growth_factor))
        num_to_activate = new_num_active_nodes - num_active_nodes
        return self.activate_nodes(num_to_activate)

    def set_num_active_nodes(self, num_desired):
        if num_desired > self.get_num_active_nodes():
            self.activate_nodes(num_desired - self.get_num_active_nodes())
        elif num_desired < self.get_num_active_nodes():
            self.deactivate_nodes(self.get_num_active_nodes() - num_desired)

    def activate_nodes(self, num_to_activate):
        """ Activate the specified number of new nodes if possible. """
        num_active_nodes = self.get_num_active_nodes()
        # Sanity checks
        if num_active_nodes == self.max_available_nodes:
            logger.warning("Attempting to activate new node when already at maximum amount")
            return False

        if num_active_nodes == len(self.cache_pool):
            logger.error("Attempting to activate new cache but all available nodes are already active")
            return False

        # Limit check
        num_available = len(self.cache_pool) - num_active_nodes
        if num_available < num_to_activate:
            logger.info("Attempting to activate " + str(num_to_activate) + "node(s) but only " + str(num_available)
                        + " available. Will activate all available nodes.")
            num_to_activate = num_available

        # ACTIVATE NODES
        num_activated = 0
        self.rw_lock.acquire_write()
        affected_nodes = self.active_nodes.copy()

        # Activate nodes in consecutive order
        for node in self.cache_pool:
            is_active = node.get_is_active()
            if num_activated == num_to_activate:
                break
            if not is_active:
                node.set_is_active(True)
                self.active_nodes.append(node)
                num_activated += 1

        # Rebalance keys in affected nodes
        self.rebalance_keys(affected_nodes)

        # Notify changes
        self.notify_pool_size_change(num_active_nodes, len(self.active_nodes))
        self.rw_lock.release_write()

        return True

    def shrink_nodes_by_factor(self, shrink_factor):
        """ Shrink number of active nodes by some shrink factor (if applicable). """
        num_active_nodes = self.get_num_active_nodes()
        if num_active_nodes <= 1:
            logger.warning("Attempting to shrink number of active nodes but already at min value, ignoring.")
            return True

        new_num_active_nodes = max(1, math.floor(num_active_nodes * shrink_factor))
        num_to_deactivate = num_active_nodes - new_num_active_nodes
        return self.deactivate_nodes(num_to_deactivate)

    def deactivate_nodes(self, num_to_deactivate):
        """ Deactivate the specified number of new nodes if possible. """
        num_active_nodes = self.get_num_active_nodes()
        # Sanity check
        if num_active_nodes == 1:
            logger.warning("Attempting to deactivate node when there is only 1 available, ignoring.")
            return False

        # Limit check
        num_available = num_active_nodes - 1
        if num_available < num_to_deactivate:
            logger.info("Attempting to deactivate " + str(num_to_deactivate) + "node(s) but only " + str(num_available)
                        + " available for this. Will deactivate all available nodes except 1")
            num_to_deactivate = num_available

        # DEACTIVATE NODES
        deactivated_nodes = []
        self.rw_lock.acquire_write()

        # Deactivate nodes in reverse order
        for node in reversed(self.active_nodes):
            if len(deactivated_nodes) == num_to_deactivate:
                break
            node.set_is_active(False)
            self.active_nodes.remove(node)
            deactivated_nodes.append(node)

        # Rebalance keys in affected nodes
        self.rebalance_keys(deactivated_nodes + self.active_nodes)

        # Clear deactivated nodes
        for deactivated_node in deactivated_nodes:
            deactivated_node.clear()

        # Notify changes
        self.notify_pool_size_change(num_active_nodes, len(self.active_nodes))
        self.rw_lock.release_write()
        return True

    def rebalance_keys(self, affected_nodes):
        """ Redistribute key/values from affected nodes to currently active nodes. """
        for affected_node in affected_nodes:
            keys = affected_node.get_all_keys()
            for key in keys:
                new_node = self.get_active_node_for_key(key)
                if new_node != affected_node:
                    # Need to take this item from the old node and put it in the new one
                    value = affected_node.get(key)
                    if value is None:
                        logger.error("Got none value when rebalancing nodes.")
                    else:
                        new_node.put(key, value)
                        affected_node.invalidate(key)

    def get_stat_ids(self):
        if len(self.stat_ids) != EXPECTED_NUM_NODES:
            logger.info("Reloading stat ids for all available nodes.")
            self.reload_stat_ids()
        return self.stat_ids

    def get_all_keys(self):
        self.rw_lock.acquire_read()
        keys = []
        for node in self.active_nodes:
            keys.extend(node.get_all_keys())
        self.rw_lock.release_read()
        return keys

    def set_cache_config(self, cache_config: CacheConfig):
        self.rw_lock.acquire_read()
        for node in self.cache_pool:
            node.set_configuration(cache_config)
        self.rw_lock.release_read()
        return True

    def get_last_min_stats(self):
        return get_aggregated_cache_stats_at_time(self.stat_ids, time.time())
