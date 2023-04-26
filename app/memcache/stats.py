from app.common import TimeBoxedCacheStats
from app.boto_utils import save_time_boxed_cache_stats
import time


class RunningCacheStats:
    """  Class to keep track of incremental cache stats from some start time to the present.  """
    start_time: int
    num_req_served: int
    num_get_req: int
    num_misses: int
    num_hits: int

    def __init__(self):
        self.reset(int(time.time()))

    def reset(self, new_start_time):
        self.start_time = new_start_time
        self.num_req_served = 0
        self.num_get_req = 0
        self.num_misses = 0
        self.num_hits = 0

    def add_req_served(self, is_get, is_miss):
        self.num_req_served += 1
        if is_get:
            self.num_get_req += 1
            if is_miss:
                self.num_misses += 1
            else:
                self.num_hits += 1

    def create_time_boxed_stat(self, end_time, num_items_in_cache, cache_size_bytes):
        return TimeBoxedCacheStats(start_time=self.start_time,
                                   end_time=end_time,
                                   num_items_in_cache=num_items_in_cache,
                                   cache_size_bytes=cache_size_bytes,
                                   num_req_served=self.num_req_served,
                                   num_get_req=self.num_get_req,
                                   num_misses=self.num_misses,
                                   num_hits=self.num_hits)

    def save_time_boxed_stat(self, node_name, num_items_in_cache, cache_size_bytes):
        end_time = int(time.time())
        tb_stat = self.create_time_boxed_stat(end_time, num_items_in_cache, cache_size_bytes)
        # TODO: publish to cloudwatch
        save_time_boxed_cache_stats(node_name, tb_stat)
        #db_instance.save_states(tb_stat)
