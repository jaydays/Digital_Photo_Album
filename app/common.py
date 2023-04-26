# THIS FILE IS USED TO DEFINE COMMON CLASSES/INTERFACES ACROSS THE VARIOUS FLASK APPS
import os
import sys
from enum import Enum
import socket

MAX_NUM_NODES = 8
EXPECTED_NUM_NODES = MAX_NUM_NODES
THIS_IP_ADDR = socket.gethostbyname(socket.gethostname())

class Resizingpolicy(Enum):
    MANUAL = "manual"
    AUTO = "automatic"


class AutoScalerConfig:
    """ Specifies configuration parameters for an instance of the auto scaler in the manager app. """
    resizing_policy: Resizingpolicy
    max_miss_rate: float
    min_miss_rate: float
    shrink_factor: float
    growth_factor: float

    def __init__(self, resizing_policy, max_miss_rate, min_miss_rate, shrink_factor, growth_factor):
        """ Create a new AutoScaleConfig instance with provided values. """
        self.resizing_policy = resizing_policy
        self.max_miss_rate = max_miss_rate
        self.min_miss_rate = min_miss_rate
        self.shrink_factor = shrink_factor
        self.growth_factor = growth_factor


class TimeBoxedCacheStats:
    """  Class that contains cache stats in a specified window of time """
    start_time: int
    end_time: int
    num_items_in_cache: int
    cache_size_bytes: int
    num_req_served: int
    num_get_req: int
    num_misses: int
    num_hits: int
    # These can be undefined if the number of get requests was 0, but normally they would be of type float
    miss_rate = None
    hit_rate = None

    def __init__(self, start_time, end_time, num_items_in_cache, cache_size_bytes, num_req_served, num_get_req,
                 num_misses, num_hits):
        self.start_time = start_time
        self.end_time = end_time
        self.num_items_in_cache = num_items_in_cache
        self.cache_size_bytes = cache_size_bytes
        self.num_req_served = num_req_served
        self.num_get_req = num_get_req
        self.num_misses = num_misses
        self.num_hits = num_hits
        if self.num_get_req > 0:
            self.miss_rate = self.num_misses / self.num_get_req
            self.hit_rate = self.num_hits / self.num_get_req


class ReplacementPolicy(Enum):
    RANDOM = "random"
    LRU = "lru"


class CacheConfig:
    """ Specifies configuration parameters for an instance of Memcache. """
    # Default Values
    replacement_policy: ReplacementPolicy
    max_size_mb: int
    max_num_items: int

    def __init__(self, replacement_policy, max_size_mb, max_num_items):
        """ Create a new CacheConfig instance with default values. """
        self.replacement_policy = replacement_policy
        self.max_size_mb = max_size_mb
        if max_num_items is None:
            self.max_num_items = 10000000  # 10 million should be plenty as a default value
        else:
            self.max_num_items = max_num_items

    def is_equivalent_to(self, other_config):
        """ Check if provided cache config instance is equivalent to this one. """
        if not isinstance(other_config, CacheConfig):
            return False
        return (self.replacement_policy == other_config.replacement_policy
                and self.max_size_mb == other_config.max_size_mb
                and self.max_num_items == other_config.max_num_items)


### LOGGING CONFIGURATION ###
app_dir = os.path.dirname(os.path.realpath(__file__))
# Create logging folder/files
logging_dir = os.path.join(app_dir, 'logs')
if not os.path.isdir(logging_dir):
    os.makedirs(logging_dir)
logging_file = os.path.join(logging_dir, 'application.log')
if not os.path.exists(logging_file):
    f = open(logging_file, "w")

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,  # Disable loggers outside our application
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            'datefmt': '%H:%M:%S'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s/%(filename)s ln %(lineno)d: %(message)s',
            'datefmt': '%H:%M:%S'
        },
    },
    'handlers': {
        'console': {  # Handler to show log msgs from our app in the console
            'class': 'logging.StreamHandler',
            'formatter': "standard",
            'level': 'INFO',
            'stream': sys.stdout
        },
        'file': {  # Handler to save log msgs from our app in a file
            'class': 'logging.FileHandler',
            'formatter': "detailed",
            'level': 'DEBUG',
            'filename': logging_file,
        },
    },
    'loggers': {
        __name__: {  # Only log msgs generated in our application will be spat out
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}
