from flask import Flask, request
from app.memcache.memcache import Memcache
from app.common import ReplacementPolicy, CacheConfig
import logging

# Configure Flask APP
memcacheapp = Flask(__name__)
memcache = Memcache()

# Define top level module logger
logger = logging.getLogger(__name__)
logger.info("START MEMCACHE APP")

@memcacheapp.route('/')
def home():
    msg = "Memcache App"
    return '<html><body><h1><i>{}</i></h1></body></html>'.format(msg)


@memcacheapp.route('/get', methods=['GET'])
def get():
    key = request.form.get('key')
    logger.info("Received GET for key=" + key)
    encoded_img_data = memcache.get(key)
    if encoded_img_data is not None:
        return {"success": True,
                "img_data": encoded_img_data
                }
    else:
        return {"success": False,
                "img_data": encoded_img_data
                }


@memcacheapp.route('/put', methods=['POST'])
def put():
    key = request.form.get('key')
    logger.info("Received PUT for key=" + key)
    encode_img_data = request.form.get('img_data')
    if memcache.put(key, encode_img_data):
        return {"success": True}
    else:
        return {"success": False}


@memcacheapp.route('/get_keys', methods=['GET'])
def get_keys():
    keys = memcache.get_all_keys()
    return {"success": (keys is not None),
            "keys": keys
            }


@memcacheapp.route('/invalidate', methods=['DELETE'])
def invalidate():
    key = request.form.get('key')
    success = memcache.invalidate(key)
    return {"success": success}


@memcacheapp.route('/clear', methods=['DELETE'])
def clear():
    success = memcache.clear()
    return {"success": success}


@memcacheapp.route('/set_configuration', methods=['POST'])
def set_configuration():
    replacement_policy_string = request.form.get('replacement_policy')
    max_size_mb_str = request.form.get('max_size_mb')
    max_num_items_str = request.form.get('max_num_items')

    if replacement_policy_string == ReplacementPolicy.RANDOM.value:
        replacement_policy = ReplacementPolicy.RANDOM
    elif replacement_policy_string == ReplacementPolicy.LRU.value:
        replacement_policy = ReplacementPolicy.RANDOM
    else:
        logger.warning("Invalid specification for replacement policy:" + replacement_policy_string)
        return {"success": False}

    max_size_mb = None if max_size_mb_str is None else float(max_size_mb_str)
    max_num_items = None if max_size_mb_str is None else int(max_num_items_str)
    if max_size_mb is None and max_num_items is None:
        logger.warning("One of max_size_mb or max_num_items must be specified.")
        return {"success": False}

    success = memcache.set_configuration(CacheConfig(replacement_policy, max_size_mb, max_num_items))
    return {"success": success}


@memcacheapp.route('/save_stats', methods=['GET'])
def save_stats():
    return {"success": memcache.save_stats() }

@memcacheapp.route('/get_is_active', methods=['GET'])
def get_is_active():
    return {"success": True,
            "is_active": memcache.get_is_active()
            }


@memcacheapp.route('/set_is_active', methods=['POST'])
def set_is_active():
    is_active_string = request.form.get('is_active')
    logger.info("Received request to change active status to:" + is_active_string)
    if is_active_string.lower() == 'true':
        memcache.set_is_active(True)
        return {"success": True}
    elif is_active_string.lower() == 'false':
        memcache.set_is_active(False)
        return {"success": True}
    else:
        logger.warning("Invalid param name:" + is_active_string)
        return {"success": False}


@memcacheapp.route('/get_stat_id', methods=['GET'])
def get_stat_id():
    return {"success": True,
            "stat_id": memcache.get_stat_id()
            }


@memcacheapp.route('/set_stat_id', methods=['POST'])
def set_stat_id():
    stat_id = request.form.get('stat_id')
    if stat_id is not None:
        memcache.set_stat_id(stat_id)
        return {"success": True}
    else:
        logger.warning("Stat id is none.")
        return {"success": False}

