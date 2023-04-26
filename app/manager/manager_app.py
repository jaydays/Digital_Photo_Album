import time
from datetime import datetime
from flask import Flask, request
from flask import render_template
import matplotlib
import matplotlib.pyplot as plt
from PIL import Image
import base64
import io
import re

import app.boto_utils
from app.apis import *
import requests

import logging

from app.manager.manager import Manager
from app.boto_utils import get_aggregated_cache_stats_at_time
from app.common import TimeBoxedCacheStats

# Configure Flask APP
managerapp = Flask(__name__, static_folder='../static')
manager = Manager()

# Define top level module logger
logger = logging.getLogger(__name__)
logger.info("START MANAGEMENT APP")

matplotlib.use('Agg')

#####################   ROUTES/UI  ###########################

@managerapp.route('/')
def home():
    this_ip = DEFAULT_IP
    return render_template("management_home.html", url="http://" + this_ip + ":5000/home_page")

@managerapp.route('/home_page')
def home_page():
    logger.debug("Launched home page.")
    this_ip = DEFAULT_IP
    print("IP: ", this_ip)
    return render_template("management_home.html", url="http://" + this_ip + ":5000/home_page")

@managerapp.route('/memcache_info')
def memcache_info():
    print("In memcache info")
    p1, p2, p3, p4, p5, p6 = list_of_plots()
    return render_template("display_stats.html", plot1 = p1.decode('UTF-8'), plot2 = p2.decode('UTF-8'), plot3 = p3.decode('UTF-8'),
                                                 plot4 = p4.decode('UTF-8'), plot5 = p5.decode('UTF-8'), plot6 = p6.decode('UTF-8'))

@managerapp.route('/c_r_policy')
def c_r_policy(): # Capacity and Replacement Policy
    print("In C&R Policy")
    return render_template("c_r_policy.html")

@managerapp.route('/resize_policy')
def resize_policy(): # Capacity and Replacement Policy
    print("In Resize Policy")
    return render_template("resize_policy.html")

@managerapp.route('/clear')
def clear():
    print("In Clear")
    manager.clear_all_nodes()
    return render_template('success_management.html', message="Cleared Mem-Cache")

@managerapp.route('/delete_all')
def delete_all(): # Capacity and Replacement Policy
    print("In Delete All")
    manager.clear_all_nodes()
    StorageApi.delete_all()
    return render_template('success_management.html', message="Cleared All App Data")

#####################   Exec Functions  ###########################

@managerapp.route('/apply_c_r_policy', methods = ['POST'])
def apply_c_r_policy(): # Capacity and Replacement Policy
    print("In Apply C&R Policy")
    selected_capacity_string = request.form.get('mem-cache-capacity')
    selected_policy_string = request.form.get('replacement-policy')

    replacement_policy = ReplacementPolicy.LRU
    if selected_policy_string == "RANDOM":
        replacement_policy = ReplacementPolicy.RANDOM

    capacity_mb = int(re.search(r'\d+', selected_capacity_string).group())

    cache_config = CacheConfig(replacement_policy=replacement_policy,
                                         max_size_mb=capacity_mb,
                                         max_num_items=None)

    manager.set_cache_config(cache_config)
    StorageApi.save_cache_config(cache_config)

    now = datetime.now()
    time_str = now.strftime("%H:%M:%S")
    FrontEndApi.notify_cache_pool_size_change(time_str)  # Used for FrontEnd updates

    msg = "Capacity is set to {} and Replacement policy to {}".format(selected_capacity_string, selected_policy_string) # Used for HTML Render
    return render_template('success_management.html', message=msg)

@managerapp.route('/apply_resize_policy', methods = ['POST'])
def apply_resize_policy(): # Resize Policy
    print("In Apply Resize Policy")
    selected_policy = request.form.get('resize-policy')
    print("Selected_policy = ", selected_policy)
    if selected_policy == "Automatic":
        return render_template('auto_resize.html')
    else: # If manual
        curr_nodes = manager.get_num_active_nodes()
        return render_template('manual_resize.html', active_nodes = curr_nodes)

@managerapp.route('/apply_manual_resize', methods = ['POST'])
def apply_manual_resize():
    print("In Apply C&R Policy")
    number_of_nodes = request.form['active-nodes']
    print("Number of nodes selected: ", number_of_nodes)
    manager.set_num_active_nodes(int(number_of_nodes)) # MemCache API

    new_config = AutoScalerConfig(resizing_policy=Resizingpolicy.MANUAL, max_miss_rate=0.75, min_miss_rate=0.25,
                                  shrink_factor=0.5, growth_factor=2)

    StorageApi.save_autoscaler_config(new_config)
    AutoScalerApi.refresh_config()

    now = datetime.now()
    time_str = now.strftime("%H:%M:%S")
    FrontEndApi.notify_cache_pool_size_change(time_str)  # Used for FrontEnd updates
    
    msg = "Resized Mem-Cache number of Nodes to {} ".format(manager.get_num_active_nodes())
    return render_template('success_management.html', message=msg)

@managerapp.route('/apply_auto_resize', methods = ['POST'])
def apply_auto_resize():
    incorrect_input = False
    try:
        max_miss_rate = float(request.form.get('max_miss_rate'))
        min_miss_rate = float(request.form.get('min_miss_rate'))
        expand_ratio = float(request.form.get('expand_ratio'))
        shrink_ratio = float(request.form.get('shrink_ratio'))
    except ValueError:
        return render_template('incorrect_input.html')

    if(min_miss_rate > max_miss_rate):
        incorrect_input = True
    if(not (max_miss_rate > 0 and max_miss_rate < 1 and min_miss_rate > 0 and min_miss_rate < 1)):
        incorrect_input = True
    if(not (expand_ratio > 0 and shrink_ratio > 0)):
        incorrect_input = True

    if(incorrect_input):
        return render_template('incorrect_input.html')


    print("RETRIEVED VALS: ", max_miss_rate, min_miss_rate, expand_ratio, shrink_ratio)

    new_config = AutoScalerConfig(resizing_policy=Resizingpolicy.AUTO, max_miss_rate=max_miss_rate,
                                  min_miss_rate=min_miss_rate, shrink_factor=shrink_ratio, growth_factor=expand_ratio)

    StorageApi.save_autoscaler_config(new_config)
    AutoScalerApi.refresh_config()

    now = datetime.now()
    time_str = now.strftime("%H:%M:%S")
    FrontEndApi.notify_cache_pool_size_change(time_str)  # Used for FrontEnd updates

    msg = "Automatic Policy Set"
    return render_template('success_management.html', message=msg)

@managerapp.route('/put', methods = ['POST'])
def put():
    key = request.form['key']
    img_data = request.form['img_data']
    return {"success": manager.put(key, img_data)}

@managerapp.route('/get', methods = ['GET'])
def get():
    key = request.form['key']
    print("WE REACH manager.get")
    return {"success": True,
            "img_data": manager.get(key)
            }

@managerapp.route('/clear_all_nodes', methods = ['DELETE'])
def clear_all_nodes():
    return {"success": manager.clear_all_nodes()}

@managerapp.route('/expand_nodes', methods = ['POST'])
def expand_nodes():
    growth_factor = float(request.form['growth_factor'])
    return {"success": manager.grow_nodes_by_factor(growth_factor)}

@managerapp.route('/shrink_nodes', methods = ['POST'])
def shrink_nodes():
    shrink_factor = float(request.form['shrink_factor'])
    return {"success": manager.shrink_nodes_by_factor(shrink_factor)}

@managerapp.route('/get_num_active_nodes', methods=['GET'])
def get_num_active_nodes():
    return {"success": True,
            "num_active_nodes": manager.get_num_active_nodes()
            }

@managerapp.route('/set_num_active_nodes', methods=['POST'])
def set_num_active_nodes():
    num_desired = int(request.form['num_desired'])
    success = manager.set_num_active_nodes(num_desired)
    return {"success": success,
            "new_num_active_nodes": manager.get_num_active_nodes()
            }

@managerapp.route('/get_stat_ids', methods=['GET'])
def get_stat_ids():
    return {"success": True,
            "stat_ids": manager.get_stat_ids()
            }

@managerapp.route('/get_all_keys', methods=['GET'])
def get_keys():
    return {"success": True,
            "keys": manager.get_all_keys()
            }

@managerapp.route('/getRate', methods=['GET'])
def get_rate():
    rate_type = request.form['type']
    last_min_stats = manager.get_last_min_stats()
    if last_min_stats is None or last_min_stats.num_get_req == 0:
        return {"success": False,
                "keys": manager.get_all_keys()
                }

    if rate_type == "hit":
        rate = last_min_stats.hit_rate
    elif rate_type == "miss":
        rate = last_min_stats.miss_rate

    return {"success": True,
            "rate": rate
            }

@managerapp.route('/invalidate', methods=['DELETE'])
def invalidate():
    key = request.form['key']
    return {"success": manager.invalidate(key=key)}

#####################   Helper Functions  ###########################
def list_of_plots():
    x_data = list(range(0,30))
    miss_rates, hit_rates, num_items_in_cache_array, size_of_cache_array, num_req_per_minute = get_last_30_min_cache_stats()
    num_active_nodes = manager.get_num_active_nodes()
    print(num_active_nodes)
    graph1 = create_pie(num_active_nodes)
    graph2 = create_plot("Miss Rate", "Time", "Miss Rate", miss_rates, x_data)
    graph3 = create_plot("Hit Rate", "Time", "Hit Rate", hit_rates, x_data)
    graph4 = create_plot("Number of Items in Cache", "Time", "Number of Items in Cache", num_items_in_cache_array, x_data)
    graph5 = create_plot("Total Size of Items in Cache", "Time", "Total Size of Items in Cache", size_of_cache_array, x_data)
    graph6 = create_plot("Number of Requests Served per Minute", "Time", "Number of Requests Served per Minute", num_req_per_minute, x_data)

    return graph1, graph2, graph3, graph4, graph5, graph6

def create_plot(title, xlabel, ylabel, ydata, xdata):
    print("creating plot ...")
    plot = plt.figure()
    plt.title(title)

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    plt.plot(xdata, ydata)
    plt.xticks([])
    plt_encoded = fig_to_base64(plot)
    plt.show()
    plt.close()
    return plt_encoded

def create_pie(num_active):
    max_nodes = 8
    active_per = round(num_active/max_nodes*100)
    inactive_per = 100-active_per

    plot = plt.figure()
    plt.title("Number of Active Nodes: {}".format(num_active))
    labels = ['Active Nodes', 'Inactive Nodes']
    sizes  = [active_per, inactive_per]
    plt.pie(sizes, labels = labels)
    plt_encoded = fig_to_base64(plot)
    plt.close()
    return plt_encoded

def fig_to_base64(fig):
    img = io.BytesIO()
    fig.savefig(img, format='png',
                bbox_inches='tight')
    img.seek(0)

    return base64.b64encode(img.getvalue())


def get_last_30_min_cache_stats():
    """ Returns series of 30 TimeBoxedCacheStats for graphing. Each TimeBoxedCacheStats represents stats at 1 min. """
    stat_ids = manager.get_stat_ids()
    current_time = int(time.time())

    # Get stats from last 31 min, needs to be 31 minutes so that we can calculate the number of requests delta
    tb_stats = app.boto_utils.get_last_31_min_stats(stat_ids, current_time)

    # Process time boxed stats to produce graph data
    miss_rates = []
    hit_rates = []
    num_items_in_cache_array = []
    size_of_cache_array = []
    num_req_per_minute = []
    prior_num_req_served = tb_stats[0].num_req_served
    for tb_stat in tb_stats[1:]:
        miss_rates.append(0 if tb_stat.miss_rate is None else tb_stat.miss_rate)
        hit_rates.append(0 if tb_stat.hit_rate is None else tb_stat.hit_rate)
        num_items_in_cache_array.append(tb_stat.num_items_in_cache)
        size_of_cache_array.append(tb_stat.cache_size_bytes)
        num_req_per_minute.append(max(0, tb_stat.num_req_served - prior_num_req_served))
        prior_num_req_served = tb_stat.num_req_served

    return miss_rates, hit_rates, num_items_in_cache_array, size_of_cache_array, num_req_per_minute


# APIs for AutoTesting
@managerapp.route('/set_configuration', methods=['POST'])
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

    success = manager.set_configuration(CacheConfig(replacement_policy, max_size_mb, max_num_items))
    return {"success": success}
