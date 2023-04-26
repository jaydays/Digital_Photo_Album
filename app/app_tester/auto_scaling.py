from app.apis import ManagerApi
import matplotlib.pyplot as plt
from app.app_tester.utils import *
import time
import os


def generate_n_requests(n):
    request_pool = AppRequestPool([])
    for i in range(0, n):
        request_pool.append_cache_request(generate_cache_request(manager=None))
    return request_pool


start_nodes = 1
expected_end_nodes = 2
ManagerApi.clear()
time.sleep(15)
ManagerApi.set_num_active_nodes(start_nodes)

times = []
miss_rates = []
pool_sizes = []
pool_size_to_miss_rate = {}
current_time = time.time()
end_time = current_time + 60*5
while current_time < end_time:
    # Get start variables
    start_time = time.time()
    start_num_nodes = ManagerApi.get_num_active_nodes()

    # Create and execute req pool
    req_pool = generate_n_requests(128)
    req_pool.execute_all_requests()

    # Get end variables
    current_time = time.time()
    end_num_nodes = ManagerApi.get_num_active_nodes()
    times.append(current_time)
    miss_rates.append(req_pool.get_miss_rate())
    pool_sizes.append(start_num_nodes)
    if start_num_nodes not in pool_size_to_miss_rate:
        pool_size_to_miss_rate[start_num_nodes] = []
    pool_size_to_miss_rate[start_num_nodes].append(req_pool.get_miss_rate())

    # Make sure each loop takes at least 30 seconds
    time_delta = time.time() - start_time
    if time_delta < 10:
        time.sleep(int(10-time_delta))
    current_time = time.time()

### PLOT MISS RATE VS NODE SIZE
max_miss_rate = 0.15
pool_sizes_seen = pool_size_to_miss_rate.keys()
pool_sizes_seen = sorted(pool_sizes_seen, key=int)
pool_sizes_miss_rates = []
max_miss_line_graph = []
for pool_size_seen in pool_sizes_seen:
    temp_miss_rates = pool_size_to_miss_rate[pool_size_seen]
    pool_sizes_miss_rates.append(float(sum(temp_miss_rates)/len(temp_miss_rates)))
    max_miss_line_graph.append(max_miss_rate)

plt.figure()
plt.plot(list(range(len(pool_sizes_seen))), pool_sizes_miss_rates, label='miss_rate')
plt.plot(list(range(len(pool_sizes_seen))), max_miss_line_graph, label='max miss rate')
plt.xticks(list(range(len(pool_sizes_seen))), pool_sizes_seen)
plt.legend(loc="upper left")
plt.xlabel("Pool Size")
plt.ylabel("Miss Rate")
plt.title("Miss Rate vs Pool Size for Growing Pool")
plt_path = os.path.join(graphs_dir, 'growing_pool_miss_rate_vs_node.png')
plt.savefig(plt_path)
plt.close(plt_path)


### PLOT MISS RATE VS TIME
max_miss_line_graph = [max_miss_rate] * len(times)

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()
ax1.plot(times, miss_rates, label='miss_rate')
ax1.plot(times, max_miss_line_graph, label='max miss rate')
ax2.plot(times, pool_sizes, label='pool size', color='g')
ax1.set_xlabel('Time')
ax1.set_ylabel('Miss Rate')
ax2.set_ylabel('Pool Size', color='g')
ax1.legend(loc="upper left")
ax2.legend(loc="upper right")
plt.title("Miss Rate And Pool Size vs Time for Growing Pool")
plt_path = os.path.join(graphs_dir, 'growing_pool_miss_rate_vs_time.png')
plt.savefig(plt_path)
plt.close(plt_path)