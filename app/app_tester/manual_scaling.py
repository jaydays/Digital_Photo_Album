from app.app_tester.utils import *
import matplotlib.pyplot as plt
from app.manager.manager import Manager

# All the image data should be the same
img_data = get_img_x_data(0)

# Setup manager
cache_manager = Manager()
cache_manager.clear_all_nodes()


# Generate request sequence for each step of the graph
request_pools_for_each_num_req = []
num_requests_array = range(100, 801, 50)
for num_req in num_requests_array:
    # Generate requests
    request_pool = AppRequestPool([])
    for i in range(0, num_req):
        request_pool.append_cache_request(generate_cache_request(manager=cache_manager))
    request_pools_for_each_num_req.append(request_pool)

node_size_arrays = [[3, 3, 3],
                    [4, 3, 2],
                    [2, 3, 4]]
node_size_descriptions = ["Constant Pool Size", "Shrinking Pool Size", "Growing Pool Size"]
for i in range(len(node_size_arrays)):
    node_size_array = node_size_arrays[i]
    print("\nNODE SIZE ARRAY: " + str(node_size_array))

    throughputs = []
    av_latencies = []
    num_req_pools_excecuted = 0
    for request_pool in request_pools_for_each_num_req:
        num_req = len(request_pool.app_requests)
        print("\nNUM REQ: " + str(num_req))
        # Clear caches for this request pool
        cache_manager.clear_all_nodes()

        # Calculate desired number of nodes and scale cache accordingly
        percent_progress = num_req_pools_excecuted/len(num_requests_array)
        desired_num_nodes = node_size_array[int(3*percent_progress)]
        while cache_manager.get_num_active_nodes() < desired_num_nodes:
            cache_manager.activate_nodes(1)
            print("AT " + str(num_req) + " SCALED NODES UP TO "
                  + str(cache_manager.get_num_active_nodes()))
        while cache_manager.get_num_active_nodes() > desired_num_nodes:
            cache_manager.deactivate_nodes(1)
            print("AT " + str(num_req) + " SCALED NODES DOWN TO "
                  + str(cache_manager.get_num_active_nodes()))

        # Excecute  request pool, get latency and throughput
        request_pool.execute_all_requests()
        throughputs.append(request_pool.throughput)
        av_latencies.append(request_pool.av_latency)
        num_req_pools_excecuted += 1

    # Save plots
    plt.figure()
    plt.plot(num_requests_array, av_latencies)
    plt.xlabel("Number of Requests")
    plt.ylabel("Av Latency Per Request (sec)")
    plt.title("Latency For " + node_size_descriptions[i])
    plt_path = os.path.join(graphs_dir, 'manual_latency ' + node_size_descriptions[i] + '.png')
    plt.savefig(plt_path)
    plt.close(plt_path)

    plt.figure()
    plt.plot(num_requests_array, throughputs)
    plt.legend(loc="upper left")
    plt.xlabel("Number of Requests")
    plt.ylabel("Throughout (req/sec)")
    plt.title("Throughput For " + node_size_descriptions[i])
    plt_path = os.path.join(graphs_dir, 'manual_throughput ' + node_size_descriptions[i] + '.png')
    plt.savefig(plt_path)
    plt.close(plt_path)

exit()
