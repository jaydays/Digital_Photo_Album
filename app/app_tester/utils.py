import time
from multiprocessing.pool import ThreadPool
from app.apis import ManagerApi
from app.manager.manager import Manager
from random import randint
import os

should_mock_cache_miss = False

# Get tester path
tester_dir = os.path.dirname(os.path.realpath(__file__))
# Create folder to save graphs
graphs_dir = os.path.join(tester_dir, 'graphs')
if not os.path.isdir(graphs_dir):
    os.makedirs(graphs_dir)

# Test images dir
test_img_dir = os.path.join(tester_dir, 'test_imgs')
if not os.path.isdir(test_img_dir):
    print("Test image directory doesn't exist")
    exit()


def mock_md5_hash_for_img_num(img_num):
    # Mock the creation of a 128bit md5 hash, partition will be img_num%16
    # Make sure string for img num is 3 digits long
    three_digit_num = "{:03d}".format(img_num)
    return hex(img_num%16).split('x')[-1] + '0000000000000000000000000000' + three_digit_num

def get_img_x_path(x):
    return os.path.join(test_img_dir, str(x) + '.jpg')


def get_img_x_data(x):
    return {'file': open(get_img_x_path(0), 'rb')} #Always use same image for now


def pick_rand_img_idx():
    return randint(0, 31)


def generate_cache_request(manager):
    img_idx = pick_rand_img_idx()
    # 25% chance of read
    read = (randint(0, 100) < 25)
    if read:
        return AppRequest(is_get=True, key=mock_md5_hash_for_img_num(img_idx), value=None, manager=manager)
    else:
        return AppRequest(is_get=False, key=mock_md5_hash_for_img_num(img_idx), value=get_img_x_data(img_idx),
                          manager=manager)

class AppRequest:
    """ Maintains a simple request to the cache. Either a get or a put. """
    is_get: bool
    key: str
    manager = None
    missed = None

    def __init__(self, is_get, key, value, manager):
        self.is_get = is_get
        self.key = key
        self.value = value
        self.manager = manager

    def mock_cache_miss(self):
        # Load an img from a url to mock delay of having to read from DB
        time.sleep(0.5)
        return None

    def execute(self):
        start_time = time.time()
        if self.is_get:
            if self.manager is None:
                img_data = ManagerApi.get(self.key)
            else:
                img_data = self.manager.get(self.key)
            if img_data is None:
                self.missed = True
                if should_mock_cache_miss:
                    self.mock_cache_miss()
            else:
                self.missed = False
        else:
            if self.manager is None:
                response = ManagerApi.put(self.key, self.value)
            else:
                self.manager.put(self.key, self.value)
        # Return latency
        return time.time() - start_time

    def reset(self):
        self.missed = None


class AppRequestPool:
    """ Maintains a list of cache requests and contains logic to execute them in a thread pool. """
    app_requests: []
    num_threads = 1
    thread_pool: ThreadPool
    latencies: []
    throughput: float
    av_latency: float
    start_exec_time: float
    end_exec_time: float

    def __init__(self, app_requests):
        self.app_requests = app_requests

    def append_cache_request(self, app_request):
        self.app_requests.append(app_request)

    def execute_request(self, cache_request):
        return cache_request.execute()

    def execute_all_requests(self):
        # Execute all requests in worker thread pool
        self.thread_pool = ThreadPool(processes=self.num_threads)
        self.start_exec_time = time.time()
        self.latencies = self.thread_pool.map(self.execute_request, self.app_requests)
        self.end_exec_time = time.time()
        self.thread_pool.close()

        # Calculate stats
        self.av_latency = sum(self.latencies)/len(self.latencies)
        self.throughput = len(self.app_requests) / (self.end_exec_time - self.start_exec_time)

    def get_miss_rate(self):
        num_misses = 0
        num_hits = 0
        for app_req in self.app_requests:
            if app_req.missed is False:
                num_hits += 1
            elif app_req.missed is True:
                num_misses += 1

        if num_misses == 0 and num_hits == 0:
            return None

        return float(num_misses/(num_hits+num_misses))

    def reset(self):
        for app_req in self.app_requests:
            app_req.reset()

