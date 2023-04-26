from datetime import datetime

import boto3
from app.common import TimeBoxedCacheStats

JASON_AWS_ACCESS_KEY_ID = ""
JASON_AWS_SECRET_ACCESS_KEY = ""

MEMCACHE_KEY_PREFIX = "node"
NUM_MEMCACHE_INSTANCES = 8


def get_new_jason_aws_session():
    return boto3.Session(aws_access_key_id=JASON_AWS_ACCESS_KEY_ID, aws_secret_access_key=JASON_AWS_SECRET_ACCESS_KEY)


def print_caller_identity(aws_session):
    if aws_session is None:
        aws_session = boto3.session.Session()
    sts = aws_session.client('sts')
    print(sts.get_caller_identity())


def create_ec2_client(aws_session):
    # Create a client object for ec2. It provides methods to connect with storage services similar to the storage API service.
    # All API services are available in the Boto3 Client. Maps 1:1 with the storage service API.
    if aws_session is None:
        aws_session = boto3.session.Session()
    return aws_session.client('ec2', region_name='us-east-1')


def create_ec2_resource(aws_session):
    # Create a resource object for ec2. It provides a high level interface to storage services.
    # Not all API services are available in the resource.
    if aws_session is None:
        aws_session = boto3.session.Session()
    return aws_session.resource('ec2', region_name='us-east-1')


def get_new_jason_ec2_resource():
    return create_ec2_resource(get_new_jason_aws_session())


def get_ec2_instance_name(ec2_instance):
    for tag in ec2_instance.tags:
        if tag['Key'] == 'Name':
            return tag['Value']

    return None


def list_ec2_instances(ec2_resource):
    for instance in ec2_resource.instances.all():
        name = get_ec2_instance_name(instance)
        if name is None:
            name = 'NONE'
        print("Name: {0}\nId: {1}\nPlatform: {2}\nType: {3}\nPublic IPv4: {4}\nAMI: {5}\nState: {6}\n"
              .format(name, instance.id, instance.platform, instance.instance_type, instance.public_ip_address,
                      instance.image.id, instance.state))


def get_memcache_ec2_instances_in_order():
    ec2_resource = get_new_jason_ec2_resource()

    name_to_instance = {}
    for instance in ec2_resource.instances.all():
        name = get_ec2_instance_name(instance)
        if name is not None:
            name_to_instance[name] = instance

    ordered_instances = []
    for i in range(NUM_MEMCACHE_INSTANCES):
        memcache_i_name = MEMCACHE_KEY_PREFIX + str(i)
        instance = name_to_instance[memcache_i_name]
        if instance is not None:
            ordered_instances.append(instance)

    return ordered_instances


def get_memcache_ip_addresses(max_num_addresses=None):
    memcache_instances = get_memcache_ec2_instances_in_order()
    result = []
    for instance in memcache_instances:
        if instance.public_ip_address is not None:
            result.append(instance.public_ip_address)
        if max_num_addresses is not None and len(result) == max_num_addresses:
            break

    return result


# def create_memcache_ec2_instances(ec2_resource, num_instances):
#     # ONLY RUN THIS ONCE TO CREATE MEMCACHE NODES
#     launch_template = {
#         'LaunchTemplateName': 'ece1779-ec2-template',
#         'Version': '1'
#     }
#
#     for i in range(num_instances):
#         instances = ec2_resource.create_instances(
#                 LaunchTemplate=launch_template,
#                 ImageId="ami-080ff70d8f5b80ba5",
#                 MinCount=1,
#                 MaxCount=1,
#                 InstanceType="t2.micro",
#                 TagSpecifications=[
#                     {
#                         'ResourceType': 'instance',
#                         'Tags': [
#                             {
#                                 'Key': 'Name',
#                                 'Value': "memcache" + str(i)
#                             },
#                         ]
#                     },
#                 ],
#             )

def get_jason_cloudwatch_client():
    session = get_new_jason_aws_session()
    return session.client('cloudwatch', region_name='us-east-1')


STATS_NAMESPACE = "TEST3"


def save_time_boxed_cache_stats(node_name: str, tb_cache_stat: TimeBoxedCacheStats):
    save_cache_stats(node_name=node_name,
                     total_num_req=tb_cache_stat.num_req_served,
                     num_get_req=tb_cache_stat.num_get_req,
                     num_misses=tb_cache_stat.num_misses,
                     num_hits=tb_cache_stat.num_hits,
                     cache_size_bytes=tb_cache_stat.cache_size_bytes,
                     num_items_in_cache=tb_cache_stat.num_items_in_cache,
                     miss_rate=tb_cache_stat.miss_rate,
                     hit_rate=tb_cache_stat.hit_rate)


def save_cache_stats(node_name, total_num_req, num_get_req, num_misses, num_hits, cache_size_bytes,
                     num_items_in_cache, miss_rate, hit_rate):
    dimensions = [{'Name': 'STATS_BY_NODE', 'Value': node_name}]
    if miss_rate is not None:
        get_jason_cloudwatch_client().put_metric_data(
            MetricData=[
                {
                    'MetricName': 'MISS_RATE',
                    'Dimensions': dimensions,
                    'Unit': 'None',
                    'Value': miss_rate,
                    'StorageResolution': 1
                },
                {
                    'MetricName': 'HIT_RATE',
                    'Dimensions': dimensions,
                    'Unit': 'None',
                    'Value': hit_rate,
                    'StorageResolution': 1
                }
            ],
            Namespace=STATS_NAMESPACE
        )

    get_jason_cloudwatch_client().put_metric_data(
        MetricData=[
            {
                'MetricName': 'TOT_NUM_REQ',
                'Dimensions': dimensions,
                'Unit': 'None',
                'Value': total_num_req,
                'StorageResolution': 1
            },
            {
                'MetricName': 'NUM_GET_REQ',
                'Dimensions': dimensions,
                'Unit': 'None',
                'Value': num_get_req,
                'StorageResolution': 1
            },
            {
                'MetricName': 'NUM_MISSES',
                'Dimensions': dimensions,
                'Unit': 'None',
                'Value': num_misses,
                'StorageResolution': 1
            },
            {
                'MetricName': 'NUM_HITS',
                'Dimensions': dimensions,
                'Unit': 'None',
                'Value': num_hits,
                'StorageResolution': 1
            },
            {
                'MetricName': 'CACHE_SIZE_BYTES',
                'Dimensions': dimensions,
                'Unit': 'None',
                'Value': cache_size_bytes,
                'StorageResolution': 1
            },
            {
                'MetricName': 'NUM_ITEMS_IN_CACHE',
                'Dimensions': dimensions,
                'Unit': 'None',
                'Value': num_items_in_cache,
                'StorageResolution': 1
            },
        ],
        Namespace=STATS_NAMESPACE
    )


def get_aggregated_cache_stats_at_time(node_names, end_time):
    client = get_jason_cloudwatch_client()
    start_time = end_time - 60

    num_req_array = get_aggregated_stat(client, node_names, start_time, end_time, "TOT_NUM_REQ")
    num_get_req_array = get_aggregated_stat(client, node_names, start_time, end_time, "NUM_GET_REQ")
    num_misses_array = get_aggregated_stat(client, node_names, start_time, end_time, "NUM_MISSES")
    num_hits_array = get_aggregated_stat(client, node_names, start_time, end_time, "NUM_HITS")
    cache_size_bytes_array = get_aggregated_stat(client, node_names, start_time, end_time, "CACHE_SIZE_BYTES")
    num_items_in_cache_array = get_aggregated_stat(client, node_names, start_time, end_time, "NUM_ITEMS_IN_CACHE")

    # All these values will default to 0 if stats not available or don't exist
    result = TimeBoxedCacheStats(start_time=None,
                                 end_time=end_time,
                                 num_items_in_cache=sum(num_items_in_cache_array),
                                 cache_size_bytes=sum(cache_size_bytes_array),
                                 num_req_served=sum(num_req_array),
                                 num_get_req=sum(num_get_req_array),
                                 num_misses=sum(num_misses_array),
                                 num_hits=sum(num_hits_array))
    return result


def get_aggregated_stat(cw_client, node_names, start_time, end_time, stat_label):
    stat_array = []
    for node_name in node_names:
        response = cw_client.get_metric_statistics(
            Namespace=STATS_NAMESPACE,
            MetricName=stat_label,
            Dimensions=[
                {
                    "Name": "STATS_BY_NODE",
                    "Value": node_name
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=5,
            Statistics=["Average"]
        )
        data_points = response['Datapoints']
        if len(data_points) == 0:
            continue
        # Sort data points by time (increasing)
        data_points.sort(key=lambda x: x['Timestamp'])
        most_recent_data_point = data_points[-1]
        stat_array.append(most_recent_data_point['Average'])
    return stat_array

def get_last_31_min_stats(node_names, end_time):
    client = get_jason_cloudwatch_client()
    num_req_array = get_last_31_min_stat(client, node_names, end_time, "TOT_NUM_REQ", 0)
    num_get_req_array = get_last_31_min_stat(client, node_names, end_time, "NUM_GET_REQ", 0)
    num_misses_array = get_last_31_min_stat(client, node_names, end_time, "NUM_MISSES", 0)
    num_hits_array = get_last_31_min_stat(client, node_names, end_time, "NUM_HITS", 0)
    cache_size_bytes_array = get_last_31_min_stat(client, node_names, end_time, "CACHE_SIZE_BYTES", 0)
    num_items_in_cache_array = get_last_31_min_stat(client, node_names, end_time, "NUM_ITEMS_IN_CACHE", 0)

    tb_results = []
    for i in range(len(num_req_array)):
        tb_results.append(TimeBoxedCacheStats(start_time=None,
                                 end_time=end_time,
                                 num_items_in_cache=num_items_in_cache_array[i],
                                 cache_size_bytes=cache_size_bytes_array[i],
                                 num_req_served=num_req_array[i],
                                 num_get_req=num_get_req_array[i],
                                 num_misses=num_misses_array[i],
                                 num_hits=num_hits_array[i]))

    return tb_results

def get_last_31_min_stat(client, node_names, end_time, stat_label, default_value):
    end_minute = end_time - (end_time % 60)
    # Returns array of 30 data points of this stat each minute until end_time
    start_minute = end_minute - (31*60)
    result = []
    for node_name in node_names:
        response = client.get_metric_statistics(
            Namespace=STATS_NAMESPACE,
            MetricName=stat_label,
            Dimensions=[
                {
                    "Name": "STATS_BY_NODE",
                    "Value": node_name
                }
            ],
            StartTime=start_minute,
            EndTime=end_minute+1,
            Period=60,
            Statistics=["Average"]
        )
        data_points = response['Datapoints']
        data_point_idx = 0
        data_points.sort(key=lambda x: x['Timestamp'])
        min_idx = 0
        for minute_timestamp in range(int(start_minute), int(end_minute)-1, 60):
            value = None
            while data_point_idx < len(data_points):
                dp_timestamp = datetime.timestamp(data_points[data_point_idx]['Timestamp'])
                if dp_timestamp < minute_timestamp:
                    data_point_idx += 1
                else:
                    break

            if data_point_idx < len(data_points):
                current_data_point = data_points[data_point_idx]
                dp_timestamp = datetime.timestamp(data_points[data_point_idx]['Timestamp'])
                if minute_timestamp <= dp_timestamp <= minute_timestamp + 60:
                    value = current_data_point['Average']
                    data_point_idx += 1

            if value is None:
                value = default_value

            if len(result) <= min_idx:
                result.append(value)
            else:
                result[min_idx] += value
            min_idx += 1

    return result


def get_stats(node_name):
    client = get_jason_cloudwatch_client()
    response = client.get_metric_statistics(
        Namespace=STATS_NAMESPACE,
        MetricName="TOT_NUM_REQ",
        Dimensions=[
            {
                "Name": "STATS_BY_NODE",
                "Value": node_name
            }
        ],
        StartTime=1678501200 - (30),
        EndTime=1678501200,  # 9:20
        Period=5,
        Statistics=[
            "SampleCount",
            "Sum",
            "Average",
            "Minimum",
            "Maximum"
        ]
    )
    data_points = response['Datapoints']
    data_points.sort(key=lambda x: x['Timestamp'])
    most_recent_data_point = data_points[-1]
    temp = most_recent_data_point['Average']
    return temp

# import time
# for i in range(12*5):
#     time.sleep(5)
#     publish_stats_by_node('NODE_1', i+1, i+1, (i+1)/2, 0, i+1, i+1)
#     publish_stats_by_node('NODE_2', i+1, i+1, i, 0, i+1, i+1)

# res = get_aggregated_cache_stats_at_time(['NODE_1', 'NODE_2'], 1678501200)
# get_stats('NODE_1')
# stop = True
