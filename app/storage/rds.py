import pymysql
from app.common import CacheConfig, ReplacementPolicy, Resizingpolicy, AutoScalerConfig


class RDS:
    # Replace the values in these variables with your own RDS instance details
    RDS_HOST = 'a2database.chxd7gakqjvk.us-east-1.rds.amazonaws.com'
    RDS_PORT = 3306
    DB_USER = 'admin'
    DB_PASSWORD = '12345678'
    DB_NAME = 'a2database'
    REGION = "us-east-1"

    TABLES = {
        'AutoScalerConfig': (
            "CREATE TABLE IF NOT EXISTS `AutoScalerConfig` ("
            "  `scaler_id` int(11) NOT NULL AUTO_INCREMENT,"
            "  `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            "  `resizing_policy` enum('manual','automatic') NULL,"
            "  `max_miss_rate` decimal(5,2) NULL,"
            "  `min_miss_rate` decimal(5,2) NULL,"
            "  `shrink_factor` decimal(5,2) NULL,"
            "  `growth_factor` decimal(5,2) NULL,"
            "  PRIMARY KEY (`scaler_id`)"
            ")"),
        'MemcacheConfig': (
            "CREATE TABLE IF NOT EXISTS`MemcacheConfig` ("
            "  `mem_id` int(11) NOT NULL AUTO_INCREMENT,"
            "  `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            "  `policy` enum('RANDOM','LRU') NOT NULL,"
            "  `capacity_mb` int(11) NOT NULL,"
            "  `max_num_items` int(11) NULL,"
            "  PRIMARY KEY (`mem_id`)"
            ")"),
        'Hash': (
            "CREATE TABLE IF NOT EXISTS `Hash` ("
            "  `hash_key` VARCHAR(50) NOT NULL,"
            "  `img_path` VARCHAR(200) NOT NULL,"
            "  PRIMARY KEY (`hash_key`)"
            ")")}

    def __init__(self):
        self.cnx = pymysql.connect(host=self.RDS_HOST, port=self.RDS_PORT, user=self.DB_USER, passwd=self.DB_PASSWORD)
        self.cnx.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME}")
        self.cnx.cursor().execute("USE {}".format(self.DB_NAME))

    def connect_database(self):
        self.cnx.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME}")
        self.cnx.cursor().execute("USE {}".format(self.DB_NAME))

    def create_tables(self):
        for table_name in self.TABLES:
            table_description = self.TABLES[table_name]
            print("Creating table {}: ".format(table_name), end='')
            self.cnx.cursor().execute(table_description)

    def query(self, query, data):
        cursor = self.cnx.cursor()
        cursor.execute("USE {}".format(self.DB_NAME))
        cursor.execute(query, data)
        return cursor

    def insert(self, query, data):
        cursor = self.query(query, data)
        id = cursor.lastrowid
        self.cnx.commit()
        cursor.close()
        return id

    def update(self, query, data):
        cursor = self.query(query, data)
        rowcount = cursor.rowcount
        self.cnx.commit()
        cursor.close()
        return rowcount

    def fetch(self, query, data):
        rows = []
        cursor = self.query(query, data)
        try:
            rows = cursor.fetchall()
        except:
            rows = []
        cursor.close()
        self.cnx.commit()
        return rows

    def __del__(self):
        if self.cnx is not None:
            self.cnx.close()

    def add_key(self, key, img_path):
        add_key_query = ("INSERT INTO Hash "
                         "(hash_key, img_path) "
                         "VALUES (%(hash_key)s, %(img_path)s) "
                         "ON DUPLICATE KEY UPDATE hash_key=%(hash_key)s, img_path=%(img_path)s;")
        add_key_query_data = {
            'hash_key': key,
            'img_path': img_path,
        }
        self.insert(add_key_query, add_key_query_data)

    def key_exists(self, key):
        query = "SELECT 1 from Hash WHERE hash_key=%s"
        result = self.fetch(query, (key,))
        return result is not None and len(result) > 0

    def get_img_path(self, key):
        get_img_path_query = "SELECT img_path from Hash WHERE hash_key=%s"
        result = self.fetch(get_img_path_query, (key,))
        if result is None or len(result) == 0:
            return None
        else:
            return result[0][0]

    def get_all_keys(self):
        get_keys_query = "SELECT hash_key from Hash"
        result = self.fetch(get_keys_query, ())
        if result is None or len(result) == 0:
            return None
        else:
            return list(sum(result, ()))

    def delete_all(self):
        #del_query = "DELETE from Hash;"
        del_query = "TRUNCATE TABLE Hash;"
        self.query(del_query, ())

    def delete_db(self):
        del_query = "DROP DATABASE a2database;"
        self.query(del_query, ())

    def get_most_recent_cache_config(self):
        get_config_query = "SELECT policy, capacity_mb, max_num_items FROM MemcacheConfig ORDER BY timestamp DESC LIMIT 1;"
        result = self.fetch(get_config_query, ())
        if result is None or len(result) == 0:
            return None

        config_entry = result[0]
        if config_entry[0] == 'RANDOM':
            replacement_policy = ReplacementPolicy.RANDOM
        else:
            replacement_policy = ReplacementPolicy.LRU
        max_size_mb = config_entry[1]
        max_num_items = config_entry[2]
        return CacheConfig(replacement_policy, max_size_mb, max_num_items)

    def add_cache_config(self, cache_config: CacheConfig):
        if cache_config.replacement_policy == ReplacementPolicy.LRU:
            replacement_policy = 'LRU'
        else:
            replacement_policy = 'RANDOM'

        add_config_query = ("INSERT INTO MemcacheConfig (policy, capacity_mb, max_num_items) "
                            "VALUES(%(replacement_policy)s, %(capacity_mb)s, %(max_num_items)s);")
        add_config_data = {
            'replacement_policy': replacement_policy,
            'capacity_mb': cache_config.max_size_mb,
            'max_num_items': cache_config.max_num_items
        }
        self.insert(add_config_query, add_config_data)

    def get_most_recent_autoscaler_config(self):
        get_config_query = "SELECT resizing_policy, max_miss_rate, min_miss_rate, shrink_factor, growth_factor \
                            FROM AutoScalerConfig ORDER BY timestamp DESC LIMIT 1;"
        result = self.fetch(get_config_query, ())
        if result is None or len(result) == 0:
            return None

        config_entry = result[0]
        if config_entry[0] == 'manual':
            resizing_policy = Resizingpolicy.MANUAL
        else:
            resizing_policy = Resizingpolicy.AUTO
        max_miss_rate = config_entry[1]
        min_miss_rate = config_entry[2]
        shrink_factor = config_entry[3]
        growth_factor = config_entry[4]
        return AutoScalerConfig(resizing_policy, max_miss_rate, min_miss_rate, shrink_factor, growth_factor)

    def add_autoscaler_config(self, scaler_config: AutoScalerConfig):
        if scaler_config.resizing_policy == Resizingpolicy.MANUAL:
            resizing_policy = 'manual'
        else:
            resizing_policy = 'automatic'

        add_config_query = ("INSERT INTO AutoScalerConfig (resizing_policy, max_miss_rate, min_miss_rate, shrink_factor, growth_factor) "
                            "VALUES(%(resizing_policy)s, %(max_miss_rate)s, %(min_miss_rate)s, %(shrink_factor)s, %(growth_factor)s);")
        add_config_data = {
            'resizing_policy': resizing_policy,
            'max_miss_rate': scaler_config.max_miss_rate,
            'min_miss_rate': scaler_config.min_miss_rate,
            'shrink_factor': scaler_config.shrink_factor,
            'growth_factor': scaler_config.growth_factor,

        }
        self.insert(add_config_query, add_config_data)