import logging
import time
from threading import Thread
from app.common import AutoScalerConfig, Resizingpolicy, TimeBoxedCacheStats, EXPECTED_NUM_NODES
from app.boto_utils import get_aggregated_cache_stats_at_time
from app.apis import ManagerApi, StorageApi

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = AutoScalerConfig(resizing_policy=Resizingpolicy.MANUAL, max_miss_rate=0.75, min_miss_rate=0.25,
                                  shrink_factor=0.5, growth_factor=2)


class AutoScaler:
    """ Contains business logic of autoscaler application. """
    config: AutoScalerConfig
    stat_poll_thread: Thread
    stat_ids = None
    last_min_stats = None  # Just used for local testing

    def __init__(self):
        logger.info("Starting a new Autoscaler instance.")

        # Load autoscaler config
        saved_config = StorageApi.get_most_recent_autoscaler_config()
        if saved_config is None:
            logger.warning("No autoscaler config saved in RDS, creating default and saving.")
            self.config = DEFAULT_CONFIG
            StorageApi.save_autoscaler_config(DEFAULT_CONFIG)
            print("Adding config to rds")
        else:
            self.config = saved_config

        self.stat_save_thread = Thread(target=self.stat_polling_loop)
        self.stat_save_thread.start()

    def refresh_configuration(self):
        new_config = StorageApi.get_most_recent_autoscaler_config()
        if new_config is None:
            logger.error("New autoscaler config is None, skipping update.")
            return False
        else:
            self.config = new_config
            return True

    def clear_last_min_stats(self):
        # Just for local testing purposes
        self.last_min_stats = None

    def stat_polling_loop(self):
        while True:
            time.sleep(60)
            if self.stat_ids is None or len(self.stat_ids) < EXPECTED_NUM_NODES:
                logger.warning("Insufficient number of stat ids for query, attempt reload.")
                self.stat_ids = ManagerApi.get_stat_ids()

            current_stats = None
            self.last_min_stats = current_stats
            if len(self.stat_ids) == EXPECTED_NUM_NODES:
                current_stats = get_aggregated_cache_stats_at_time(self.stat_ids, time.time())
                logger.info("MISS RATE: " + str(current_stats.miss_rate))
            else:
                logger.error("Insufficient number of stat ids for query. Needed "
                             + str(EXPECTED_NUM_NODES) + " found " + str(len(self.stat_ids)))

            if self.config.resizing_policy == Resizingpolicy.AUTO:
                if current_stats is None:
                    logger.warning("Couldn't load current stats")
                elif current_stats.miss_rate is None or current_stats.num_get_req == 0:
                    logger.info("Miss rate not available")
                else:
                    if current_stats.miss_rate < self.config.min_miss_rate:
                        logger.info("Shrinking pool size by factor of " + str(self.config.shrink_factor))
                        try:
                            ManagerApi.shrink_nodes(self.config.shrink_factor)
                        except:
                            logger.error("Shrink Request Failed.")
                    elif current_stats.miss_rate > self.config.max_miss_rate:
                        logger.info("Growing pool size by factor of " + str(self.config.growth_factor))
                        try:
                            ManagerApi.expand_nodes(self.config.growth_factor)
                        except:
                            logger.error("Grow Request Failed.")
