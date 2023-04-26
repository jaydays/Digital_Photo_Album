import logging.config
from app.common import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)
logging.getLogger(__name__).info("Application Starting")
