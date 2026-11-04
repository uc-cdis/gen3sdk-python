from cdislogging import get_logger

LOG_FORMAT = "[%(asctime)s][%(levelname)7s] %(message)s"
logging = get_logger("gen3", format=LOG_FORMAT, log_level="info")
