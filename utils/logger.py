"""
Logging Utility Module.
"""

import logging
import os

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logger(log_filename, log_level=logging.DEBUG):
    """Sets up and returns a logger.""" 
    log_path = os.path.join(LOG_DIR, log_filename)
    logger = logging.getLogger(log_filename)

    if not logger.hasHandlers():
        logger.handlers.clear()  # remove them
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger