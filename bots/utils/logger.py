import logging
import os
import sys

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logger(log_filename, log_level=logging.DEBUG):
    """Sets up and returns a logger."""
    print(f"setup_logger called for {log_filename}")
    log_path = os.path.join(LOG_DIR, log_filename)
    logger = logging.getLogger(log_filename)
    logger.propagate = False

    # Always clear handlers before adding new ones
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s")

    # File Handler (UTF-8 safe)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console Handler (Safe for Windows console)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    if os.name == 'nt':
        console_handler.stream.reconfigure(encoding='utf-8')
    logger.addHandler(console_handler)

    logger.setLevel(log_level)
    return logger

