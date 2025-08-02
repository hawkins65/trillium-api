import logging
import os
import sys
from datetime import datetime

def setup_logging(script_name, log_dir=None, level=logging.INFO):
    """
    Sets up a standardized logger for a script.

    Args:
        script_name (str): The name of the script (e.g., os.path.basename(__file__).replace('.py', '')).
        log_dir (str, optional): The directory where log files will be stored.
                                 Defaults to '~/log'.
        level (int, optional): The logging level (e.g., logging.INFO, logging.DEBUG).
                               Defaults to logging.INFO.
    Returns:
        logging.Logger: The configured logger instance.
    """
    if log_dir is None:
        log_dir = os.path.expanduser('~/log')

    os.makedirs(log_dir, exist_ok=True)

    # Remove all existing handlers to prevent duplicate logs
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)

    logger = logging.getLogger(script_name)
    logger.setLevel(level)

    # File handler
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{script_name}_log_{formatted_time}.log"))
    file_handler.setLevel(level)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO) # Console always INFO or higher
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger
