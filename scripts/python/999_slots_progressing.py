import os
import sys
import csv
import glob
import logging
import json
import getpass
import time
from datetime import datetime

# Debug flag
DEBUG = False

def setup_logging(script_name, log_dir=None, level=logging.DEBUG if DEBUG else logging.INFO):
    """
    Sets up a standardized logger for a script.

    Args:
        script_name (str): The name of the script (e.g., os.path.basename(__file__).replace('.py', '')).
        log_dir (str, optional): The directory where log files will be stored.
                                 Defaults to '~/log'.
        level (int, optional): The logging level (e.g., logging.INFO, logging.DEBUG).
                               Defaults to logging.DEBUG if DEBUG is True, else logging.INFO.
    Returns:
        logging.Logger: The configured logger instance.
    """
    if log_dir is None:
        log_dir = os.path.expanduser('~/log')

    os.makedirs(log_dir, exist_ok=True)

    # Get or create logger
    logger = logging.getLogger(script_name)
    
    # Clear existing handlers to prevent duplicates
    logger.handlers.clear()
    
    # Set logger level
    logger.setLevel(level)

    # File handler
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{script_name}_log_{formatted_time}.log"))
    file_handler.setLevel(level)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler - Set to DEBUG level when DEBUG is True
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)  # Changed from logging.INFO to level
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    if DEBUG:
        logger.debug(f"Logging initialized with level {logging.getLevelName(level)}")
        logger.debug(f"Python version: {sys.version}")
        logger.debug(f"Running as user: {getpass.getuser()}")
        logger.debug(f"Script path: {os.path.abspath(__file__)}")
        logger.debug(f"Script modification time: {datetime.fromtimestamp(os.stat(__file__).st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.debug(f"Logger handlers: {[h.__class__.__name__ for h in logger.handlers]}")

    return logger

# Initialize logger
logger = setup_logging('999_slots_progressing')

def count_collected_slots(epoch_dir):
    collected_slots = set()
    logger.info(f"Scanning {epoch_dir} for collected slots...")
    
    try:
        os.chdir(epoch_dir)
    except Exception as e:
        logger.error(f"Failed to change to directory {epoch_dir}: {str(e)}")
        return 0

    # Scan main epoch directory for CSV files
    main_dir_slots_before = len(collected_slots)
    try:
        csv_files = glob.glob("slot_data_thread_*.csv")
        logger.info(f"Main epoch directory: Found {len(csv_files)} CSV files")
        
        if not csv_files:
            logger.info(f"No CSV files found in main epoch directory {epoch_dir}")
        else:
            for file in csv_files:
                try:
                    with open(file, "r") as f:
                        csv_reader = csv.DictReader(f)
                        for row in csv_reader:
                            if 'block_slot' in row:
                                slot = int(row['block_slot'])
                                collected_slots.add(slot)
                except Exception as e:
                    logger.error(f"Error reading CSV file {file}: {str(e)}")
        
        main_dir_slots = len(collected_slots) - main_dir_slots_before
        logger.info(f"Main epoch directory: Found {main_dir_slots} slots")
    except Exception as e:
        logger.error(f"Error accessing main epoch directory {epoch_dir}: {str(e)}")

    # Scan run* directories
    try:
        run_dirs = glob.glob('run*')
        # Sort run directories numerically
        run_dirs.sort(key=lambda x: int(x[3:]) if x[3:].isdigit() else float('inf'))
        
        if not run_dirs:
            logger.info("No run directories found.")
        
        for run_dir in run_dirs:
            run_dir_slots_before = len(collected_slots)
            try:
                if not os.path.isdir(run_dir):
                    logger.warning(f"Skipping {run_dir}: Not a directory")
                    continue
                if not os.access(run_dir, os.R_OK | os.X_OK):
                    logger.error(f"No read/execute access to {run_dir}")
                    continue
                
                csv_files = glob.glob(os.path.join(run_dir, "slot_data_thread_*.csv"))
                logger.info(f"{run_dir}: Found {len(csv_files)} CSV files")
                
                if not csv_files:
                    logger.info(f"No CSV files found in {run_dir}")
                    continue
                    
                for file in csv_files:
                    try:
                        with open(file, "r") as f:
                            csv_reader = csv.DictReader(f)
                            for row in csv_reader:
                                if 'block_slot' in row:
                                    slot = int(row['block_slot'])
                                    collected_slots.add(slot)
                    except Exception as e:
                        logger.error(f"Error reading CSV file {file}: {str(e)}")
                
                run_dir_slots = len(collected_slots) - run_dir_slots_before
                logger.info(f"{run_dir}: Found {run_dir_slots} slots")
                
            except Exception as e:
                logger.error(f"Error accessing run directory {run_dir}: {str(e)}")
    except Exception as e:
        logger.error(f"Error listing run directories in {epoch_dir}: {str(e)}")

    count = len(collected_slots)
    logger.info(f"Found {count} collected slots in {epoch_dir}.")
    return count

def main():
    logger.info("version 16")
    if len(sys.argv) != 2:
        logger.error("Usage: python3 999_slots_progressing.py <epoch_number>")
        sys.exit(1)
    epoch_number = sys.argv[1]
    epoch_dir = f"/home/smilax/block-production/get_slots/epoch{epoch_number}"
    if not os.path.exists(epoch_dir):
        logger.error(f"Epoch directory {epoch_dir} does not exist.")
        sys.exit(1)
    count = count_collected_slots(epoch_dir)
    # Write slot count to JSON file
    output_file = os.path.expanduser('~/api/999_slots_progressing.json')
    try:
        with open(output_file, 'w') as f:
            json.dump({'slots_count': count, 'epoch': epoch_number}, f)
        logger.info(f"Wrote slot count {count} for epoch {epoch_number} to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write to {output_file}: {str(e)}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
    