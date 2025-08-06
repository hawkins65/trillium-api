#!/usr/bin/env python3
import os
import csv
import sys
import argparse
import glob
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
from datetime import datetime

# Constants
SLOTS_PER_EPOCH = 432000
CSV_DIR = "/home/smilax/trillium_api/wss_slot_duration"
LOG_DIR = os.path.expanduser("~/log")

def setup_logging(epoch):
    """Set up logging for the epoch slot reporter"""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"epoch{epoch}_slot_report.log")
    
    # Configure logging
    # Logging config moved to unified configurations - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # Logger setup moved to unified configuration
    logger.info(f"Starting epoch {epoch} slot range reporting")
    logger.info(f"Log file: {log_file}")
    
    return logger

def get_epoch_number():
    """Get epoch number from command line argument or user input"""
    parser = argparse.ArgumentParser(description='Report slot range and missing slots for a given epoch')
    parser.add_argument('epoch', nargs='?', type=int, help='Epoch number to process')
    
    args = parser.parse_args()
    
    if args.epoch is not None:
        return args.epoch
    
    # Prompt user for epoch number
    while True:
        try:
            epoch = input("Enter epoch number: ").strip()
            return int(epoch)
        except ValueError:
            print("Please enter a valid integer for the epoch number.")

def find_csv_files(epoch, logger):
    """Find all CSV files for the given epoch"""
    epoch_dir = os.path.join(CSV_DIR, f"epoch{epoch}")
    
    if not os.path.exists(epoch_dir):
        logger.error(f"Directory {epoch_dir} does not exist")
        raise FileNotFoundError(f"Directory {epoch_dir} does not exist")
    
    # Find all CSV files in the epoch directory
    csv_pattern = os.path.join(epoch_dir, f"*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.error(f"No CSV files found in {epoch_dir}")
        raise FileNotFoundError(f"No CSV files found in {epoch_dir}")
    
    csv_files.sort()  # Sort for consistent processing order
    logger.info(f"Found {len(csv_files)} CSV files to process")
    return csv_files

def get_slot_range(csv_files, epoch_start, epoch_end, logger):
    """Read slot data from all CSV files and return slots within the epoch range"""
    found_slots = set()
    
    for csv_file in csv_files:
        logger.info(f"Processing file: {csv_file}")
        try:
            with open(csv_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                if 'slot' not in reader.fieldnames:
                    logger.warning(f"File {csv_file} missing 'slot' column")
                    continue
                
                file_slots = set()
                for row in reader:
                    try:
                        slot = int(row['slot'])
                        if epoch_start <= slot <= epoch_end:
                            file_slots.add(slot)
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Skipping invalid row in {csv_file}: {e}")
                        continue
                
                logger.info(f"File {csv_file}: Found {len(file_slots)} slots within epoch range")
                found_slots.update(file_slots)
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {e}")
            continue
    
    return found_slots

def report_slots_and_missing(found_slots, epoch, epoch_start, epoch_end, logger):
    """Report the slot range found and identify missing slots"""
    if not found_slots:
        logger.error("No slots found in any CSV files for the epoch")
        logger.info(f"Expected slot range for epoch {epoch}: {epoch_start} to {epoch_end}")
        logger.info(f"All {SLOTS_PER_EPOCH} slots in the epoch are missing")
        return
    
    min_slot = min(found_slots)
    max_slot = max(found_slots)
    logger.info(f"Found slots range: {min_slot} to {max_slot}")
    
    # Calculate corresponding epoch(s) for the found slots
    min_epoch = min_slot // SLOTS_PER_EPOCH
    max_epoch = max_slot // SLOTS_PER_EPOCH
    if min_epoch == max_epoch:
        logger.info(f"Found slots belong to epoch: {min_epoch}")
    else:
        logger.info(f"Found slots span epochs: {min_epoch} to {max_epoch}")
    
    # Identify missing slots within the epoch range
    expected_slots = set(range(epoch_start, epoch_end + 1))
    missing_slots = expected_slots - found_slots
    missing_count = len(missing_slots)
    total_slots = epoch_end - epoch_start + 1
    coverage_percentage = (len(found_slots) / total_slots) * 100 if total_slots > 0 else 0
    
    logger.info(f"Total slots expected in epoch {epoch}: {total_slots}")
    logger.info(f"Slots found: {len(found_slots)} ({coverage_percentage:.2f}% coverage)")
    logger.info(f"Missing slots: {missing_count} ({(missing_count / total_slots * 100):.2f}% of expected slots)")
    
    if missing_slots:
        missing_slots_list = sorted(missing_slots)
        # Log a sample of missing slots to avoid overwhelming the log
        sample_size = min(20, len(missing_slots))
        logger.info(f"Sample of missing slots (first {sample_size}): {missing_slots_list[:sample_size]}")
        if len(missing_slots) > sample_size:
            logger.info(f"... and {len(missing_slots) - sample_size} more missing slots")
    else:
        logger.info("No missing slots within the epoch range")

def main():
    """Main function"""
    try:
        # Get epoch number
        epoch = get_epoch_number()
        
        # Set up logging
        logger = setup_logging(epoch)
        
        # Calculate epoch slot range
        epoch_start = epoch * SLOTS_PER_EPOCH
        epoch_end = epoch_start + SLOTS_PER_EPOCH - 1
        logger.info(f"Expected slot range for epoch {epoch}: {epoch_start} to {epoch_end}")
        
        # Find CSV files for the epoch
        csv_files = find_csv_files(epoch, logger)
        
        # Get slot range within epoch boundaries
        found_slots = get_slot_range(csv_files, epoch_start, epoch_end, logger)
        
        # Report slot range and missing slots
        report_slots_and_missing(found_slots, epoch, epoch_start, epoch_end, logger)
        
        logger.info("Slot range reporting completed successfully")
        return 0
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        return 1
    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Fatal error: {e}")
        else:
            print(f"Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())