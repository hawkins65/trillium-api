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
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor
import statistics

# Constants
SLOTS_PER_EPOCH = 432000
CSV_DIR = "/home/smilax/trillium_api/data/monitoring/wss_slot_duration"
LOG_DIR = os.path.expanduser("~/log")
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "smilax"
DB_NAME = "sol_blocks"
STD_DEV_MULTIPLIER = 2.0  # Number of standard deviations for acceptable range

def setup_logging(epoch):
    """Set up logging for the epoch processor"""
    logger.info(f"Starting epoch {epoch} slot duration processing")
    return logger

def get_epoch_number():
    """Get epoch number from command line argument or user input"""
    parser = argparse.ArgumentParser(description='Process slot duration data for a given epoch')
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

def find_csv_files(epoch):
    """Find all CSV files for the given epoch"""
    epoch_dir = os.path.join(CSV_DIR, f"epoch{epoch}")
    
    if not os.path.exists(epoch_dir):
        raise FileNotFoundError(f"Directory {epoch_dir} does not exist")
    
    # Find all CSV files in the epoch directory
    csv_pattern = os.path.join(epoch_dir, f"*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {epoch_dir}")
    
    csv_files.sort()  # Sort for consistent processing order
    return csv_files

def get_skipped_and_next_produced_slots(epoch, logger):
    """Query the database for skipped slots and the next produced slot after each"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            database=DB_NAME
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query to get skipped slots and the next produced slot
        query = """
        WITH skipped_slots AS (
            SELECT block_slot
            FROM leader_schedule
            WHERE epoch = %s AND block_produced = false
        ),
        next_produced AS (
            SELECT ls.block_slot
            FROM leader_schedule ls
            WHERE ls.epoch = %s AND ls.block_produced = true
            AND ls.block_slot > ANY (SELECT block_slot FROM skipped_slots)
            AND ls.block_slot = (
                SELECT MIN(block_slot)
                FROM leader_schedule ls2
                WHERE ls2.epoch = ls.epoch
                AND ls2.block_produced = true
                AND ls2.block_slot > ls.block_slot
            )
        )
        SELECT block_slot FROM skipped_slots
        UNION
        SELECT block_slot FROM next_produced
        ORDER BY block_slot;
        """
        cursor.execute(query, (epoch, epoch))
        excluded_slots = [row['block_slot'] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(excluded_slots)} slots to exclude (skipped and their next produced slots)")
        
        cursor.close()
        conn.close()
        return set(excluded_slots)
    
    except Exception as e:
        logger.error(f"Error querying database for skipped slots: {e}")
        raise

def read_slot_data(csv_files, logger):
    """Read slot data from all CSV files and aggregate by slot"""
    slot_data = defaultdict(list)  # slot -> list of duration values
    total_records = 0
    
    for csv_file in csv_files:
        logger.info(f"Processing file: {csv_file}")
        
        try:
            with open(csv_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                # Validate expected columns
                expected_columns = ['timestamp', 'slot', 'duration_nanos', 'identity_pubkey', 'name', 'vote_account_pubkey']
                if not all(col in reader.fieldnames for col in expected_columns):
                    logger.warning(f"File {csv_file} has unexpected column structure: {reader.fieldnames}")
                
                file_records = 0
                for row in reader:
                    try:
                        slot = int(row['slot'])
                        duration_str = row['duration_nanos'].strip()
                        
                        # Skip rows with invalid duration
                        if duration_str == 'N/A' or duration_str == '':
                            logger.warning(f"Skipping row in {csv_file} for slot {slot}: Invalid duration '{duration_str}'")
                            continue
                            
                        duration = int(duration_str)
                        if duration <= 0:
                            logger.warning(f"Skipping row in {csv_file} for slot {slot}: Non-positive duration {duration}")
                            continue
                            
                        slot_data[slot].append(duration)
                        file_records += 1
                        total_records += 1
                        
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Skipping invalid row in {csv_file}: {row} - Error: {e}")
                        continue
                
                logger.info(f"Processed {file_records} records from {csv_file}")
                
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {e}")
            continue
    
    logger.info(f"Total records processed: {total_records}")
    logger.info(f"Unique slots found: {len(slot_data)}")
    
    return slot_data

def calculate_epoch_slot_range(epoch, slot_data, logger):
    """Calculate the expected slot range for the given epoch"""
    if not slot_data:
        logger.error("No slot data to determine epoch boundaries")
        return None, None
    
    min_data_slot = min(slot_data.keys())
    max_data_slot = max(slot_data.keys())
    
    # Calculate which epoch these slots belong to
    data_epoch_start = (min_data_slot // SLOTS_PER_EPOCH) * SLOTS_PER_EPOCH
    data_epoch_end = data_epoch_start + SLOTS_PER_EPOCH - 1
    
    # For the requested epoch, calculate the expected range
    expected_epoch_start = epoch * SLOTS_PER_EPOCH
    expected_epoch_end = expected_epoch_start + SLOTS_PER_EPOCH - 1
    
    logger.info(f"Data spans epoch slots: {data_epoch_start} to {data_epoch_end}")
    logger.info(f"Requested epoch {epoch} expected slots: {expected_epoch_start} to {expected_epoch_end}")
    
    # Check if our data is from the requested epoch
    if (min_data_slot >= expected_epoch_start and max_data_slot <= expected_epoch_end):
        logger.info(f"Data appears to be from requested epoch {epoch}")
        return expected_epoch_start, expected_epoch_end
    else:
        actual_epoch = min_data_slot // SLOTS_PER_EPOCH
        logger.warning(f"Data appears to be from epoch {actual_epoch}, not requested epoch {epoch}")
        logger.warning(f"Using actual data epoch range: {data_epoch_start} to {data_epoch_end}")
        return data_epoch_start, data_epoch_end

def process_slot_durations(slot_data, epoch, logger):
    """Process slot durations, select best duration based on statistical deviation, and identify missing slots"""
    processed_slots = {}
    excluded_slots = get_skipped_and_next_produced_slots(epoch, logger)
    duplicate_count = 0
    
    # Process each slot's duration data
    for slot, durations in slot_data.items():
        if slot in excluded_slots:
            logger.debug(f"Excluding slot {slot} (skipped or next produced after skipped)")
            continue
        if durations:
            if len(durations) > 1:
                duplicate_count += 1
                logger.debug(f"Slot {slot}: {len(durations)} entries")
                
                # Sort durations in ascending order
                sorted_durations = sorted(durations)
                
                # Calculate mean and standard deviation of all durations
                mean_duration = statistics.mean(durations)
                try:
                    std_dev = statistics.stdev(durations)
                except statistics.StatisticsError:
                    std_dev = 0  # If only one duration or identical durations, std_dev is 0
                
                # Try each duration, starting with the fastest
                selected_duration = None
                for i, duration in enumerate(sorted_durations):
                    # Calculate percentage difference from mean
                    percentage_diff = abs(duration - mean_duration) / mean_duration * 100 if mean_duration > 0 else 0
                    
                    # Check if duration is within acceptable range (k * standard deviation)
                    if std_dev == 0 or abs(duration - mean_duration) <= STD_DEV_MULTIPLIER * std_dev:
                        selected_duration = duration
                        logger.debug(f"Slot {slot}: Selected duration {duration}ns (percentage diff from mean: {percentage_diff:.2f}%)")
                        break
                    else:
                        logger.debug(f"Slot {slot}: Discarded duration {duration}ns (percentage diff from mean: {percentage_diff:.2f}%, exceeds {STD_DEV_MULTIPLIER} std dev {std_dev:.2f})")
                
                # If no duration is within range, use the mean
                if selected_duration is None:
                    selected_duration = mean_duration
                    logger.warning(f"Slot {slot}: No duration within {STD_DEV_MULTIPLIER} std dev, using mean {selected_duration:.2f}ns")
            
            else:
                # Single duration, use it directly
                selected_duration = durations[0]
                logger.debug(f"Slot {slot}: Single duration {selected_duration}ns")
            
            processed_slots[slot] = selected_duration
    
    logger.info(f"Found {duplicate_count} slots with duplicate entries")
    logger.info(f"Excluded {len(excluded_slots)} slots (skipped or next produced after skipped)")
    
    if not processed_slots:
        logger.error("No valid slot data found after filtering")
        return processed_slots, []
    
    # Calculate the expected epoch slot range
    epoch_start, epoch_end = calculate_epoch_slot_range(epoch, slot_data, logger)
    
    if epoch_start is None or epoch_end is None:
        logger.error("Could not determine epoch slot range")
        return processed_slots, []
    
    # Find missing slots in the epoch
    missing_slots = []
    for slot in range(epoch_start, epoch_end + 1):
        if slot not in processed_slots and slot not in excluded_slots:
            missing_slots.append(slot)
    
    # Calculate coverage statistics
    total_expected_slots = epoch_end - epoch_start + 1 - len(excluded_slots)
    data_coverage = len(processed_slots) / total_expected_slots * 100 if total_expected_slots > 0 else 0
    missing_percentage = len(missing_slots) / total_expected_slots * 100 if total_expected_slots > 0 else 0
    
    logger.info(f"Epoch slot range: {epoch_start} to {epoch_end}")
    logger.info(f"Total expected slots (after exclusions): {total_expected_slots}")
    logger.info(f"Data coverage: {len(processed_slots)}/{total_expected_slots} ({data_coverage:.2f}%)")
    logger.info(f"Missing slots: {len(missing_slots)} ({missing_percentage:.2f}% of expected slots)")
    
    # Log some missing slots (not all to avoid spam)
    if missing_slots:
        missing_slots.sort()
        sample_missing = missing_slots[:20]  # Show first 20
        logger.warning(f"Sample missing slots: {sample_missing}")
        if len(missing_slots) > 20:
            logger.warning(f"... and {len(missing_slots) - 20} more missing slots")
    
    return processed_slots, missing_slots

def write_output_file(processed_slots, epoch, logger):
    """Write the processed slot durations to output CSV file"""
    output_file = os.path.join(CSV_DIR, f"epoch{epoch}_slot_duration.csv")
    
    # Calculate epoch boundaries for the requested epoch
    epoch_start = epoch * SLOTS_PER_EPOCH
    epoch_end = epoch_start + SLOTS_PER_EPOCH - 1
    
    # Filter slots to only include those from the requested epoch
    filtered_slots = {slot: duration for slot, duration in processed_slots.items() 
                     if epoch_start <= slot <= epoch_end}
    
    # Count slots that were filtered out
    filtered_out_count = len(processed_slots) - len(filtered_slots)
    
    if filtered_out_count > 0:
        logger.warning(f"Filtered out {filtered_out_count} slots that don't belong to epoch {epoch}")
        
        # Log some examples of filtered slots for debugging
        filtered_out_slots = [slot for slot in processed_slots.keys() if not (epoch_start <= slot <= epoch_end)]
        if filtered_out_slots:
            sample_filtered = sorted(filtered_out_slots)[:10]  # Show first 10
            logger.warning(f"Sample filtered slots: {sample_filtered}")
    
    if not filtered_slots:
        logger.error(f"No slots found for epoch {epoch} after filtering")
        logger.error(f"Available slot range: {min(processed_slots.keys())} to {max(processed_slots.keys())}")
        logger.error(f"Expected epoch {epoch} range: {epoch_start} to {epoch_end}")
        raise ValueError(f"No slots found for epoch {epoch}")
    
    try:
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['slot', 'duration_nanos'])
            
            # Sort slots for consistent output
            sorted_slots = sorted(filtered_slots.keys())
            
            for slot in sorted_slots:
                duration = filtered_slots[slot]
                # Round to integer for output
                writer.writerow([slot, int(round(duration))])
        
        logger.info(f"Output written to: {output_file}")
        logger.info(f"Total slots written: {len(filtered_slots)}")
        logger.info(f"Slot range in output: {min(filtered_slots.keys())} to {max(filtered_slots.keys())}")
        
    except Exception as e:
        logger.error(f"Error writing output file {output_file}: {e}")
        raise

def main():
    """Main function"""
    try:
        # Get epoch number
        epoch = get_epoch_number()
        
        # Set up logging
        logger = setup_logging(epoch)
        
        # Find CSV files for the epoch
        logger.info(f"Looking for CSV files for epoch {epoch}")
        csv_files = find_csv_files(epoch)
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Read slot data from all files
        slot_data = read_slot_data(csv_files, logger)
        
        if not slot_data:
            logger.error("No valid slot data found in CSV files")
            return 1
        
        # Process slot durations and find missing slots
        processed_slots, missing_slots = process_slot_durations(slot_data, epoch, logger)
        
        # Write output file
        write_output_file(processed_slots, epoch, logger)
        
        # Final summary
        logger.info("Processing completed successfully")
        logger.info(f"Processed {len(processed_slots)} total slots")
        logger.info(f"Missing slots: {len(missing_slots)}")
        
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