import os
import sys
import csv
import glob
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import json

# Debug flag
DEBUG = False

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
                        # Suppress error for interrupted processing (invalid int conversion)
                        if "invalid literal for int() with base 10:" in str(e):
                            logger.debug(f"Ignoring CSV parsing error from interrupted processing in {file}: {str(e)}")
                        else:
                            logger.error(f"Error reading CSV file {file}: {str(e)}")
                
                run_dir_slots = len(collected_slots) - run_dir_slots_before
                logger.info(f"{run_dir}: Found {run_dir_slots} slots")
                
                # Check for good.json and poor.json files specifically in run0
                if run_dir == 'run0':
                    good_json_path = os.path.join(run_dir, 'good.json')
                    poor_json_path = os.path.join(run_dir, 'poor.json')
                    
                    good_exists = os.path.exists(good_json_path)
                    poor_exists = os.path.exists(poor_json_path)
                    
                    if not good_exists and not poor_exists:
                        logger.warning(f"ALERT: Both good.json and poor.json files are missing from {run_dir}")
                    elif not good_exists:
                        logger.warning(f"ALERT: good.json file is missing from {run_dir}")
                    elif not poor_exists:
                        logger.warning(f"ALERT: poor.json file is missing from {run_dir}")
                    else:
                        logger.info(f"Both good.json and poor.json files found in {run_dir}")
                    
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
    epoch_dir = f"/home/smilax/trillium_api/data/epochs/epoch{epoch_number}"
    if not os.path.exists(epoch_dir):
        logger.error(f"Epoch directory {epoch_dir} does not exist.")
        sys.exit(1)
    count = count_collected_slots(epoch_dir)
    # Write slot count to JSON file
    # Ensure output directory exists
    output_dir = os.path.expanduser('~/trillium_api/data/slot_progressing')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, '999_slots_progressing.json')
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
    